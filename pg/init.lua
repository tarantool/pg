-- init.lua (internal file)

local fiber = require('fiber')
local driver = require('pg.driver')
local ffi = require('ffi')

local pool_mt
local conn_mt

-- internal execute function
-- returns executions state (1 - ok, 0 - error, -1 - bad connection,
-- -2 - fiber cancelled),
-- returns array of datasets and status messages
local function pg_execute(conn, sql, ...)
    local pg_status, msg = conn:sendquery(sql, ...)
    if pg_status ~= 1 then
        return pg_status, msg
    end
    local error_status = nil
    local error_msg = nil
    local results = {}
    local result_no = 0
    while true do
        pg_status,
            results[result_no * 2 + 2], results[result_no * 2 + 1] =
            conn:resultget()
        if pg_status == nil then
            break
        end
        if pg_status < 0 then
            -- we need to repeat geting result until we get null result value
            -- just preserve error msg and status
            error_status = pg_status
            error_msg = msg
        end
        result_no = result_no + 1
    end
    if error_status ~= nil then
        return error_status, error_msg
    end
    return 1, results
end

-- pool error helper
local function get_error(raise, msg)
    if raise then
        error(msg)
    end
    return nil, msg
end

--create a new connection
local function conn_create(pg_conn, pool)
    local queue = fiber.channel(1)
    queue:put(true)
    local conn = setmetatable({
        usable = true,
        pool = pool,
        conn = pg_conn,
        queue = queue,
        -- we can use ffi gc to return pg connection to pool
        __gc_hook = ffi.gc(ffi.new('void *'),
            function(self)
                pg_conn:close()
                if not pool.virtual then
                    pool.queue:put(nil)
                end
            end)
    }, conn_mt)

    return conn
end

-- get connection from pool
local function conn_get(pool)
    local pg_conn = pool.queue:get()
    local status
    if pg_conn == nil then
        status, pg_conn = driver.connect(pool.conn_string)
        if status == -1 then
            return get_error(pool.raise, pg_conn)
        end
        if status == -2 then
            return status
        end
    end
    return conn_create(pg_conn, pool)
end

local function conn_put(conn)
    local pgconn = conn.conn
    --erase conn for gc
    conn.conn = nil
    if not conn.queue:get() then
        conn.usable = false
        return nil
    end
    conn.usable = false
    return pgconn
end

conn_mt = {
    __index = {
        execute = function(self, sql, ...)
            if not self.usable then
                return get_error(self.raise.pool, 'Connection is not usable')
            end
            if not self.queue:get() then
                self.queue:put(false)
                return get_error(self.raise.pool, 'Connection is broken')
            end
            local status, datas = pg_execute(self.conn, sql, ...)
            if status == -1 then
                self.queue:put(false)
                return get_error(self.raise.pool, datas)
            end
            self.queue:put(true)
            return unpack(datas)
        end,
        begin = function(self)
            self:execute('BEGIN')
        end,
        commit = function(self)
            self:execute('COMMIT')
        end,
        rollback = function(self)
            self:execute('ROLLBACK')
        end,
        ping = function(self)
            local pool = self.pool
            self.pool = {raise = false}
            local data, msg = self:execute('SELECT 1 AS code')
            self.pool = pool
            return data ~= nil and data[1].code == 1
        end,
        active = function(self)
            if not self.usable then
                return get_error(self.raise.pool, 'Connection is not usable')
            end
            if not self.queue:get() then
                self.queue:put(false)
                return get_error(self.raise.pool, 'Connection is broken')
            end
            local status, msg = self.conn:active()
            if status ~= 1 then
                self.queue:put(false)
                return get_error(self.raise.pool, msg)
            end
            self.queue:put(true)
            return msg
        end
    }
}

local function build_conn_string(opts)
    if opts.conn_string then
        return opts.conn_string
    end
    local connb = {}
    if opts.host then
        table.insert(connb, string.format(" host='%s'", opts.host))
    end
    if opts.port then
        table.insert(connb, string.format(" port='%s'", opts.port))
    end
    if opts.user then
        table.insert(connb, string.format(" user='%s'", opts.user))
    end
    if opts.pass or opts.password then
        table.insert(connb, string.format(" password='%s'",
            opts.pass or opts.password))
    end
    if opts.db then
        table.insert(connb, string.format(" dbname='%s'", opts.db))
    end
    return table.concat(connb)
end

-- Create connection pool. Accepts pg connection params (host, port, user,
-- password, dbname) separatelly or in one string, size and raise flag.
local function pool_create(opts)
    opts = opts or {}
    local conn_string = build_conn_string(opts)
    opts.size = opts.size or 1
    local queue = fiber.channel(opts.size)

    for i = 1, opts.size do
        local status, conn = driver.connect(conn_string)
        if status < 0 then
            while queue:count() > 0 do
                local pg_conn = queue:get()
                pg_conn:close()
            end
            if status == -1 then
                return get_error(opts.raise, conn)
            else
                return -2
            end
        end
        queue:put(conn)
    end

    return setmetatable({
        -- connection variables
        host        = opts.host,
        port        = opts.port,
        user        = opts.user,
        pass        = opts.pass,
        db          = opts.db,
        size        = opts.size,
        conn_string  = conn_string,

        -- private variables
        queue       = queue,
        raise       = opts.raise,
        usable      = true
    }, pool_mt)
end

-- Close pool
local function pool_close(self)
    self.usable = false
    for i = 1, self.size do
        local pg_conn = self.queue:get()
        if pg_conn ~= nil then
            pg_conn:close()
        end
    end
    return 1
end

-- Returns connection
local function pool_get(self)
    if not self.usable then
        return get_error(self.raise, 'Pool is not usable')
    end
    local conn = conn_get(self)
    local reset_sql = 'BEGIN; RESET ALL; COMMIT;'
    if conn:active() then
        reset_sql = 'ROLLBACK; ' .. reset_sql
    end
    conn:execute(reset_sql)
    return conn
end

-- Free binded connection
local function pool_put(self, conn)
    if conn.usable then
        self.queue:put(conn_put(conn))
    end
end

pool_mt = {
    __index = {
            get = pool_get;
        put = pool_put;
        close = pool_close;
    }
}

-- Create connection. Accepts pg connection params (host, port, user,
-- password, dbname) separatelly or in one string and raise flag.
local function connect(opts)
    opts = opts or {}
    local pool = {virtual = true, raise = opts.raise}

    local conn_string = build_conn_string(opts)
    local status, pg_conn = driver.connect(conn_string)
    if status == -1 then
        return get_error(pool.raise, pg_conn)
    end
    if status == -2 then
        return -2
    end
    return conn_create(pg_conn, pool)
end

return {
    connect = connect;
    pool_create = pool_create;
}
