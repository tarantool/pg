-- init.lua (internal file)

local fiber = require('fiber')
local driver = require('pg.driver')
local ffi = require('ffi')

local pool_mt
local conn_mt

-- internal postgres connection function
local function pg_connect(conn_str)
    local pg_status, conn = driver.connect(conn_str)
    if pg_status == -1 then
        return nil, conn
    end
    return conn
end

-- internal execute function
-- returns executions state (1 - ok, 0 - error, -1 - bad connection),
-- result set and status message
local function pg_execute(conn, sql, ...)
    local pg_status, msg = conn:sendquery(sql, ...)
    if pg_status ~= 1 then
        return pg_status, msg
    end
    local recordset = {}
    local last_msg = nil
    local error_status = nil
    local error_msg = nil
    while true do
        pg_status, msg = conn:resultget(recordset)
        if pg_status == nil then
            break
        end
        if pg_status ~= 1 then
            -- we need to repeat geting result until we get null result value
            -- just preserve error msg and status
            error_status = pg_status
            error_msg = msg
        end
        last_msg = msg
    end
    if error_status ~= nil then
        return error_status, nil, error_msg
    end
    return 1, recordset, last_msg
end

-- pool error helper
local function pool_error(pool, msg)
    if pool.raise then
        error(msg)
    end
    return nil, msg
end

-- create cursor object
local function conn_get(pool)
    local pgconn = pool.queue:get()
    if pgconn == nil then
        pgconn, msg = pg_connect(pool.conn_str)
        if pgconn == nil then
            return pool_error(pool, msg)
        end
    end
    local queue = fiber.channel(1)
    queue:put(true)
    local conn = setmetatable({
        usable = true,
        pool = pool,
        conn = pgconn,
        queue = queue,
	--we can use ffi gc to return pg connection to pool
        __gc_hook = ffi.gc(ffi.new('void *'),
            function(self)
               pool.queue:put(pgconn)
            end)
    }, conn_mt)

    return conn
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
                return pool_error(self.pool, 'Connection is not usable')
            end
            if not self.queue:get() then
                self.queue:put(false)
                return pool_error(self.pool, 'Connection is broken')
            end
            local status, data, msg = pg_execute(self.conn, sql, ...)
            if status == -1 then
                self.queue:put(false)
                return pool_error(self.pool, msg)
            end
            self.queue:put(true)
            if status == 0 then
                return pool_error(self.pool, msg)
            end
            return data, msg
        end,
        begin = function(self)
            return self:execute('BEGIN')
        end,
        commit = function(self)
            return self:execute('COMMIT')
        end,
        rollback = function(self)
            return self:execute('ROLLBACK')
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
                return pool_error(self.pool, 'Connection is not usable')
            end
            if not self.queue:get() then
                self.queue:put(false)
                return pool_error(self.pool, 'Connection is broken')
            end
            local status, msg = self.conn:active()
            if status ~= 1 then
                self.queue:put(false)
                return pool_error(self.pool, msg)
            end
            self.queue:put(true)
            return msg
        end

    }
}

-- Create connection pool. Accepts pg connection params (host, port, user,
-- password, dbname) separatelly or in one string, size and raise flag.
local function pool_connect(opts)
    opts = opts or {}
    opts.size = opts.size or 1
    local conn_string
    if opts.conn_string then
        conn_string = opts.conn_string
    else
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
        conn_string = table.concat(connb)
    end

    local queue = fiber.channel(opts.size)

    for i = 1, opts.size do
        local conn, msg = pg_connect(conn_string)
        if conn == nil then
            while queue:count() > 0 do
                local conn = queue:get()
                close(conn)
            end
            return pool_error(self, msg)
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
        local conn = self.queue:get()
        conn.close(conn)
    end
    return 1
end

-- Returns cursor (contains connection) connection
local function pool_get(self)
    if not self.usable then
        return pool_error(self, 'Pool is not usable')
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

return {
    connect = pool_connect;
}
