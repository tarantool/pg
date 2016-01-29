-- sql.lua (internal file)

local fiber = require('fiber')
local driver = require('pg.driver')
local socket = require('socket')

local pool_mt

-- internal postgres connection function
local function pg_connect(conn_str)
    local pg_status, conn = driver.connect(conn_str)
    if pg_status == -1 then
        return conn_error(pool, conn)
    end
    while true do
        local wait, fd = conn:connpoll()
        if wait == 0 then
            break
        end
       if wait == -1 then
           return nil, fd
       end
       if wait == 1 then
           socket.iowait(fd, 1)
       elseif wait == 2 then
           socket.iowait(fd, 2)
       else
           return nil, "Unwanted driver reply"
       end
    end
    return conn
end

-- internal execute function
-- returns executions state (1 - ok, 0 - error, -1 - bad connection),
-- result set and status message
local function pg_execute(conn, sql, ...)
    local pg_status, msg = conn:executeasync(sql, ...)
    if pg_status ~= 1 then
        return pg_status, msg
    end
    local fd = msg
    local result = {}
    local last_msg = nil
    local error_status = nil
    local error_msg = nil
    while true do
        pg_status, msg = conn:resultavailable()
	if pg_status ~= 1 then
	    return pg_status, msg
        end
	if msg then
	    pg_status, msg = conn:resultget(result)
	    if pg_status ~= 1 then
	        --we need to repeat geting result until we get null result value
		error_status = pg_status
	        error_msg = msg
            elseif msg == nil then
	        break
	    end
	    last_msg = msg
	else
	    socket.iowait(fd, 1)
        end
    end
    if error_status ~= nil then
        return error_status, nil, error_msg
    end
    return 1, result, last_msg
end

-- pool error helper
local function pool_error(pool, msg)
    if pool.raise then
        error(msg)
    end
    return nil, msg
end

-- unlink cursor and pg connection, return connection to pool
local function cursor_free(cursor)
    local conn = cursor.conn
    cursor.pool.queue:put(conn)
    cursor.pool.conns[conn] = nil
    cursor.conn = nil
end

-- create cursor object
local function cursor_get(pool)
    local self = newproxy(true)
    local meta = getmetatable(self)
    meta.pool = pool
    meta.__queue = fiber.channel(1)
    meta.__lock_fiber = 0
    meta.__lock_cnt = 0
    meta.__broken = false
    meta.__queue:put(true)
    meta.conn = pool.queue:get()
    if meta.conn == nil then
        meta.conn, msg = pg_connect(pool.conn_str)
	if not meta.conn then
	    pool.queue:put(nil)
	    return pool_error(pool, msg)
	end
    end
    pool.conns[meta.conn] = fiber.id()

    -- lock cursor for use
    meta.lock = function (self)
        if self.__lock_fiber == fiber.id() then
	    self.__lock_cnt = self.__lock_cnt + 1
	    return not self.__broken
	end
        local state = self.__queue:get()
        if not state then
	    self.__queue:put(state)
	end
	self.__lock_fiber = fiber.id()
	self.__lock_cnt = 1
	return state
    end

    -- unlock cursor, broken should be true if connection is bad
    meta.unlock = function (self, broken)
        self.__broken = self.__broken or broken
        self.__lock_cnt = self.__lock_cnt - 1
	if self.__lock_cnt == 0 then
	    self.__lock_fiber = 0
            self.__queue:put(not self.__broken)
	end
    end

    -- execute sql. returns recordset and status message
    -- you can pass multistatement sql without parameters or
    -- single statement with parameters (pg limitation)
    meta.execute = function (self, sql, ...)
        if not self:lock() then
	    return pool_error(self.pool, 'Connection is unusable')
	end
        local succ, pg_status, res, msg = pcall(pg_execute, self.conn, sql, ...)
	if not succ then
	    self:unlock()
	    return pool_error(self.pool, pg_status)
	end
        if pg_status == -1 then
	    self:unlock(true)
            cursor_free(self)
	    return pool_error(self.pool, msg)
        end
	self:unlock()
        if pg_status ~= 1 then
            return pool_error(self.pool, msg)
        end
        return res, msg
    end

    -- true if connection has active transaction
    meta.active = function (self)
        if not self:lock() then
	    return pool_error(self.pool, 'Connection is unusable')
	end
        local pg_status, active = self.conn:active(self)
	if pg_status == -1 then
	    self:unlock(true)
	    cursor_free(self)
	    return pool_error(self.pool, active)
	end
        self:unlock()
	return active
    end

    -- try to execute sql, true if success
    meta.ping = function (self)
        if not self:lock() then
	    return pool_error(self.pool, 'Connection is unusable')
	end
        local raise = self.pool.raise
        self.pool.raise = false
        local res = self:execute('SELECT 1 AS code')
        self.pool.raise = raise
	self:unlock()
        return res ~= nil and res[1].code == 1
    end

    meta.begin = function (self)
        return self:execute('BEGIN')
    end

    meta.commit = function (self)
        return self:execute('COMMIT')
    end

    meta.rollback = function (self)
	return self:execute('ROLLBACK')
    end

    -- unlink connection and cursor, return connection to pool
    meta.free = function (self)
        if not self:lock() then
            return pool_error(self.pool, 'Connection is unusable')
        end
        if self:active() then
            self:rollback()
        end
        cursor_free(self)
        self:unlock(true)
    end

    meta.__index = function(tab, key)
        return meta[key]
    end
    meta.__newindex = function(tab, key, value)
        meta[key] = value
    end
    meta.__gc = function(self)
        if not meta.conn then
	    return
	end
	self.pool.raise = false
	cursor_free(self, false)
    end
    return self
end

-- bind connection to fiber
local function cursor_bind(self)
    if fiber.self().storage.__pg_cursor == nil then
        fiber.self().storage.__pg_cursor = {}
    end
    if fiber.self().storage.__pg_cursor[self] ~= nil then
        return fiber.self().storage.__pg_cursor[self]
    end
    fiber.self().storage.__pg_cursor[self] = cursor_get(self)
    return fiber.self().storage.__pg_cursor[self]
end

-- unbind connection from fiber
local function cursor_unbind(self, force)
    if fiber.self().storage.__pg_cursor == nil or 
      fiber.self().storage.__pg_cursor[self] == nil then
        return
    end
    local cursor = fiber.self().storage.__pg_cursor[self]
    if cursor:active() then
        if force then
	    cursor:commit()
	else
            return
	end
    end
    cursor_free(cursor)
    fiber.self().storage.__pg_cursor[self] = nil
end

-- Create connection pool helper
local function pool_create(opts)
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

    return setmetatable({
        driver = driver,

        -- connection variables
        host        = opts.host,
        port        = opts.port,
        user        = opts.user,
        pass        = opts.pass,
        db          = opts.db,
	size        = opts.size,
        conn_string  = conn_string,

        -- private variables
        queue       = fiber.channel(opts.size),
	conns       = {},
        raise       = opts.raise
    }, pool_mt)
end

-- Init connection pool
local function pool_init(pool)
    for i = 1, pool.size do
        local conn, msg = pg_connect(pool.conn_string)
	if conn == nil then
	    while pool.queue:count() > 0 do
	        local conn = pool.queue:get()
		pool.conns[conn] = nil
		conn.close(conn)
	    end
	    return pool_error(pool, msg)
	end
	pool.queue:put(conn)
	pool.conns[conn] = 0
    end
    return pool.queue:count()
end

-- Create connection pool. Accepts pg connection params (host, port, user,
-- password, dbname) separatelly or in one string, size and raise flag.
-- Pool can use as standalone connection, in this case pool bind 
-- connection to fiber until transaction is active
local function pool_connect(opts)
    local pool = pool_create(opts)
    local count, msg = pool_init(pool)
    if not count then
        return nil, msg
    end
    return pool
end

-- Close pool
local function pool_close(self)
    for conn, id in pairs(self.conns) do
        if id == fiber.id() then
	    return pool_error(self, 'Current fiber has active connection!')
        end
    end
    for i = 1, self.size do
        local conn = self.queue:get()
	self.conns[conn] = nil
	conn.close(conn)
    end
    return 1
end

-- Proxy method
local function pool_execute(self, sql, ...)
    local cursor = cursor_bind(self)
    local res, msg = cursor:execute(sql, ...)
    cursor_unbind(self)
    return res, msg
end

-- Proxy method 
local function pool_ping(self)
    local cursor = cursor_bind(self)
    local res = cursor:ping()
    cursor_unbind(self)
    return res
end

-- Proxy method
local function pool_begin(self)
    local cursor = cursor_bind(self)
    return cursor:execute('BEGIN')
end

-- Proxy method
local function pool_commit(self)
    local cursor = cursor_bind(self)
    local res, msg = cursor:execute('COMMIT')
    cursor_unbind(self)
    return res, msg
end

-- Proxy method
local function pool_rollback(self)
    local cursor = cursor_bind(self)
    local res, msg = cursor:execute('ROLLBACK')
    cursor_unbind(self)
    return res, msg
end

-- Return dedicated connection, that won't be automatically returned 
-- to pool after transaction finish
local function pool_get(self)
    local cursor = cursor_get(self)
    local reset_sql = 'BEGIN; RESET ALL; COMMIT;'
    if cursor:active() then
        reset_sql = 'ROLLBACK; ' .. reset_sql
    end
    cursor:execute(reset_sql)
    return cursor
end

-- Free binded connection
local function pool_free(self)
    local cursor = cursor_bind(self)
    cursor_unbind(self, true)
end

pool_mt = {
    __index = {
        close = pool_close;
        execute = pool_execute;
        ping = pool_ping;
        begin = pool_begin;
        rollback = pool_rollback;
        commit = pool_commit;
	get = pool_get;
	free = pool_free;
    }
}

return {
    connect = pool_connect;
}
