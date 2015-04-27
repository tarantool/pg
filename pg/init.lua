-- sql.lua (internal file)

local fiber = require('fiber')
local driver = require('pg.driver')

local conn_mt

-- pg.connect({host = host, port = port, user = user, pass = pass, db = db,
--             raise = false })
-- pg.connect({connstring = 'host=host, port=port', raise = false})
-- @param connstring - PostgreSQL connection string
--  http://www.postgresql.org/docs/9.4/static/libpq-connect.html#LIBPQ-CONNSTRING
-- @param debug if option raise set in 'false' and an error will be happened
--   the function will return 'nil' as the first variable and text of error as
--   the second value.
-- @return connector to database or throw error
local function connect(opts)
    opts = opts or {}
    local connstring
    if opts.connstring then
        connstring = opts.connstring
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
        connstring = table.concat(connb)
    end

    local s, c = driver.connect(connstring)
    if s == nil then
        if opts.raise then
            error(c)
        end
        return nil, c
    end

    return setmetatable({
        driver = c,

        -- connection variables
        host        = opts.host,
        port        = opts.port,
        user        = opts.user,
        pass        = opts.pass,
        db          = opts.db,
        connstring  = connstring,

        -- private variables
        queue       = {},
        processing  = false,

        -- throw exception if error
        raise       = opts.raise
    }, conn_mt)
end

--
-- Close connection
--
local function close(self)
    return self.driver:close()
end

-- example:
-- local tuples, arows, txtst = db:execute(sql, args)
--   tuples - a table of tuples (tables)
--   arows  - count of affected rows
--   txtst  - text status (Postgresql specific)

-- the method throws exception by default.
-- user can change the behaviour by set 'connection.raise'
-- attribute to 'false'
-- in the case it will return negative arows if error and
-- txtst will contain text of error
local function execute(self, sql, ...)
    -- waits until connection will be free
    while self.processing do
        self.queue[ fiber.id() ] = fiber.channel()
        self.queue[ fiber.id() ]:get()
        self.queue[ fiber.id() ] = nil
    end
    self.processing = true
    local status, reason = pcall(self.driver.execute, self.driver, sql, ...)
    self.processing = false
    if not status then
        if self.raise then
            error(reason)
        end
        return nil, reason
    end

    -- wakeup one waiter
    for fid, ch in pairs(self.queue) do
        ch:put(true, 0)
        self.queue[ fid ] = nil
        break
    end
    return reason
end

-- pings database
-- returns true if success. doesn't throw any errors
local function ping(self)
    local raise = self.raise
    self.raise = false
    local res = self:execute('SELECT 1 AS code')
    self.raise = raise
    return res ~= nil and res[1].code == 1
end

-- begin transaction
local function begin(self)
    return self:execute('BEGIN') ~= nil
end

-- commit transaction
local function commit(self)
    return self:execute('COMMIT') ~= nil
end

-- rollback transaction
local function rollback(self)
    return self:execute('ROLLBACK') ~= nil
end

conn_mt = {
    __index = {
        close = close;
        execute = execute;
        ping = ping;
        begin = begin;
        rollback = rollback;
        commit = commit;
    }
}

return {
    connect = connect;
}
