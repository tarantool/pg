#!/usr/bin/env tarantool

package.path = "../?/init.lua;./?/init.lua"
package.cpath = "../?.so;../?.dylib;./?.so;./?.dylib"

local pg = require('pg')
local json = require('json')
local tap = require('tap')
local f = require('fiber')

local host, port, user, pass, db = string.match(os.getenv('PG') or '',
    "([^:]*):([^:]*):([^:]*):([^:]*):([^:]*)")

local p, msg = pg.pool_create({ host = host, port = port, user = user, pass = pass,
    db = db, raise = false, size = 2 })
if p == nil then error(msg) end

local conn, msg = pg.connect({ host = host, port = port, user = user, pass = pass,
    db = db, raise = false})
if conn == nil then error(msg) end

function test_old_api(t, c)
    t:plan(15)
    t:ok(c ~= nil, "connection")
    -- Add an extension to 'tap' module
    getmetatable(t).__index.q = function(test, stmt, result, ...)
        test:is_deeply({c:execute(stmt, ...)}, {{result}, true},
            ... ~= nil and stmt..' % '..json.encode({...}) or stmt)
    end
    t:ok(c:ping(), "ping")
    if p == nil then
        return
    end
    t:q('SELECT 123::text AS bla, 345', {{ bla = '123', ['?column?'] = 345 }})
    t:q('SELECT -1 AS neg, NULL AS abc', {{ neg = -1 }})
    t:q('SELECT -1.1 AS neg, 1.2 AS pos', {{ neg = -1.1, pos = 1.2 }})
    t:q('SELECT ARRAY[1,2] AS arr, 1.2 AS pos', {{ arr = '{1,2}', pos = 1.2}})
    t:q('SELECT $1 AS val', {{ val = 'abc' }}, 'abc')
    t:q('SELECT $1 AS val', {{ val = 123 }}, 123)
    t:q('SELECT $1 AS val', {{ val = true }}, true)
    t:q('SELECT $1 AS val', {{ val = false }}, false)
    t:q('SELECT $1 AS val, $2 AS num, $3 AS str',
        {{ val = false, num = 123, str = 'abc'}}, false, 123, 'abc')
    t:q('SELECT * FROM (VALUES (1,2), (2,3)) t', {
        { column1 = 1, column2 = 2}, { column1 = 2, column2 = 3}})

    t:test("tx", function(t)
        t:plan(7)
        if not c:execute("CREATE TABLE _tx_test (a int)") then
            return
        end

        t:ok(c:begin(), "begin")
        c:execute("INSERT INTO _tx_test VALUES(10)");
        t:q('SELECT * FROM _tx_test', {{ a  = 10 }})
        t:ok(c:rollback(), "roolback")
        t:q('SELECT * FROM _tx_test', {})

        t:ok(c:begin(), "begin")
        c:execute("INSERT INTO _tx_test VALUES(10)");
        t:ok(c:commit(), "commit")
        t:q('SELECT * FROM _tx_test', {{ a  = 10 }})

        c:execute("DROP TABLE _tx_test")
    end)

    t:test("numeric string binding", function(t)
        t:plan(1)
        if not c:execute([[CREATE TABLE TEST1 ( TEST1_ID SERIAL PRIMARY KEY,
                           TEST1_REQUESTID VARCHAR(32)
                           NOT NULL DEFAULT '' );]]) then
            return
        end
        c:execute('INSERT INTO TEST1 (TEST1_REQUESTID) VALUES ($1)', '12E30')
        t:q('SELECT TEST1_REQUESTID FROM TEST1;',
            {{ test1_requestid = '12E30'}})

        c:execute("DROP TABLE TEST1")
    end)

    local status, reason = pcall(c.execute, c, 'DROP TABLE unknown_table')
    t:like(reason, 'unknown_table', 'error')
end

function test_gc(t, p)
    t:plan(1)
    p:get()
    local c = p:get()
    c = nil
    collectgarbage('collect')
    t:is(p.queue:count(), p.size, 'gc connections')
end

function test_conn_fiber1(c, q)
    for i = 1, 10 do
        c:execute('SELECT pg_sleep(0.05)')
    end
    q:put(true)
end

function test_conn_fiber2(c, q)
    for i = 1, 25 do
        c:execute('SELECT pg_sleep(0.02)')
    end
    q:put(true)
end

function test_conn_concurrent(t, p)
    t:plan(1)
    local c = p:get()
    local q = f.channel(2)
    local t1 = f.time()
    f.create(test_conn_fiber1, c, q)
    f.create(test_conn_fiber2, c, q)
    q:get()
    q:get()
    p:put(c)
    t:ok(f.time() - t1 >= 0.95, 'concurrent connections')
end

function test_pg_int64(t, p)
    t:plan(1)
    conn = p:get()
    conn:execute('create table int64test (id bigint)')
    conn:execute('insert into int64test values(1234567890123456789)')
    local r, m = conn:execute('select id from int64test')
    conn:execute('drop table int64test')
    t:ok(r[1][1]['id'] == 1234567890123456789LL, 'int64 test')
    p:put(conn)
end

function test_pg_decimal(t, p)
    t:plan(8)

    -- Setup
    conn = p:get()
    t:isnt(conn, nil, 'connection is established')
    local num = 4500
    conn:execute('CREATE TABLE dectest (num NUMERIC(7,2))')
    conn:execute(('INSERT INTO dectest VALUES(%d)'):format(num))

    local res, r, _
    -- dec_cast is 'n'
    t:is(conn.dec_cast, 'n', 'decimal casting type is "n" by default')
    r, _ = conn:execute('SELECT num FROM dectest')
    res = r[1][1]['num']
    t:is(type(res), 'number', 'type is "number"')
    t:is(res, num, 'decimal number is correct')
    -- dec_cast is 's'
    conn.dec_cast = 's'
    r, _ = conn:execute('SELECT num FROM dectest')
    res = r[1][1]['num']
    t:is(type(res), 'string', 'type is "string"')
    t:is(res, '4500.00', 'decimal number is correct')
    -- dec_cast is 'd'
    conn.dec_cast = 'd'
    r, _ = conn:execute('SELECT num FROM dectest')
    res = r[1][1]['num']
    t:is(type(res), 'cdata', 'type is "decimal"')
    t:is(res, num, 'decimal number is correct')

    -- Teardown
    conn:execute('DROP TABLE dectest')
    p:put(conn)
end

tap.test('connection old api', test_old_api, conn)
local pool_conn = p:get()
tap.test('connection old api via pool', test_old_api, pool_conn)
p:put(pool_conn)
tap.test('test collection connections', test_gc, p)
tap.test('connection concurrent', test_conn_concurrent, p)
tap.test('int64', test_pg_int64, p)
tap.test('decimal', test_pg_decimal, p)
p:close()
