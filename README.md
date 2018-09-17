# pg - PostgreSQL connector for [Tarantool][]

[![Build Status](https://travis-ci.org/tarantool/pg.png?branch=master)](https://travis-ci.org/tarantool/pg)

## Getting Started

### Prerequisites

 * Tarantool 1.6.5+ with header files (tarantool && tarantool-dev packages)
 * PostgreSQL 8.1+ header files (libpq-dev package)

### Installation

Clone repository and then build it using CMake:

``` bash
git clone https://github.com/tarantool/pg.git
cd pg && cmake . -DCMAKE_BUILD_TYPE=RelWithDebugInfo
make
make install
```

You can also use LuaRocks:

``` bash
luarocks install https://raw.githubusercontent.com/tarantool/pg/master/pg-scm-1.rockspec --local
```

See [tarantool/rocks][TarantoolRocks] for LuaRocks configuration details.

### Usage

``` lua
local pg = require('pg')
local conn = pg.connect({host = localhost, user = 'user', pass = 'pass', db = 'db'})
local tuples = conn:execute("SELECT ? AS a, 'xx' AS b", 42))
conn:begin()
conn:execute("INSERT INTO test VALUES(1, 2, 3)")
conn:commit()
```

## API Documentation

### `conn = pg:connect(opts = {})`

Connect to a database.

*Options*:

 - `host` - a hostname to connect
 - `port` - a port numner to connect
 - `user` - username
 - `pass` or `password` - a password
 - `db` - a database name
 - `conn_string` (mutual exclusive with host, port, user, pass, db) - PostgreSQL
   [connection string][PQconnstring]

*Returns*:

 - `connection ~= nil` on success
 - `error(reason)` on error

### `conn:execute(statement, ...)`

Execute a statement with arguments in the current transaction.

*Returns*:
 - `{ { { column1 = value, column2 = value }, ... }, { {column1 = value, ... }, ...}, ...}, true` on success
 - `error(reason)` on error

*Example*:
```
tarantool> conn:execute("SELECT ? AS a, 'xx' AS b", 42)
---
- - - a: 42
      b: xx
    ...
```

### `conn:begin()`

Begin a transaction.

*Returns*: `true`

### `conn:commit()`

Commit current transaction.

*Returns*: `true`

### `conn:rollback()`

Rollback current transaction.

*Returns*: `true`

### `conn:ping()`

Execute a dummy statement to check that connection is alive.

*Returns*:

 - `true` on success
 - `false` on failure

#### `pool = pg.pool_create(opts = {})`

Create a connection pool with count of size established connections.

*Options*:

 - `host` - hostname to connect to
 - `port` - port number to connect to
 - `user` - username
 - `password` - password
 - `db` - database name
 - `size` - count of connections in pool

*Returns*

 - `pool ~=nil` on success
 - `error(reason)` on error

### `conn = pool:get()`

Get a connection from pool. Reset connection before returning it. If connection
is broken then it will be reestablished. If there is no free connections then
calling fiber will sleep until another fiber returns some connection to pool.

*Returns*:

 - `conn ~= nil`
 
### `pool:put(conn)`

Return a connection to connection pool.

*Options*

 - `conn` - a connection

## Comments

All calls to connections api will be serialized, so it should to be safe to
use one connection from some count of fibers. But you should understand,
that you can have some unwanted behavior across db calls, for example if
another fiber 'injects' some sql between two your calls.

# See Also

 * [Tests][]
 * [Tarantool][]
 * [Tarantool Rocks][TarantoolRocks]

[Tarantool]: http://github.com/tarantool/tarantool
[Tests]: https://github.com/tarantool/pg/tree/master/test
[PQconnstring]: http://www.postgresql.org/docs/9.4/static/libpq-connect.html#LIBPQ-CONNSTRING
[TarantoolRocks]: https://github.com/tarantool/rocks
