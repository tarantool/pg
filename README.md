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
 - `connstring` (mutual exclusive with host, port, user, pass, db) - PostgreSQL
   [connection string][PQconnstring]
 - `raise` = false - raise an exceptions instead of returning nil, reason in
   all API functions

*Returns*:

 - `connection ~= nil` on success
 - `nil, reason` on error if `raise` is false
 - `error(reason)` on error if `raise` is true

### `conn:execute(statement, ...)`

Execute a statement with arguments in the current transaction.

*Returns*:
 - `{ { column1 = value, column2 = value }, ... }` on success
 - `nil, reason` on error if `raise` is false
 - `error(reason)` on error if `raise` is true

*Example*:
```
tarantool> conn:execute("SELECT ? AS a, 'xx' AS b", 42)
---
- - a: 42
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

## See Also

 * [Tests][]
 * [Tarantool][]
 * [Tarantool Rocks][TarantoolRocks]

[Tarantool]: http://github.com/tarantool/tarantool
[Tests]: https://github.com/tarantool/pg/tree/master/test
[PQconnstring]: http://www.postgresql.org/docs/9.4/static/libpq-connect.html#LIBPQ-CONNSTRING
[TarantoolRocks]: https://github.com/tarantool/rocks
