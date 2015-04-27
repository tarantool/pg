package = 'pg'
version = 'scm-1'
source  = {
    url    = 'git://github.com/tarantool/pg.git',
    branch = 'master',
}
description = {
    summary  = "PostgreSQL connector for Tarantool",
    homepage = 'https://github.com/tarantool/pg',
    license  = 'BSD',
}
dependencies = {
    'lua >= 5.1'
}
build = {
    type = 'builtin',
    modules = {
        ['pg.driver'] = 'pg/driver.c',
        ['pg.init'] = 'pg/init.lua',
    }
}
-- vim: syntax=lua
