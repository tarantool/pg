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
external_dependencies = {
    TARANTOOL = {
        header = "tarantool/tarantool.h"
    },
    POSTGRESQL = {
        header = "postgresql/libpq-fe.h",
        library = "pq"
    }
}
build = {
    type = 'builtin',
    modules = {
        ['pg.driver'] = {
            sources = 'pg/driver.c',
            incdirs = {
                "$(TARANTOOL_INCDIR)/tarantool",
                "$(POSTGRESQL_INCDIR)/postgresql"
            },
            libdir = "$(POSTGRESQL_LIBDIR)",
            libraries = "pq"
        },
        ['pg.init'] = 'pg/init.lua',
    }
}
-- vim: syntax=lua
