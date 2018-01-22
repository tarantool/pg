package = 'pg'
version = '2.0.1-1'
source  = {
    url = 'git://github.com/tarantool/pg.git',
    tag = '2.0.1',
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
    type = 'cmake';
    variables = {
        CMAKE_BUILD_TYPE="RelWithDebInfo";
        TARANTOOL_INSTALL_LIBDIR="$(LIBDIR)";
        TARANTOOL_INSTALL_LUADIR="$(LUADIR)";
    };
}
-- vim: syntax=lua
