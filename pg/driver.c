/*
 * Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following
 * conditions are met:
 *
 * 1. Redistributions of source code must retain the above
 *    copyright notice, this list of conditions and the
 *    following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above
 *    copyright notice, this list of conditions and the following
 *    disclaimer in the documentation and/or other materials
 *    provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY <COPYRIGHT HOLDER> ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * <COPYRIGHT HOLDER> OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 * BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */
#include <module.h>

#include <stddef.h>
#include <assert.h>
#include <stdlib.h>
#include <errno.h>

#include <lua.h>
#include <lauxlib.h>

#include <libpq-fe.h>
/* PostgreSQL types (see catalog/pg_type.h) */
#define INT2OID 21
#define INT4OID 23
#define INT8OID 20
#define NUMERICOID 1700
#define BOOLOID 16
#define TEXTOID 25

static inline PGconn *
lua_check_pgconn_nothrow(struct lua_State *L, int index)
{
	return *(PGconn **) luaL_checkudata(L, index, "pg");
}

static inline PGconn *
lua_check_pgconn(struct lua_State *L, int index)
{
	PGconn *conn = lua_check_pgconn_nothrow(L, index);
	if (conn == NULL)
		luaL_error(L, "Attempt to use closed connection");
	return conn;
}

/** do execute request (is run in the other thread) */
static ssize_t
pg_exec(va_list ap)
{
	PGconn *conn			= va_arg(ap, PGconn*);
	const char *sql			= va_arg(ap, const char*);
	int count			= va_arg(ap, int);
	Oid *paramTypes			= va_arg(ap, Oid*);
	const char **paramValues	= va_arg(ap, const char**);
	const int *paramLengths		= va_arg(ap, int*);
	const int *paramFormats		= va_arg(ap, int*);
	PGresult **res			= va_arg(ap, PGresult**);

	*res = PQexecParams(conn, sql,
		count, paramTypes, paramValues, paramLengths, paramFormats, 0);
	return 0;
}


/** push query result into lua stack */
static int
lua_pg_pushresult(struct lua_State *L, PGresult *r)
{
	if (!r)
		luaL_error(L, "PG internal error: zero rults");

	switch(PQresultStatus(r)) {
	case PGRES_COMMAND_OK:
		lua_newtable(L);
		if (*PQcmdTuples(r) == 0) {
			lua_pushnumber(L, 0);
		} else {
			lua_pushstring(L, PQcmdTuples(r));
			double v = lua_tonumber(L, -1);
			lua_pop(L, 1);
			lua_pushnumber(L, v);
		}
		lua_pushstring(L, PQcmdStatus(r));
		PQclear(r);
		return 3;

	case PGRES_TUPLES_OK:
		break;

	case PGRES_BAD_RESPONSE:
		PQclear(r);
		luaL_error(L, "Broken postgresql response");
		return 0;

	case PGRES_FATAL_ERROR:
	case PGRES_NONFATAL_ERROR:
	case PGRES_EMPTY_QUERY:
		lua_pushstring(L, PQresultErrorMessage(r));
		PQclear(r);
		luaL_error(L, "%s", lua_tostring(L, -1));
		return 0;

	default:
		luaL_error(L, "pg: unsupported result type");
		return 0;
	}

	lua_newtable(L);
	int count = PQntuples(r);
	int cols = PQnfields(r);
	int i;
	for (i = 0; i < count; i++) {
		lua_pushnumber(L, i + 1);
		lua_newtable(L);
		int j;
		for (j = 0; j < cols; j++) {
			if (PQgetisnull(r, i, j))
				continue;
			lua_pushstring(L, PQfname(r, j));
			const char *s = PQgetvalue(r, i, j);
			int len = PQgetlength(r, i, j);

			switch (PQftype(r, j)) {
				case INT2OID:
				case INT4OID:
				case INT8OID:
				case NUMERICOID: {
					lua_pushlstring(L, s, len);
					double v = lua_tonumber(L, -1);
					lua_pop(L, 1);
					lua_pushnumber(L, v);
					break;
				}
				case BOOLOID:
					if (*s == 't' || *s == 'T')
						lua_pushboolean(L, 1);
					else
						lua_pushboolean(L, 0);
					break;
				default:
					lua_pushlstring(L, s, len);
					break;
			}
			lua_settable(L, -3);
		}
		lua_settable(L, -3);
	}

	if (*PQcmdTuples(r) == 0) {
		lua_pushnumber(L, 0);
	} else {
		lua_pushstring(L, PQcmdTuples(r));
		double v = lua_tonumber(L, -1);
		lua_pop(L, 1);
		lua_pushnumber(L, v);
	}
	lua_pushstring(L, PQcmdStatus(r));
	PQclear(r);
	return 3;
}


/** execute method */
static int
lua_pg_execute(struct lua_State *L)
{
	PGconn *conn = lua_check_pgconn(L, 1);
	const char *sql = lua_tostring(L, 2);

	int count = lua_gettop(L) - 2;
	const char **paramValues = NULL;
	int  *paramLengths = NULL;
	int  *paramFormats = NULL;
	Oid *paramTypes = NULL;

	if (count > 0) {
		/* Allocate memory for params using lua_newuserdata */
		char *buf = (char *) lua_newuserdata(L, count *
			(sizeof(*paramValues) + sizeof(*paramLengths) +
			 sizeof(*paramFormats) + sizeof(*paramTypes)));

		paramValues = (const char **) buf;
		buf += count * sizeof(*paramValues);
		paramLengths = (int *) buf;
		buf += count * sizeof(*paramLengths);
		paramFormats = (int *) buf;
		buf += count * sizeof(*paramFormats);
		paramTypes = (Oid *) buf;
		buf += count * sizeof(*paramTypes);

		int i, j, idx;
		for(i = 0, idx = 3; i < count; i++, idx++) {
			if (lua_isnil(L, idx)) {
				paramValues[i] = NULL;
				paramLengths[i] = 0;
				paramFormats[i] = 0;
				paramTypes[i] = 0;
				continue;
			}

			if (lua_isboolean(L, idx)) {
				int v = lua_toboolean(L, idx);
				static const char pg_true[] = "t";
				static const char pg_false[] = "f";
				paramValues[i] = v ? pg_true : pg_false;
				paramLengths[i] = 1;
				paramFormats[i] = 0;
				paramTypes[i] = BOOLOID;
				continue;
			}

			size_t len;
			const char *s = lua_tolstring(L, idx, &len);

			if (lua_isnumber(L, idx)) {
				paramTypes[i] = NUMERICOID;
				paramValues[i] = s;
				paramLengths[i] = len;
				paramFormats[i] = 0;
				continue;
			}

			paramValues[i] = s;
			paramLengths[i] = len;
			paramFormats[i] = 0;
			paramTypes[i] = TEXTOID;
		}

		/* transform sql placeholders */
		luaL_Buffer b;
		luaL_buffinit(L, &b);
		char num[10];
		for (i = 0, j = 1; sql[i]; i++) {
			if (sql[i] != '?') {
				luaL_addchar(&b, sql[i]);
				continue;
			}
			luaL_addchar(&b, '$');

			snprintf(num, 10, "%d", j++);
			luaL_addstring(&b, num);
		}
		luaL_pushresult(&b);
		sql = lua_tostring(L, -1);
	}

	static int running = 0;

	PGresult *res = NULL;
	/* PQconn can't be shared between threads */
	assert(!running); /* checked by Lua */
	running = 1;
	if (coio_call(pg_exec, conn,
			sql, count, paramTypes, paramValues,
			paramLengths, paramFormats, &res) == -1) {

		luaL_error(L, "Can't execute sql: %s",
			strerror(errno));
	}
	running = 0;

	lua_settop(L, 0);
	return lua_pg_pushresult(L, res);
}

/**
 * close connection
 */
static int
lua_pg_close(struct lua_State *L)
{
	PGconn **pconn = (PGconn **) luaL_checkudata(L, 1, "pg");
	if (*pconn == NULL) {
		lua_pushboolean(L, 0);
		return 1;
	}
	PQfinish(*pconn);
	*pconn = NULL;
	lua_pushboolean(L, 1);
	return 1;
}

/**
 * collect connection
 */
static int
lua_pg_gc(struct lua_State *L)
{
	PGconn **pconn = (PGconn **) luaL_checkudata(L, 1, "pg");
	if (*pconn != NULL)
		PQfinish(*pconn);
	*pconn = NULL;
	return 0;
}

static int
lua_pg_tostring(struct lua_State *L)
{
	PGconn *conn = lua_check_pgconn(L, 1);
	lua_pushfstring(L, "PQconn: %p", conn);
	return 1;
}

/**
 * prints warnings from Postgresql into tarantool log
 */
static void
pg_notice(void *arg, const char *message)
{
	say_info("Postgresql: %s", message);
	(void)arg;
}

/**
 * do connect to postgresql (is run in the other thread)
 */
static ssize_t
pg_connect(va_list ap)
{
	const char *constr = va_arg(ap, const char*);
	PGconn **conn = va_arg(ap, PGconn**);
	*conn = PQconnectdb(constr);
	if (*conn)
		PQsetNoticeProcessor(*conn, pg_notice, NULL);
	return 0;
}

/**
 * quote variable
 */
static int
lua_pg_quote(struct lua_State *L)
{
	if (lua_gettop(L) < 2) {
		lua_pushnil(L);
		return 1;
	}
	PGconn *conn = lua_check_pgconn(L, 1);
	size_t len;
	const char *s = lua_tolstring(L, -1, &len);

	s = PQescapeLiteral(conn, s, len);

	if (!s)
		luaL_error(L, "Can't allocate memory");
	lua_pushstring(L, s);
	free((void *)s);
	return 1;
}

/**
 * quote identifier
 */
static int
lua_pg_quote_ident(struct lua_State *L)
{
	if (lua_gettop(L) < 2) {
		lua_pushnil(L);
		return 1;
	}
	PGconn *conn = lua_check_pgconn(L, 1);
	size_t len;
	const char *s = lua_tolstring(L, -1, &len);

	s = PQescapeIdentifier(conn, s, len);

	if (!s)
		luaL_error(L, "Can't allocate memory");
	lua_pushstring(L, s);
	free((void *)s);
	return 1;
}

/**
 * connect to postgresql
 */
static int
lbox_net_pg_connect(struct lua_State *L)
{
	if (lua_gettop(L) < 1 || !lua_isstring(L, 1))
		luaL_error(L, "Usage: pg.connect(connstring)");

	const char *constr = lua_tostring(L, 1);
	PGconn *conn = NULL;

	if (coio_call(pg_connect, constr, &conn) == -1) {
		luaL_error(L, "Can't connect to postgresql: %s",
			strerror(errno));
	}

	if (PQstatus(conn) != CONNECTION_OK) {
		luaL_Buffer b;
		luaL_buffinit(L, &b);
		luaL_addstring(&b, PQerrorMessage(conn));
		luaL_pushresult(&b);
		PQfinish(conn);
		lua_error(L);
	}

	lua_pushboolean(L, 1);
	PGconn **ptr = (PGconn **)lua_newuserdata(L, sizeof(conn));
	*ptr = conn;
	luaL_getmetatable(L, "pg");
	lua_setmetatable(L, -2);

	return 2;
}

LUA_API int
luaopen_pg_driver(lua_State *L)
{
	static const struct luaL_reg methods [] = {
		{"execute",	lua_pg_execute},
		{"quote",	lua_pg_quote},
		{"quote_ident",	lua_pg_quote_ident},
		{"close",	lua_pg_close},
		{"__tostring",	lua_pg_tostring},
		{"__gc",	lua_pg_gc},
		{NULL, NULL}
	};

	luaL_newmetatable(L, "pg");
	lua_pushvalue(L, -1);
	luaL_register(L, NULL, methods);
	lua_setfield(L, -2, "__index");
	lua_pushstring(L, "pg");
	lua_setfield(L, -2, "__metatable");
	lua_pop(L, 1);

	lua_newtable(L);
	static const struct luaL_reg meta [] = {
		{"connect", lbox_net_pg_connect},
		{NULL, NULL}
	};
	luaL_register(L, NULL, meta);
	return 1;
}
