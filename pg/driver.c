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

#include <stdint.h>

static int
save_pushstring_wrapped(struct lua_State *L)
{
	char *str = (char *)lua_topointer(L, 1);
	lua_pushstring(L, str);
	return 1;
}

static int
safe_pushstring(struct lua_State *L, char *str)
{
	lua_pushcfunction(L, save_pushstring_wrapped);
	lua_pushlightuserdata(L, str);
	return lua_pcall(L, 1, 1, 0);
}

static inline PGconn *
lua_check_pgconn(struct lua_State *L, int index)
{
	PGconn *conn = *(PGconn **) luaL_checkudata(L, index, "pg");
	if (conn == NULL)
		luaL_error(L, "Driver fatal error (No connection)");
	return conn;
}

/**
 * put query result tuples to given table on lua stack
 */
static int
safe_pg_parsetuples(struct lua_State *L)
{
	PGresult *r = (PGresult *)lua_topointer(L, 1);
	int rows = PQntuples(r);
	int cols = PQnfields(r);
	size_t pos = lua_objlen(L, -1) + 1;
	int row;
	for (row = 0; row < rows; ++row, ++pos) {
		lua_pushnumber(L, pos);
		lua_newtable(L);
		int col;
		for (col = 0; col < cols; ++col) {
			if (PQgetisnull(r, row, col))
				continue;
			lua_pushstring(L, PQfname(r, col));
			const char *val = PQgetvalue(r, row, col);
			int len = PQgetlength(r, row, col);

			switch (PQftype(r, col)) {
				case INT2OID:
				case INT4OID:
				case NUMERICOID: {
					lua_pushlstring(L, val, len);
					double v = lua_tonumber(L, -1);
					lua_pop(L, 1);
					lua_pushnumber(L, v);
					break;
				}
				case INT8OID: {
					long long v = strtoll(val, NULL, 10);
					luaL_pushint64(L, v);
					break;
				}
				case BOOLOID:
					if (*val == 't' || *val == 'T')
						lua_pushboolean(L, 1);
					else
						lua_pushboolean(L, 0);
					break;
				default:
					lua_pushlstring(L, val, len);
					break;
			}
			lua_settable(L, -3);
		}
		lua_settable(L, -3);
	}
	return 0;
}

/**
 * push query execution status to lua stack
 */
static int
safe_pg_parsestatus(struct lua_State *L)
{
	PGresult *r = (PGresult *)lua_topointer(L, 1);
	lua_newtable(L);
	lua_pushstring(L, "status");
	if (*PQcmdTuples(r) == 0) {
		lua_pushnumber(L, 0);
	} else {
		lua_pushstring(L, PQcmdTuples(r));
		double v = lua_tonumber(L, -1);
		lua_pop(L, 1);
		lua_pushnumber(L, v);
	}
	lua_settable(L, -3);
	lua_pushstring(L, "message");
	lua_pushstring(L, PQcmdStatus(r));
	lua_settable(L, -3);
	return 1;
}

/**
 * push error info to lua table
 */
static int
safe_pg_resulterror(struct lua_State *L)
{
	int code = lua_tointeger(L, 1);
	lua_newtable(L);
	lua_pushstring(L, "status");
	lua_pushinteger(L, code);
	lua_settable(L, -3);
	lua_pushstring(L, "message");
	lua_pushvalue(L, 2);
	lua_settable(L, -3);
	return 1;
}

/**
 * load result fom postgres into lua
 */
static int
lua_pg_resultget(struct lua_State *L)
{
	PGconn *conn = lua_check_pgconn(L, 1);

	PGresult *res = PQgetResult(conn);
	if (!res) {
		lua_pushinteger(L, 1);
		lua_pushnil(L);
		return 2;
	}
	int fail = 0;
	int status;
	switch (status = PQresultStatus(res)){
	case PGRES_TUPLES_OK:
	case PGRES_SINGLE_TUPLE:
		lua_pushinteger(L, 1);
		lua_pushcfunction(L, safe_pg_parsetuples);
		lua_pushlightuserdata(L, res);
		lua_pushvalue(L, 2);
		if ((fail = lua_pcall(L, 2, 0, 0)))
			break;
		lua_pushcfunction(L, safe_pg_parsestatus);
		lua_pushlightuserdata(L, res);
		fail = lua_pcall(L, 1, 1, 0);
		break;
	case PGRES_COMMAND_OK:
		lua_pushinteger(L, 1);
		lua_pushcfunction(L, safe_pg_parsestatus);
		lua_pushlightuserdata(L, res);
		fail = lua_pcall(L, 1, 1, 0);
		break;
	case PGRES_FATAL_ERROR:
	case PGRES_EMPTY_QUERY:
	case PGRES_NONFATAL_ERROR:
		lua_pushinteger(L, (PQstatus(conn) == CONNECTION_BAD) ? -1: 0);
		lua_pushcfunction(L, safe_pg_resulterror);
		lua_pushinteger(L, status);
		if ((fail = safe_pushstring(L, PQerrorMessage(conn))))
			break;
		fail = lua_pcall(L, 2, 1, 0);
		break;
	default:
		lua_pushinteger(L, -1);
		lua_pushcfunction(L, safe_pg_resulterror);
		lua_pushinteger(L, status);
		if ((fail = safe_pushstring(L, "Unwanted result status")))
			break;
		fail = lua_pcall(L, 2, 1, 0);
	}

	PQclear(res);
	if (fail)
		return lua_error(L);
	return 2;
}

/**
 * check for available results
 */
static int
lua_pg_resultavailable(struct lua_State *L)
{
	PGconn *conn = lua_check_pgconn(L, 1);
	if (PQconsumeInput(conn) != 1)
	{
		if (PQstatus(conn) == CONNECTION_BAD)
			lua_pushinteger(L, -1);
		else
			lua_pushinteger(L, 0);
		lua_pushstring(L, PQerrorMessage(conn));
		return 2;
	}
	lua_pushnumber(L, 1);
	lua_pushboolean(L, PQisBusy(conn) == 0);
	return 2;
}

/**
 * start query execution
 */
static int
lua_pg_executeasync(struct lua_State *L)
{
	PGconn *conn = lua_check_pgconn(L, 1);
	const char *sql = lua_tostring(L, 2);

	int paramCount = lua_gettop(L) - 2;

	const char **paramValues = NULL;
	int  *paramLengths = NULL;
	Oid *paramTypes = NULL;
	int res = 0;
	if (paramCount > 0) {
		/* Allocate memory for params using lua_newuserdata */
		char *buf = (char *) lua_newuserdata(L, paramCount *
			(sizeof(*paramValues) + sizeof(*paramLengths) +
			 sizeof(*paramTypes)));

		paramValues = (const char **) buf;
		buf += paramCount * sizeof(*paramValues);
		paramLengths = (int *) buf;
		buf += paramCount * sizeof(*paramLengths);
		paramTypes = (Oid *) buf;

		int idx;
		for (idx = 0; idx < paramCount; ++idx) {
			if (lua_isnil(L, idx + 3)) {
				paramValues[idx] = NULL;
				paramLengths[idx] = 0;
				paramTypes[idx] = 0;
				continue;
			}

			if (lua_isboolean(L, idx + 3)) {
				static const char pg_true[] = "t";
				static const char pg_false[] = "f";
				paramValues[idx] =
					lua_toboolean(L, idx + 3) ?
					pg_true : pg_false;
				paramLengths[idx] = 1;
				paramTypes[idx] = BOOLOID;
				continue;
			}

			if (lua_isnumber(L, idx + 3)) {
				size_t len;
				paramValues[idx] = lua_tolstring(L, idx + 3, &len);
				paramLengths[idx] = len;
				paramTypes[idx] = NUMERICOID;
				continue;
			}

			//we will pass all other types as strings
			size_t len;
			paramValues[idx] = lua_tolstring(L, idx + 3, &len);
			paramLengths[idx] = len;
			paramTypes[idx] = TEXTOID;
		}
		res = PQsendQueryParams(conn, sql, paramCount, paramTypes,
			paramValues, paramLengths, NULL, 0);
	}
	else
	{
		res = PQsendQuery(conn, sql);
	}
	if (res == -1) {
		if (PQstatus(conn) == CONNECTION_BAD)
			lua_pushinteger(L, -1);
		else
			lua_pushinteger(L, 0);
		lua_pushstring(L, PQerrorMessage(conn));
		return 2;
	}
	lua_pushinteger(L, 1);
	lua_pushinteger(L, PQsocket(conn));

	return 2;
}

/**
 * test that connection has active transaction
 */
static int
lua_pg_transaction_active(struct lua_State *L)
{
	PGconn *conn = lua_check_pgconn(L, 1);
	PGTransactionStatusType status;
	switch (status = PQtransactionStatus(conn)){
		case PQTRANS_IDLE:
		case PQTRANS_ACTIVE:
		case PQTRANS_INTRANS:
		case PQTRANS_INERROR:
			lua_pushinteger(L, 1);
			lua_pushboolean(L, status != PQTRANS_IDLE);
			return 2;
		default:
			lua_pushinteger(L, -1);
			lua_pushstring(L, PQerrorMessage(conn));
			return 2;
	}
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
	int fail = safe_pushstring(L, (char *)s);
	free((void *)s);
	return fail ? lua_error(L) : 1;
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
	int fail = safe_pushstring(L, (char *)s);
	free((void *)s);
	return fail ? lua_error(L) : 1;
}

/**
 * start connection to postgresql
 */
static int
lua_pg_connect(struct lua_State *L)
{
	if (lua_gettop(L) < 1 || !lua_isstring(L, 1))
		luaL_error(L, "Usage: pg.connect(connstring)");

	const char *constr = lua_tostring(L, 1);
	PGconn *conn = NULL;

	conn = PQconnectStart(constr);
	if (!conn) {
		lua_pushinteger(L, -1);
		lua_pushstring(L, "Can't allocate PG connection structure");
		return 2;
	}

	if (PQstatus(conn) == CONNECTION_BAD) {
		lua_pushinteger(L, -1);
		int fail = safe_pushstring(L, PQerrorMessage(conn));
		PQfinish(conn);
		return fail ? lua_error(L) : 2;
	}

	lua_pushinteger(L, 1);
	PGconn **ptr = (PGconn **)lua_newuserdata(L, sizeof(conn));
	*ptr = conn;
	luaL_getmetatable(L, "pg");
	lua_setmetatable(L, -2);
	return 2;
}

/**
 * poll postgresql connection
 */
static int
lua_pg_connpoll(struct lua_State *L)
{
	PGconn *conn = lua_check_pgconn(L, 1);
	PostgresPollingStatusType status = PQconnectPoll(conn);
	if (status == PGRES_POLLING_OK)
	{
		PQsetNoticeProcessor(conn, pg_notice, NULL);
		lua_pushnumber(L, 0);
		return 1;
	}
	if (status == PGRES_POLLING_READING)
	{
		lua_pushnumber(L, 1);
		lua_pushnumber(L, PQsocket(conn));
		return 2;
	}
	if (status == PGRES_POLLING_WRITING)
	{
		lua_pushnumber(L, 2);
		lua_pushnumber(L, PQsocket(conn));
		return 2;
	}
	//Connection failed
	lua_pushinteger(L, -1);
	int fail = safe_pushstring(L, PQerrorMessage(conn));
	PQfinish(conn);
	return fail ? lua_error(L) : 2;
}

LUA_API int
luaopen_pg_driver(lua_State *L)
{
	static const struct luaL_reg methods [] = {
		{"executeasync",lua_pg_executeasync},
		{"resultavailable", lua_pg_resultavailable},
		{"resultget", lua_pg_resultget},
		{"quote",	lua_pg_quote},
		{"quote_ident",	lua_pg_quote_ident},
		{"close",	lua_pg_close},
		{"connpoll",	lua_pg_connpoll},
		{"active", lua_pg_transaction_active},
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
		{"connect", lua_pg_connect},
		{NULL, NULL}
	};
	luaL_register(L, NULL, meta);
	return 1;
}
