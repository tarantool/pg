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
#include <stddef.h>
#include <assert.h>
#include <stdlib.h>
#include <errno.h>

#include <lua.h>
#include <lauxlib.h>

#include <libpq-fe.h>
#include <pg_config.h>
/* PostgreSQL types (see catalog/pg_type.h) */
#define INT2OID 21
#define INT4OID 23
#define INT8OID 20
#define NUMERICOID 1700
#define BOOLOID 16
#define TEXTOID 25

#include <stdint.h>

#undef PACKAGE_VERSION
#include <module.h>

/**
 * The fallthrough attribute with a null statement serves as a fallthrough
 * statement. It hints to the compiler that a statement that falls through
 * to another case label, or user-defined label in a switch statement is
 * intentional and thus the -Wimplicit-fallthrough warning must not trigger.
 * The fallthrough attribute may appear at most once in each attribute list,
 * and may not be mixed with other attributes. It can only be used in a switch
 * statement (the compiler will issue an error otherwise), after a preceding
 * statement and before a logically succeeding case label, or user-defined
 * label.
 */
#if defined(__cplusplus) && __has_cpp_attribute(fallthrough)
#  define FALLTHROUGH [[fallthrough]]
#elif __has_attribute(fallthrough) || (defined(__GNUC__) && __GNUC__ >= 7)
#  define FALLTHROUGH __attribute__((fallthrough))
#else
#  define FALLTHROUGH
#endif

struct dec_opt {
	char cast;
	int dnew_index;
};
typedef struct dec_opt dec_opt_t;

/**
 * Infinity timeout from tarantool_ev.c. I mean, this should be in
 * a module.h file.
 */
#define TIMEOUT_INFINITY 365 * 86400 * 100.0
static const char pg_driver_label[] = "__tnt_pg_driver";

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
	PGconn **conn_p = (PGconn **)luaL_checkudata(L, index, pg_driver_label);
	if (conn_p == NULL || *conn_p == NULL)
		luaL_error(L, "Driver fatal error (closed connection "
			   "or not a connection)");
	return *conn_p;
}

/**
 * Push native lua error with code -3
 */
static int
lua_push_error(struct lua_State *L)
{
	lua_pushnumber(L, -3);
	lua_insert(L, -2);
	return 2;
}

/**
 * Parse pg values to lua
 */
static int
parse_pg_value(struct lua_State *L, PGresult *res, int row, int col, dec_opt_t *dopt)
{
	if (PQgetisnull(res, row, col))
		return false;
	// Procedure called in pcall environment, don't use safe_pushstring
	lua_pushstring(L, PQfname(res, col));
	const char *val = PQgetvalue(res, row, col);
	int len = PQgetlength(res, row, col);

	switch (PQftype(res, col)) {
		case NUMERICOID: {
			if (dopt->cast == 's') {
				lua_pushlstring(L, val, len);
				break;
			}
			else if (dopt->cast == 'd' && dopt->dnew_index != -1) {
				lua_rawgeti(L, LUA_REGISTRYINDEX, dopt->dnew_index);
				lua_pushlstring(L, val, len);
				int fail = lua_pcall(L, 1, 1, 0);
				if (fail) {
					lua_pop(L, 2);
					return false;
				}
				break;
			}
			/* 'n': fallthrough */
			FALLTHROUGH;
		}
		case INT2OID:
		case INT4OID: {
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
	}
	lua_settable(L, -3);
	return true;
}

/**
 * Push query result tuples to given table on lua stack
 */
static int
safe_pg_parsetuples(struct lua_State *L)
{
	PGresult *res = (PGresult *)lua_topointer(L, 1);
	dec_opt_t *dopt = (dec_opt_t *)lua_topointer(L, 2);
	int row, rows = PQntuples(res);
	int col, cols = PQnfields(res);
	lua_newtable(L);
	for (row = 0; row < rows; ++row) {
		lua_pushnumber(L, row + 1);
		lua_newtable(L);
		for (col = 0; col < cols; ++col)
			parse_pg_value(L, res, row, col, dopt);
		lua_settable(L, -3);
	}
	return 1;
}

#if 0
Now we return only recordset without status
/**
 * Push query execution status to lua stack
 */
static int
safe_pg_parsestatus(struct lua_State *L)
{
	PGresult *r = (PGresult *)lua_topointer(L, 1);
	lua_newtable(L);
	lua_pushstring(L, "tuples");
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
#endif

/**
 * Wait until postgres returns something
 */
static int
pg_wait_for_result(PGconn *conn)
{
	int sock = PQsocket(conn);
	while (true) {
		if (fiber_is_cancelled())
			return -2;
		if (PQconsumeInput(conn) != 1)
			return PQstatus(conn) == CONNECTION_BAD ? -1: 0;
		if (PQisBusy(conn))
			coio_wait(sock, COIO_READ, TIMEOUT_INFINITY);
		else
			break;
	}
	return 1;
}

/**
 * Appends result from postgres to lua table
 */
static int
pg_resultget(struct lua_State *L, PGconn *conn, int *res_no, int status_ok, dec_opt_t *dopt)
{
	int wait_res = pg_wait_for_result(conn);
	if (wait_res != 1)
	{
		lua_pushinteger(L, wait_res);
		if (wait_res == -2)
			safe_pushstring(L, "Fiber was cancelled");
		else
			lua_pushstring(L, PQerrorMessage(conn));
		return 0;
	}

	PGresult *pg_res = PQgetResult(conn);
	if (!pg_res) {
		return 0;
	}
	if (status_ok != 1) {
		// Fail mode, just skip all other results
		PQclear(pg_res);
		return status_ok;
	}
	int res = -1;
	int fail = 0;
	int status = PQresultStatus(pg_res);
	switch (status) {
		case PGRES_TUPLES_OK:
			lua_pushinteger(L, (*res_no)++);
			lua_pushcfunction(L, safe_pg_parsetuples);
			lua_pushlightuserdata(L, pg_res);
			lua_pushlightuserdata(L, dopt);
			fail = lua_pcall(L, 2, 1, 0);
			if (!fail) {
				lua_settable(L, -3);
				break;
			}
			break;
		case PGRES_COMMAND_OK:
			res = 1;
			break;
		case PGRES_FATAL_ERROR:
		case PGRES_EMPTY_QUERY:
		case PGRES_NONFATAL_ERROR:
			lua_pushinteger(L,
				(PQstatus(conn) == CONNECTION_BAD) ? -1: 1);
			fail = safe_pushstring(L, PQerrorMessage(conn));
			break;
		default:
			lua_pushinteger(L, -1);
			fail = safe_pushstring(L,
				"Unwanted execution result status");
	}

	PQclear(pg_res);
	if (fail) {
		lua_push_error(L);
		res = -1;
	}
	return res;
}

/**
 * Parse lua value
 */
static void
lua_parse_param(struct lua_State *L,
	int idx, const char **value, int *length, Oid *type)
{
    /* Serialized [u]int64_t */
	static char buf[512];
	static char *pos = NULL;
    /* lua_parse_param(L, idx + 5, ...) */
	if (idx == 5) {
		*buf = '\0';
		pos = buf;
	}

	if (lua_isnil(L, idx)) {
		*value = NULL;
		*length = 0;
		*type = 0;
		return;
	}

	if (lua_isboolean(L, idx)) {
		static const char pg_true[] = "t";
		static const char pg_false[] = "f";
		*value = lua_toboolean(L, idx) ? pg_true : pg_false;
		*length = 1;
		*type = BOOLOID;
		return;
	}

	if (lua_type(L, idx) == LUA_TNUMBER) {
		size_t len;
		*value = lua_tolstring(L, idx, &len);
		*length = len;
		*type = NUMERICOID;
		return;
	}

	if (luaL_iscdata(L, idx)) {
		uint32_t ctypeid = 0;
		void *cdata = luaL_checkcdata(L, idx, &ctypeid);
		int len = 0;
		if (ctypeid == luaL_ctypeid(L, "int64_t")) {
			len = snprintf(pos, sizeof(buf) - (pos - buf), "%ld", *(int64_t*)cdata);
			*type = INT8OID;
		}
		else if (ctypeid == luaL_ctypeid(L, "uint64_t")) {
			len = snprintf(pos, sizeof(buf) - (pos - buf), "%lu", *(uint64_t*)cdata);
			*type = NUMERICOID;
		}

		if (len > 0) {
			*value = pos;
			*length = len;
			pos += len + 1;
			return;
		}
	}

	// We will pass all other types as strings
	size_t len;
	*value = lua_tolstring(L, idx, &len);
	*length = len;
	*type = TEXTOID;
}

/**
 * Start query execution
 */
static int
lua_pg_execute(struct lua_State *L)
{
	PGconn *conn = lua_check_pgconn(L, 1);

	dec_opt_t dopt = {'n', -1};
	if (lua_isstring(L, 2)) {
		const char *dec_cast_type = lua_tostring(L, 2);
		if (*dec_cast_type == 'n' ||
			*dec_cast_type == 's' ||
			*dec_cast_type == 'd')
			dopt.cast = *dec_cast_type;
	}

	if (!lua_isstring(L, 4)) {
		safe_pushstring(L, "Second param should be a sql command");
		return lua_push_error(L);
	}

	if (lua_isfunction(L, 3)) {
		lua_pushvalue(L, 3);
		dopt.dnew_index = luaL_ref(L, LUA_REGISTRYINDEX);
	}

	const char *sql = lua_tostring(L, 4);
	int paramCount = lua_gettop(L) - 4;

	const char **paramValues = NULL;
	int  *paramLengths = NULL;
	Oid *paramTypes = NULL;

	int res = 0;
	if (paramCount > 0) {
		/* Allocate chunk of memory for params */
		char *buf = (char *)lua_newuserdata(L, paramCount *
			(sizeof(*paramValues) + sizeof(*paramLengths) +
			 sizeof(*paramTypes)));

		paramValues = (const char **) buf;
		buf += paramCount * sizeof(*paramValues);
		paramLengths = (int *) buf;
		buf += paramCount * sizeof(*paramLengths);
		paramTypes = (Oid *) buf;

		int idx;
		for (idx = 0; idx < paramCount; ++idx) {
			lua_parse_param(L, idx + 5, paramValues + idx,
				paramLengths + idx, paramTypes + idx);
		}
		res = PQsendQueryParams(conn, sql, paramCount, paramTypes,
			paramValues, paramLengths, NULL, 0);
	}
	else
		res = PQsendQuery(conn, sql);

	if (res == -1) {
		lua_pushinteger(L, PQstatus(conn) == CONNECTION_BAD ? -1: 0);
		lua_pushstring(L, PQerrorMessage(conn));
		if (dopt.dnew_index != -1)
			luaL_unref(L, LUA_REGISTRYINDEX, dopt.dnew_index);
		return 2;
	}
	lua_pushinteger(L, 0);
	lua_newtable(L);

	int res_no = 1;
	int status_ok = 1;
	while ((status_ok = pg_resultget(L, conn, &res_no, status_ok, &dopt)));

	if (dopt.dnew_index != -1)
		luaL_unref(L, LUA_REGISTRYINDEX, dopt.dnew_index);

	return 2;
}

/**
 * Test that connection has active transaction
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
 * Close connection
 */
static int
lua_pg_close(struct lua_State *L)
{
	PGconn **conn_p = (PGconn **)luaL_checkudata(L, 1, pg_driver_label);
	if (conn_p == NULL || *conn_p == NULL) {
		lua_pushboolean(L, 0);
		return 1;
	}
	PQfinish(*conn_p);
	*conn_p = NULL;
	lua_pushboolean(L, 1);
	return 1;
}

/**
 * Collect connection
 */
static int
lua_pg_gc(struct lua_State *L)
{
	PGconn **conn_p = (PGconn **)luaL_checkudata(L, 1, pg_driver_label);
	if (conn_p && *conn_p)
		PQfinish(*conn_p);
	if (conn_p)
		*conn_p = NULL;
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
 * Prints warnings from Postgresql into tarantool log
 */
static void
pg_notice(void *arg, const char *message)
{
	say_info("Postgresql: %s", message);
	(void)arg;
}

#if PG_VERSION_NUM >= 90000
/**
 * Quote variable
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
	const char *s = lua_tolstring(L, 2, &len);

	s = PQescapeLiteral(conn, s, len);

	if (!s)
		luaL_error(L, "Can't allocate memory");
	int fail = safe_pushstring(L, (char *)s);
	free((void *)s);
	return fail ? lua_push_error(L): 1;
}

/**
 * Quote identifier
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
	const char *s = lua_tolstring(L, 2, &len);

	s = PQescapeIdentifier(conn, s, len);

	if (!s)
		luaL_error(L, "Can't allocate memory");
	int fail = safe_pushstring(L, (char *)s);
	free((void *)s);
	return fail ? lua_push_error(L): 1;
}

#endif

/**
 * Start connection to postgresql
 */
static int
lua_pg_connect(struct lua_State *L)
{
	if (lua_gettop(L) != 1 || !lua_isstring(L, 1))
		luaL_error(L, "Usage: pg.connect(connstring)");

	const char *constr = lua_tostring(L, 1);
	PGconn *conn = NULL;

	conn = PQconnectStart(constr);
	if (!conn) {
		lua_pushinteger(L, -1);
		int fail = safe_pushstring(L,
			"Can't allocate PG connection structure");
		return fail ? lua_push_error(L): 2;
	}

	if (PQstatus(conn) == CONNECTION_BAD) {
		lua_pushinteger(L, -1);
		int fail = safe_pushstring(L, PQerrorMessage(conn));
		PQfinish(conn);
		return fail ? lua_push_error(L): 2;
	}

	PostgresPollingStatusType status = PGRES_POLLING_WRITING;
	while (true) {
		if (fiber_is_cancelled()) {
			lua_pushinteger(L, -2);
			safe_pushstring(L, "Fiber was cancelled");
			return 1;
		}

		int sock = PQsocket(conn);
		if (status == PGRES_POLLING_READING)
			coio_wait(sock, COIO_READ, TIMEOUT_INFINITY);
		if (status == PGRES_POLLING_WRITING)
			coio_wait(sock, COIO_WRITE, TIMEOUT_INFINITY);

		status = PQconnectPoll(conn);
		if (status == PGRES_POLLING_OK) {
			PQsetNoticeProcessor(conn, pg_notice, NULL);
			lua_pushinteger(L, 1);
			PGconn **conn_p = (PGconn **)
				lua_newuserdata(L, sizeof(conn));
			*conn_p = conn;
			luaL_getmetatable(L, pg_driver_label);
			lua_setmetatable(L, -2);
			return 2;
		}
		if (status == PGRES_POLLING_READING || status == PGRES_POLLING_WRITING)
			continue;
		break;
	}
	lua_pushinteger(L, -1);
	int fail = safe_pushstring(L, PQerrorMessage(conn));
	PQfinish(conn);
	return fail ? lua_push_error(L): 2;
}

LUA_API int
luaopen_pg_driver(lua_State *L)
{
	static const struct luaL_Reg methods [] = {
		{"execute",	lua_pg_execute},
#if PG_VERSION_NUM >= 90000
		{"quote",	lua_pg_quote},
		{"quote_ident",	lua_pg_quote_ident},
#endif
		{"close",	lua_pg_close},
		{"active",	lua_pg_transaction_active},
		{"__tostring",	lua_pg_tostring},
		{"__gc",	lua_pg_gc},
		{NULL, NULL}
	};

	luaL_newmetatable(L, pg_driver_label);
	lua_pushvalue(L, -1);
	luaL_register(L, NULL, methods);
	lua_setfield(L, -2, "__index");
	lua_pushstring(L, pg_driver_label);
	lua_setfield(L, -2, "__metatable");
	lua_pop(L, 1);

	lua_newtable(L);
	static const struct luaL_Reg meta [] = {
		{"connect", lua_pg_connect},
		{NULL, NULL}
	};
	luaL_register(L, NULL, meta);
	return 1;
}
