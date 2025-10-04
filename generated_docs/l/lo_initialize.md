# lo_initialize

## Location
[src/interfaces/libpq/fe-lobj.c:843-1022](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-lobj.c#L843-L1022)

## Overview
Static initialization function that discovers and caches function OIDs for all PostgreSQL large object operations, ensuring efficient subsequent large object function calls.

## Definition
```c
static int lo_initialize(PGconn *conn)
```

## Detailed Description
This static function initializes the large object function infrastructure for a PostgreSQL connection by querying the system catalog to discover the Object IDs (OIDs) of all large object-related functions. It performs this discovery only once per connection and caches the results in the connection structure for efficient reuse.

The function queries pg_catalog.pg_proc to find function OIDs for all required large object operations including lo_open, lo_close, lo_creat, lo_create, lo_unlink, lo_lseek, lo_lseek64, lo_tell, lo_tell64, lo_truncate, lo_truncate64, loread, and lowrite. It validates that all essential functions are available and reports specific errors for missing functions.

This initialization approach allows PostgreSQL to support large objects across different server versions by dynamically discovering available functionality rather than assuming fixed function OIDs.

## Parameters / Member Variables
- `conn`: PostgreSQL database connection handle to initialize for large object operations

## Dependencies
- Functions called/Symbols referenced:
  - pqClearConnErrorState
  - malloc
  - MemSet
  - [PQexec](../P/PQexec.md)
  - [PQgetvalue](../P/PQgetvalue.md)
  - [PQntuples](../P/PQntuples.md)
  - [PQclear](../P/PQclear.md)
  - [libpq_append_conn_error](libpq_append_conn_error.md)
  - strcmp
  - atoi
- Called from (representative examples):
  - [lo_open](lo_open.md)
  - [lo_close](lo_close.md)
  - [lo_truncate](lo_truncate.md)
  - [lo_read](lo_read.md)
  - [lo_write](lo_write.md)
  - [lo_lseek](lo_lseek.md)
  - [lo_creat](lo_creat.md)
  - [lo_tell](lo_tell.md)
  - [lo_unlink](lo_unlink.md)

## Notes and Other Information
- Returns 0 on success, -1 on failure
- Static function, only accessible within fe-lobj.c
- Performs one-time initialization per connection, subsequent calls return immediately if already initialized
- Allocates and populates PGlobjfuncs structure to cache function OIDs
- Validates presence of all essential large object functions (stone age functions)
- Gracefully handles newer functions that may not exist in older PostgreSQL versions
- Essential for the client-side large object API to function correctly
- Memory allocation failure and missing critical functions result in initialization failure
- Clears connection error state at the beginning of operation

## Simplified Source

```c
static int lo_initialize(PGconn *conn)
{
    PGresult *res;
    PGlobjfuncs *lobjfuncs;
    int n;

    // Basic validation
    if (conn == NULL)
        return -1;

    // Clear error state and check if already initialized
    pqClearConnErrorState(conn);
    if (conn->lobjfuncs != NULL)
        return 0;

    // Allocate structure to cache function OIDs
    lobjfuncs = (PGlobjfuncs *) malloc(sizeof(PGlobjfuncs));
    if (lobjfuncs == NULL) {
        libpq_append_conn_error(conn, "out of memory");
        return -1;
    }
    MemSet((char *) lobjfuncs, 0, sizeof(PGlobjfuncs));

    // Query pg_catalog to get all large object function OIDs at once
    const char *query =
        "select proname, oid from pg_catalog.pg_proc "
        "where proname in ("
        "'lo_open', 'lo_close', 'lo_creat', 'lo_create', 'lo_unlink', "
        "'lo_lseek', 'lo_lseek64', 'lo_tell', 'lo_tell64', "
        "'lo_truncate', 'lo_truncate64', 'loread', 'lowrite') "
        "and pronamespace = (select oid from pg_catalog.pg_namespace "
        "where nspname = 'pg_catalog')";

    res = PQexec(conn, query);
    if (res == NULL || res->resultStatus != PGRES_TUPLES_OK) {
        free(lobjfuncs);
        if (res) PQclear(res);
        if (!res) return -1;
        libpq_append_conn_error(conn, "query to initialize large object functions did not return data");
        return -1;
    }

    // Parse results and populate function OID cache
    for (n = 0; n < PQntuples(res); n++) {
        const char *fname = PQgetvalue(res, n, 0);
        Oid foid = (Oid) atoi(PQgetvalue(res, n, 1));

        // Map function names to structure fields
        if (strcmp(fname, "lo_open") == 0)
            lobjfuncs->fn_lo_open = foid;
        else if (strcmp(fname, "lo_close") == 0)
            lobjfuncs->fn_lo_close = foid;
        else if (strcmp(fname, "lo_creat") == 0)
            lobjfuncs->fn_lo_creat = foid;
        else if (strcmp(fname, "lo_create") == 0)
            lobjfuncs->fn_lo_create = foid;
        else if (strcmp(fname, "lo_unlink") == 0)
            lobjfuncs->fn_lo_unlink = foid;
        else if (strcmp(fname, "lo_lseek") == 0)
            lobjfuncs->fn_lo_lseek = foid;
        else if (strcmp(fname, "lo_lseek64") == 0)
            lobjfuncs->fn_lo_lseek64 = foid;
        else if (strcmp(fname, "lo_tell") == 0)
            lobjfuncs->fn_lo_tell = foid;
        else if (strcmp(fname, "lo_tell64") == 0)
            lobjfuncs->fn_lo_tell64 = foid;
        else if (strcmp(fname, "lo_truncate") == 0)
            lobjfuncs->fn_lo_truncate = foid;
        else if (strcmp(fname, "lo_truncate64") == 0)
            lobjfuncs->fn_lo_truncate64 = foid;
        else if (strcmp(fname, "loread") == 0)
            lobjfuncs->fn_lo_read = foid;
        else if (strcmp(fname, "lowrite") == 0)
            lobjfuncs->fn_lo_write = foid;
    }
    PQclear(res);

    // Validate that all essential functions are available
    const char *required_funcs[] = {
        "lo_open", "lo_close", "lo_creat", "lo_unlink",
        "lo_lseek", "lo_tell", "loread", "lowrite"
    };
    Oid required_oids[] = {
        lobjfuncs->fn_lo_open, lobjfuncs->fn_lo_close,
        lobjfuncs->fn_lo_creat, lobjfuncs->fn_lo_unlink,
        lobjfuncs->fn_lo_lseek, lobjfuncs->fn_lo_tell,
        lobjfuncs->fn_lo_read, lobjfuncs->fn_lo_write
    };

    for (int i = 0; i < 8; i++) {
        if (required_oids[i] == 0) {
            libpq_append_conn_error(conn, "cannot determine OID of function %s",
                                   required_funcs[i]);
            free(lobjfuncs);
            return -1;
        }
    }

    // Success - cache the function OIDs in connection
    conn->lobjfuncs = lobjfuncs;
    return 0;
}
```