# lo_truncate

## Location
[src/interfaces/libpq/fe-lobj.c:131-194](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-lobj.c#L131-L194)

## Overview
Truncates an existing large object to the specified size, removing or adding null bytes as necessary to achieve the target length.

## Definition
```c
int lo_truncate(PGconn *conn, int fd, size_t len)
```

## Detailed Description
The `lo_truncate` function modifies the size of an open large object. If the specified length is smaller than the current size, the large object is truncated and data beyond the new length is lost. If the specified length is larger than the current size, the large object is extended with null bytes. This function includes important compatibility checks for PostgreSQL versions prior to 8.3 where the truncate functionality was not available. The function also validates that the length parameter fits within a 32-bit signed integer range due to backend limitations.

## Parameters / Member Variables
- `conn`: PostgreSQL connection handle for the database session
- `fd`: File descriptor of the open large object to truncate
- `len`: Target size for the large object in bytes (must be <= INT_MAX)

## Dependencies
- Functions called/Symbols referenced:
  - [lo_initialize](lo_initialize.md)
  - [libpq_append_conn_error](libpq_append_conn_error.md)
  - [PQfn](../P/PQfn.md)
  - [PQresultStatus](../P/PQresultStatus.md)
  - [PQclear](../P/PQclear.md)
- Called from (representative examples):
  - (Limited direct usage - primarily available through libpq interface)

## Notes and Other Information
- Returns 0 on success, -1 on failure
- Requires PostgreSQL 8.3 or later (function checks availability at runtime)
- Length parameter is limited to INT_MAX due to backend function constraints
- If truncating to a smaller size, data beyond the new length is permanently lost
- If extending to a larger size, new space is filled with null bytes
- Function performs runtime compatibility checks for older PostgreSQL versions
- For larger sizes beyond INT_MAX, consider using lo_truncate64 instead
- The underlying backend function only accepts signed 32-bit integers despite the size_t parameter

## Simplified Source

```c
int lo_truncate(PGconn *conn, int fd, size_t len) {
    PQArgBlock argv[2];
    PGresult *res;
    int retval;
    int result_len;

    // Initialize large object function lookup table
    if (lo_initialize(conn) < 0)
        return -1;

    // Check if truncate function is available (PostgreSQL 8.3+)
    if (conn->lobjfuncs->fn_lo_truncate == 0) {
        libpq_append_conn_error(conn, "cannot determine OID of function %s",
                               "lo_truncate");
        return -1;
    }

    // Validate length fits in 32-bit signed integer
    if (len > (size_t) INT_MAX) {
        libpq_append_conn_error(conn, "argument of lo_truncate exceeds integer range");
        return -1;
    }

    // Prepare arguments: file descriptor and new length
    argv[0].isint = 1;
    argv[0].len = 4;
    argv[0].u.integer = fd;

    argv[1].isint = 1;
    argv[1].len = 4;
    argv[1].u.integer = (int) len;

    // Call backend lo_truncate function
    res = PQfn(conn, conn->lobjfuncs->fn_lo_truncate,
               &retval, &result_len, 1, argv, 2);

    // Check result and return status
    if (PQresultStatus(res) == PGRES_COMMAND_OK) {
        PQclear(res);
        return retval; // Success: return backend result
    } else {
        PQclear(res);
        return -1; // Failure
    }
}
```