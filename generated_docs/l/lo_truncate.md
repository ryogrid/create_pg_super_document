# lo_truncate

## Location
src/interfaces/libpq/fe-lobj.c: 131 - 194

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
  - lo_initialize
  - libpq_append_conn_error
  - PQfn
  - PQresultStatus
  - PQclear
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