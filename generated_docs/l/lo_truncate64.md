# lo_truncate64

## Location
src/interfaces/libpq/fe-lobj.c: 195 - 244

## Overview
Truncates an existing large object to the specified 64-bit size, supporting large objects beyond the 32-bit integer limit.

## Definition
```c
int lo_truncate64(PGconn *conn, int fd, pg_int64 len)
```

## Detailed Description
The `lo_truncate64` function provides the same functionality as `lo_truncate` but accepts a 64-bit integer length parameter, allowing it to handle large objects that exceed the 2GB limit imposed by 32-bit integers. This function performs network byte order conversion on the length parameter before sending it to the backend, ensuring proper communication across different architectures. Like `lo_truncate`, it can both shrink and extend large objects, filling extended areas with null bytes.

## Parameters / Member Variables
- `conn`: PostgreSQL connection handle for the database session
- `fd`: File descriptor of the open large object to truncate
- `len`: Target size for the large object in bytes (64-bit signed integer)

## Dependencies
- Functions called/Symbols referenced:
  - lo_initialize
  - libpq_append_conn_error
  - lo_hton64
  - PQfn
  - PQresultStatus
  - PQclear
- Called from (representative examples):
  - my_truncate (in testlo64.c)

## Notes and Other Information
- Returns 0 on success, -1 on failure
- Supports 64-bit length values, removing the INT_MAX limitation of lo_truncate
- Requires PostgreSQL version that supports the lo_truncate64 backend function
- Performs network byte order conversion (lo_hton64) for cross-platform compatibility
- If truncating to a smaller size, data beyond the new length is permanently lost
- If extending to a larger size, new space is filled with null bytes
- Essential for applications working with large objects exceeding 2GB in size
- Function checks availability at runtime and returns error if backend function is not available
- Used primarily in specialized applications dealing with very large binary data