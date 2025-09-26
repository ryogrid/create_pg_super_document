# PQendcopy

## Location
[src/interfaces/libpq/fe-exec.c:2949-2979](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L2949-L2979)

## Overview
PQendcopy is a deprecated PostgreSQL libpq function that finalizes the COPY command protocol after completing the data transfer portion of a copy in/out operation.

## Definition
```c
int PQendcopy(PGconn *conn)
```

## Detailed Description
PQendcopy is used to complete the command protocol after finishing the data transfer portion of a COPY IN or COPY OUT operation. This function is marked as deprecated because it's cleaner and more robust to use PQgetResult to obtain the transfer status instead. The function provides a simple wrapper around the internal pqEndcopy3 function, which handles the protocol-level details of ending the copy operation.

The function returns 0 on success and 1 on failure. It includes a basic null pointer check for the connection parameter before proceeding with the operation.

## Parameters / Member Variables
- `conn`: Connection object representing the database connection (checked for null)

## Dependencies
- Functions called/Symbols referenced:
  - pqEndcopy3
- Called from (representative examples):
  - libpqrcv_endstreaming (in WAL receiver)
  - initPopulateTable (in pgbench)
  - ecpg_check_PQresult (in ECPG)

## Notes and Other Information
- This function is deprecated in favor of using PQgetResult for better status handling
- Provides a simple success/failure return value (0 for success, 1 for failure)
- Includes basic null pointer validation for the connection parameter
- Part of the legacy COPY protocol API in libpq
- Located in src/interfaces/libpq/fe-exec.c:2949-2979
- Used in various PostgreSQL components including replication, pgbench, and ECPG