# RetrieveWalSegSize

## Location
[src/bin/pg_basebackup/streamutil.c:347-425](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/streamutil.c#L347-L425)

## Overview
Retrieves and validates the WAL segment size from the PostgreSQL server, handling version-specific differences.

## Definition

```c
bool
RetrieveWalSegSize(PGconn *conn)
```
## Detailed Description
RetrieveWalSegSize determines the WAL segment size used by the connected PostgreSQL server. For PostgreSQL versions 10 and later, it executes "SHOW wal_segment_size" to retrieve the current setting, parsing the result to handle different units (MB/GB) and converting to bytes. For older versions, it uses the default WAL segment size. The function validates the retrieved size to ensure it's a valid power of two between 1 MB and 1 GB, and sets the global WalSegSz variable accordingly.

## Parameters / Member Variables
- `*conn`: PGconn pointer to an active PostgreSQL connection
## Dependencies
- Functions called/Symbols referenced:
  - [PQserverVersion](../P/PQserverVersion.md)
  - [PQexec](../P/PQexec.md)
  - [PQresultStatus](../P/PQresultStatus.md)
  - [PQntuples](../P/PQntuples.md)
  - [PQnfields](../P/PQnfields.md)
  - [PQgetvalue](../P/PQgetvalue.md)
  - [PQclear](../P/PQclear.md)
  - IsValidWalSegSize
  - sscanf
  - strcmp
  - ngettext
  - pg_log_error
  - pg_log_error_detail
  - MINIMUM_VERSION_FOR_SHOW_CMD
  - DEFAULT_XLOG_SEG_SIZE
  - PGRES_TUPLES_OK
- Called from (representative examples):
  - [main](../m/main.md) (in pg_basebackup.c, pg_receivewal.c)

## Notes and Other Information
- Returns true on success, false on failure
- Sets the global variable WalSegSz with the determined segment size in bytes  
- Handles unit conversion from MB/GB to bytes automatically
- For PostgreSQL versions before 10, defaults to DEFAULT_XLOG_SEG_SIZE (16MB)
- Validates that the WAL segment size is a power of two between 1 MB and 1 GB
- Critical for proper WAL streaming and backup operations in pg_basebackup utilities
- The function assumes a valid database connection and will assert if conn is NULL