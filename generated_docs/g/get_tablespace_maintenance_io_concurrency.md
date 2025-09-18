# get_tablespace_maintenance_io_concurrency

## Location
[src/backend/utils/cache/spccache.c:229-237](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/spccache.c#L229-L237)

## Overview
Returns the maintenance I/O concurrency setting for a specified tablespace, falling back to the global setting if not configured at the tablespace level.

## Definition
```c
int get_tablespace_maintenance_io_concurrency(Oid spcid)
```

## Detailed Description
This function retrieves the maintenance I/O concurrency setting for a given tablespace OID. It first fetches the tablespace cache entry using `get_tablespace()`, then checks if the tablespace has specific maintenance I/O concurrency options configured. If the tablespace doesn't have custom options set (opts is NULL) or if the maintenance_io_concurrency value is negative (indicating default), it returns the global `maintenance_io_concurrency` setting. Otherwise, it returns the tablespace-specific setting.

The maintenance I/O concurrency setting controls how many concurrent I/O operations can be performed during maintenance operations like VACUUM, which helps optimize performance for maintenance tasks.

## Parameters / Member Variables
- `spcid`: The OID (Object Identifier) of the tablespace for which to retrieve the maintenance I/O concurrency setting

## Dependencies
- Functions called/Symbols referenced:
  - [get_tablespace](get_tablespace.md)
  - TableSpaceCacheEntry
  - maintenance_io_concurrency (global variable)
- Called from (representative examples):
  - [heap_index_delete_tuples](../h/heap_index_delete_tuples.md)
  - read_stream_begin_relation
  - SPCCACHE_H (header declaration)

## Notes and Other Information
- This function is part of the tablespace cache subsystem (spccache.c)
- Returns an integer value representing the number of concurrent I/O operations allowed
- Uses a negative value check to determine if the default global setting should be used
- The function provides a clean interface for accessing tablespace-specific or global maintenance I/O concurrency settings
- Located in src/backend/utils/cache/spccache.c at lines 229-237