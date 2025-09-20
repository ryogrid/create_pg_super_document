# get_loadable_libraries

## Location
[src/bin/pg_upgrade/function.c:55-145](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/function.c#L55-L145)

## Overview
Fetches the names of all libraries containing C-language functions and logical replication output plugins from the old PostgreSQL cluster during pg_upgrade operations.

## Definition
```c
void get_loadable_libraries(void)
```

## Detailed Description
This function systematically collects library names from all databases in the old PostgreSQL cluster to ensure they can be verified in the new installation during upgrade. It performs two main collection tasks: first, it queries each database for libraries containing non-built-in C functions by examining pg_proc entries where prolang matches ClanguageId and probin is not NULL. Second, it includes logical replication output plugin names from active replication slots. The collected library information is stored in the global os_info.libraries array along with associated database numbers for later verification by check_loadable_libraries().

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - pg_malloc (memory allocation)
  - [connectToServer](../c/connectToServer.md) (database connection)
  - [executeQueryOrDie](../e/executeQueryOrDie.md) (SQL query execution)
  - [count_old_cluster_logical_slots](../c/count_old_cluster_logical_slots.md) (logical slot counting)
  - [PQntuples](../P/PQntuples.md), PQgetvalue, PQclear, PQfinish (PostgreSQL result handling)
  - [pg_strdup](../p/pg_strdup.md) (string duplication)
  - [pg_free](../p/pg_free.md) (memory deallocation)
  - [DbInfo](../D/DbInfo.md), LibraryInfo, LogicalSlotInfoArr (structure types)
  - FirstNormalObjectId, ClanguageId (constants)
- Called from (representative examples):
  - [check_and_dump_old_cluster](../c/check_and_dump_old_cluster.md)

## Notes and Other Information
- Modifies global os_info.libraries and os_info.num_libraries
- Removes duplicate library names within each database automatically via DISTINCT query
- Does not eliminate duplicates across different databases or between C functions and logical replication plugins
- Memory allocated for libraries must be freed elsewhere in the program
- Uses FirstNormalObjectId to exclude built-in system functions
- Handles invalid replication slots by skipping them during plugin collection