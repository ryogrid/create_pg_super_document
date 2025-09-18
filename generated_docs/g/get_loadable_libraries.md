# get_loadable_libraries

## Location
src/bin/pg_upgrade/function.c: 55 - 145

## Overview
Fetches the names of all libraries containing C-language functions and logical replication output plugins from the old PostgreSQL cluster during pg_upgrade operations.

## Definition
```c
void get_loadable_libraries(void)
```

## Detailed Description
This function systematically collects library names from all databases in the old PostgreSQL cluster to ensure they can be verified in the new installation during upgrade. It performs two main collection tasks: first, it queries each database for libraries containing non-built-in C functions by examining pg_proc entries where prolang matches ClanguageId and probin is not NULL. Second, it includes logical replication output plugin names from active replication slots. The collected library information is stored in the global os_info.libraries array along with associated database numbers for later verification by check_loadable_libraries().

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - pg_malloc (memory allocation)
  - connectToServer (database connection)
  - executeQueryOrDie (SQL query execution)
  - count_old_cluster_logical_slots (logical slot counting)
  - PQntuples, PQgetvalue, PQclear, PQfinish (PostgreSQL result handling)
  - pg_strdup (string duplication)
  - pg_free (memory deallocation)
  - DbInfo, LibraryInfo, LogicalSlotInfoArr (structure types)
  - FirstNormalObjectId, ClanguageId (constants)
- Called from (representative examples):
  - check_and_dump_old_cluster

## Notes and Other Information
- Modifies global os_info.libraries and os_info.num_libraries
- Removes duplicate library names within each database automatically via DISTINCT query
- Does not eliminate duplicates across different databases or between C functions and logical replication plugins
- Memory allocated for libraries must be freed elsewhere in the program
- Uses FirstNormalObjectId to exclude built-in system functions
- Handles invalid replication slots by skipping them during plugin collection