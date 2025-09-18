# print_db_infos

## Location
[src/bin/pg_upgrade/info.c:797-812](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/info.c#L797-L812)

## Overview
Prints debugging information for all databases in a DbInfoArr structure, including their relations and replication slots.

## Definition
```c
static void print_db_infos(DbInfoArr *db_arr)
```

## Detailed Description
This function provides verbose logging output for database information during the pg_upgrade process. It iterates through all databases in the provided DbInfoArr and logs each database name at PG_VERBOSE level. For each database, it calls helper functions to print detailed information about the database's relations and replication slots. This is primarily used for debugging and monitoring the upgrade process, allowing administrators to see what databases and their associated objects are being processed.

## Parameters / Member Variables
- `db_arr`: Pointer to DbInfoArr structure containing database information to be printed

## Dependencies
- Functions called/Symbols referenced:
  - [pg_log](pg_log.md)
  - [print_rel_infos](print_rel_infos.md)
  - [print_slot_infos](print_slot_infos.md)
  - [DbInfoArr](../D/DbInfoArr.md) (struct type)
  - [DbInfo](../D/DbInfo.md) (struct type)
  - PG_VERBOSE (log level constant)
- Called from (representative examples):
  - [get_db_rel_and_slot_infos](../g/get_db_rel_and_slot_infos.md)

## Notes and Other Information
- This is a static function only used within src/bin/pg_upgrade/info.c
- Uses PG_VERBOSE logging level, so output is only visible when verbose logging is enabled
- Part of the pg_upgrade utility's debugging and monitoring system
- Provides hierarchical output: database name followed by its relations and slots
- Database names are quoted in the output for clarity