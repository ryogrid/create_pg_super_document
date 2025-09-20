# vacuum_db

## Location
[src/bin/initdb/initdb.c:1983-1992](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/initdb/initdb.c#L1983-L1992)

## Overview
Performs final cleanup operations on the template1 database by running ANALYZE and VACUUM FREEZE during database initialization.

## Definition

```c
static void
vacuum_db(FILE *cmdfd)
```
## Detailed Description
The vacuum_db function performs essential database maintenance operations as the final step in PostgreSQL database initialization. It executes two critical commands in sequence:

1. **ANALYZE**: Collects statistics about the distribution of values in table columns. This statistical information is stored in the system catalogs (pg_statistic) and is used by the PostgreSQL query planner to make informed decisions about query execution plans. Running ANALYZE after all initial data has been loaded ensures that the optimizer has accurate information about the newly created system catalogs.

2. **VACUUM FREEZE**: Performs a special type of vacuum operation that "freezes" all existing tuples by advancing their transaction IDs to the frozen transaction ID (FrozenTransactionId). This operation:
   - Prevents transaction ID wraparound issues
   - Marks all existing rows as committed and visible to all future transactions
   - Ensures the database is in a clean, stable state for use as a template
   - Reclaims any dead space that may have been created during initialization

The strategic ordering (ANALYZE before VACUUM FREEZE) ensures that the collected statistics are preserved through the freezing process, giving the template1 database accurate optimizer statistics that will be inherited by databases created from this template.

## Parameters / Member Variables
- : FILE pointer to the command file where SQL statements are written for execution during database initialization

## Dependencies
- Functions called/Symbols referenced:
  - PG_CMD_PUTS (macro for writing SQL strings to the command file)

- Called from:
  - [initialize_data_directory](../i/initialize_data_directory.md) (main database initialization function)

## Notes and Other Information
- This is typically the final maintenance step in the initdb process
- The VACUUM FREEZE operation is particularly important for template databases to ensure they start with all rows in a "clean" transaction state
- The statistics collected by ANALYZE help ensure good query performance from the start
- Template1 serves as the default template for new databases, so its state affects all subsequently created databases
- Running these operations during initdb is more efficient than deferring them to runtime
- The frozen state helps with long-term database stability and prevents potential transaction ID wraparound issues