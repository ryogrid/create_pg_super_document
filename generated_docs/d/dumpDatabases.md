# dumpDatabases

## Location
[src/bin/pg_dump/pg_dumpall.c:1581-1676](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dumpall.c#L1581-L1676)

## Overview
Orchestrates the dumping of all databases by iterating through allowed databases and invoking pg_dump for each one with appropriate options.

## Definition


## Detailed Description
This function is responsible for dumping the contents of all databases in a PostgreSQL cluster as part of the pg_dumpall utility. It queries the system catalog to find all databases that allow connections (datallowconn is true) and are not marked with connection limit -2. The function processes databases in a specific order: template1 first, then all other databases alphabetically. This ordering prevents issues that could arise when using the --clean option, such as trying to drop the currently connected database.

For each database, the function determines the appropriate pg_dump options based on whether it's a system database (template1/postgres) and whether the --clean option was specified. System databases are handled specially since they're assumed to already exist in the target installation. The function skips template0 (even if marked as allowing connections) and any explicitly excluded databases.

## Parameters / Member Variables
- : Active PostgreSQL database connection used to query the system catalog for database information

## Dependencies
- Functions called/Symbols referenced:
  - [executeQuery](../e/executeQuery.md) (executes SQL queries)
  - [simple_string_list_member](../s/simple_string_list_member.md) (checks database exclusion list)
  - pg_log_info (logging utility)
  - [sanitize_line](../s/sanitize_line.md) (sanitizes database names for output)
  - [runPgDump](../r/runPgDump.md) (executes pg_dump for individual databases)
  - PG_BINARY_A (file mode constant)
  - fopen (file operations)
- Called from (representative examples):
  - [main](../m/main.md) (in pg_dumpall.c at line 646)

## Notes and Other Information
- Skips databases with datallowconn=false or datconnlimit=-2 to avoid connection failures
- Always skips template0 regardless of its datallowconn setting
- Processes template1 first, then other databases alphabetically to avoid --clean operation conflicts
- For system databases (template1, postgres), uses different options depending on --clean flag
- Handles file output by temporarily closing and reopening output files for each database dump
- Fatal error occurs if pg_dump fails on any database, ensuring data consistency
- Special handling for database exclusion list allows selective database dumping