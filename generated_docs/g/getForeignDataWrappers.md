# getForeignDataWrappers

## Location
[src/bin/pg_dump/pg_dump.c:9662-9751](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L9662-L9751)

## Overview
Reads all foreign data wrappers from the system catalogs and returns them in a structured format for pg_dump processing.

## Definition
```c
FdwInfo *getForeignDataWrappers(Archive *fout, int *numForeignDataWrappers)
```

## Detailed Description
This function is part of the pg_dump utility and is responsible for extracting complete information about foreign data wrappers from the PostgreSQL system catalog `pg_foreign_data_wrapper`. It constructs a comprehensive SQL query to retrieve all relevant FDW metadata including owner, handler, validator, access control lists, and options. The function processes the query results and creates an array of `FdwInfo` structures, each containing all necessary information for dumping and restoring foreign data wrappers. Each FDW is also assigned a dump ID and evaluated for dumpability based on the current dump criteria.

## Parameters / Member Variables
- `fout`: Archive handle for the pg_dump operation, used for executing SQL queries
- `numForeignDataWrappers`: Output parameter that receives the count of foreign data wrappers found

## Dependencies
- Functions called/Symbols referenced:
  - [createPQExpBuffer](../c/createPQExpBuffer.md)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - [PQntuples](../P/PQntuples.md)/PQfnumber/PQgetvalue/PQgetisnull
  - [pg_malloc](../p/pg_malloc.md)
  - atooid
  - [AssignDumpId](../A/AssignDumpId.md)
  - [getRoleName](getRoleName.md)
  - [selectDumpableObject](../s/selectDumpableObject.md)
  - [pg_strdup](../p/pg_strdup.md)
  - [PQclear](../P/PQclear.md)
  - [destroyPQExpBuffer](../d/destroyPQExpBuffer.md)
- Called from (representative examples):
  - [getSchemaData](getSchemaData.md)

## Notes and Other Information
- The function queries multiple FDW attributes in a single SQL statement for efficiency
- It uses `array_to_string` and `pg_options_to_table` to format FDW options as a readable string
- Access control information is retrieved using the `acldefault` function to get default privileges
- The returned `FdwInfo` array must be freed by the caller
- Each FDW is marked with `DO_FDW` object type for proper categorization in the dump process
- ACL components are conditionally included based on whether ACL information exists