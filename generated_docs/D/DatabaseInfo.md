# DatabaseInfo

## Location
[src/bin/pg_amcheck/pg_amcheck.c:149-154](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_amcheck/pg_amcheck.c#L149-L154)

## Overview
DatabaseInfo is a structure used in PostgreSQL's pg_amcheck utility to store essential information about a database that needs to be checked, including the database name, amcheck extension schema, and unique constraint checking status.

## Definition
```c
typedef struct DatabaseInfo
{
    char       *datname;
    char       *amcheck_schema; /* escaped, quoted literal */
    bool        is_checkunique;
} DatabaseInfo;
```

## Detailed Description
The DatabaseInfo structure encapsulates the key attributes of a database that are relevant for pg_amcheck operations. It stores the database name for connection and identification purposes, tracks the schema where the amcheck extension is installed (formatted as an escaped, quoted literal for direct SQL usage), and maintains a flag indicating whether unique constraint checking is enabled for this particular database. This structure is used during the database compilation phase to organize and prepare databases for integrity checking operations.

## Parameters / Member Variables
- `datname`: Name of the database to be checked
- `amcheck_schema`: Schema where the amcheck extension is installed, formatted as an escaped and quoted SQL literal ready for query construction
- `is_checkunique`: Boolean flag indicating whether unique constraint checking should be performed on this database

## Dependencies
- Functions called/Symbols referenced:
  - (No direct function calls from struct definition)
- Called from (representative examples):
  - [RelationInfo](../R/RelationInfo.md) (contains DatabaseInfo member)
  - [compile_database_list](../c/compile_database_list.md)
  - [compile_relation_list_one_db](../c/compile_relation_list_one_db.md)
  - [main](../m/main.md) (in pg_amcheck)

## Notes and Other Information
- Defined in src/bin/pg_amcheck/pg_amcheck.c:149-154
- Used during the database compilation phase to organize databases for checking
- The amcheck_schema field is pre-formatted for SQL queries to avoid repeated escaping/quoting operations
- Part of the hierarchical structure used by pg_amcheck where databases contain relations to be checked
- The is_checkunique flag allows per-database control over unique constraint verification
- Used in conjunction with RelationInfo structures to represent the complete checking context