# check_objfilter

## Location
[src/bin/scripts/vacuumdb.c:428-451](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/scripts/vacuumdb.c#L428-L451)

## Overview
Validates that the object filtering options specified on the vacuumdb command line are mutually compatible and prevents conflicting filter combinations.

## Definition
```c
void check_objfilter(void)
```

## Detailed Description
This function performs validation checks on the global `objfilter` variable to ensure that incompatible filtering options are not used together in the vacuumdb utility. It enforces logical constraints by checking for conflicting combinations of object filters and reports fatal errors when incompatible options are detected. The function helps prevent user errors that would result in ambiguous or contradictory vacuum operations.

The validation rules enforced are:
1. Cannot vacuum all databases and a specific database simultaneously
2. Cannot vacuum specific tables and all tables in schema(s) simultaneously  
3. Cannot vacuum specific tables while excluding schemas
4. Cannot vacuum all tables in schemas while also excluding schemas

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - OBJFILTER_ALL_DBS (filter flag for all databases)
  - OBJFILTER_DATABASE (filter flag for specific database)
  - OBJFILTER_TABLE (filter flag for specific tables)
  - OBJFILTER_SCHEMA (filter flag for schema-based filtering)
  - OBJFILTER_SCHEMA_EXCLUDE (filter flag for schema exclusion)
  - [pg_fatal](../p/pg_fatal.md) (error reporting function)
- Called from (representative examples):
  - [main](../m/main.md) (vacuumdb main function)
  - [VacObjFilter](../V/VacObjFilter.md) (object filter processing)

## Notes and Other Information
- Uses bitwise AND operations to check for simultaneous presence of conflicting flags
- Terminates the program with pg_fatal() when incompatible options are detected
- Part of the vacuumdb command-line utility validation system
- Helps ensure that vacuum operations have clear, unambiguous scope