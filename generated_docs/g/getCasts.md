# getCasts

## Location
[src/bin/pg_dump/pg_dump.c:8598-8690](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L8598-L8690)

## Overview
Retrieves basic information about most type casts in the PostgreSQL system for use by pg_dump, excluding certain automatically-created casts like range-to-multirange conversions.

## Definition

```c
CastInfo *
getCasts(Archive *fout, int *numCasts)
```
## Detailed Description
The  function queries the  system catalog to retrieve information about type cast definitions in the database. It implements version-specific logic to handle different PostgreSQL versions, with special filtering for PostgreSQL 14.0+ to exclude automatically-created casts from ranges to their corresponding multirange types.

The function constructs different SQL queries based on the server version: for PostgreSQL 14.0 and later, it includes a subquery to filter out range-to-multirange casts that are automatically created by the system. For each cast found, it creates a  structure and attempts to construct a descriptive name by concatenating the source and target type names.

## Parameters / Member Variables
- : Archive pointer for the pg_dump operation, used for executing SQL queries and version checking
- : Output parameter that receives the number of casts found

## Dependencies
- Functions called/Symbols referenced:
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - [CastInfo](../C/CastInfo.md)
  - [pg_malloc](../p/pg_malloc.md)
  - atooid
  - [AssignDumpId](../A/AssignDumpId.md)
  - [findTypeByOid](../f/findTypeByOid.md)
  - [initPQExpBuffer](../i/initPQExpBuffer.md)
  - [selectDumpableCast](../s/selectDumpableCast.md)
- Called from (representative examples):
  - [getSchemaData](getSchemaData.md)

## Notes and Other Information
- Version-dependent behavior: PostgreSQL 14.0+ excludes range-to-multirange casts
- Cast names are constructed by concatenating source and target type names for sorting purposes
- Results are ordered by source type OID, then target type OID (ORDER BY 3,4)
- If type information cannot be found, the cast name remains empty
- Each cast's dumpability is determined by selectDumpableCast()
- Handles all cast contexts (implicit, assignment, explicit) and methods (function, inout, binary)