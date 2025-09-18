# getCollations

## Location
src/bin/pg_dump/pg_dump.c: 6100 - 6171

## Overview
Reads all collations from the PostgreSQL system catalogs and returns them in a CollInfo structure array for pg_dump processing.

## Definition
```c
CollInfo *getCollations(Archive *fout, int *numCollations)
```

## Detailed Description
The getCollations function is part of pg_dump's catalog scanning infrastructure that retrieves all collation objects defined in the database. It queries the pg_collation system catalog to collect collation metadata including names, namespaces, owners, and encoding information. The function allocates an array of CollInfo structures to store the collation information and uses selectDumpableObject to determine which collations should be included in the dump based on the current dump configuration. This function operates during the schema discovery phase of pg_dump and is essential for preserving locale-specific sorting and character classification behavior.

## Parameters / Member Variables
- `fout`: Archive structure containing connection and dump configuration information
- `numCollations`: Output parameter that receives the total number of collations found

## Dependencies
- Functions called/Symbols referenced:
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - [findNamespace](../f/findNamespace.md)
  - [getRoleName](getRoleName.md)
  - [AssignDumpId](../A/AssignDumpId.md)
  - [selectDumpableObject](../s/selectDumpableObject.md)
  - pg_malloc
  - atooid
- Called from (representative examples):
  - [getSchemaData](getSchemaData.md)

## Notes and Other Information
- Retrieves all collations including builtin collations; system-defined collations are filtered at dump-out time rather than during collection
- Each collation is assigned a dump ID for dependency tracking
- The collencoding field stores the encoding associated with the collation
- Collations are critical for proper text sorting and comparison behavior in restored databases
- Memory allocation is done upfront for the entire collation array based on query results