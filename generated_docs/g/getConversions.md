# getConversions

## Location
[src/bin/pg_dump/pg_dump.c:6172-6239](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L6172-L6239)

## Overview
Reads all conversions from the PostgreSQL system catalogs and returns them in a ConvInfo structure array for pg_dump processing.

## Definition
```c
ConvInfo *getConversions(Archive *fout, int *numConversions)
```

## Detailed Description
The getConversions function is part of pg_dump's catalog scanning infrastructure that retrieves all conversion objects defined in the database. It queries the pg_conversion system catalog to collect conversion metadata including names, namespaces, and owners. Conversions in PostgreSQL define how to transform text from one character encoding to another. The function allocates an array of ConvInfo structures to store the conversion information and uses selectDumpableObject to determine which conversions should be included in the dump based on the current dump configuration. This function operates during the schema discovery phase of pg_dump.

## Parameters / Member Variables
- `fout`: Archive structure containing connection and dump configuration information
- `numConversions`: Output parameter that receives the total number of conversions found

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
- Retrieves all conversions including builtin conversions; system-defined conversions are filtered at dump-out time rather than during collection
- Each conversion is assigned a dump ID for dependency tracking
- Conversions are essential for multi-encoding database environments and proper character set handling
- The function stores basic metadata needed to recreate conversion objects during database restore
- Memory allocation is done upfront for the entire conversion array based on query results