# getOperators

## Location
src/bin/pg_dump/pg_dump.c: 6018 - 6099

## Overview
Reads all operators from the PostgreSQL system catalogs and returns them in an OprInfo structure array for pg_dump processing.

## Definition


## Detailed Description
The getOperators function is part of pg_dump's catalog scanning infrastructure that retrieves all operators defined in the database. It queries the pg_operator system catalog to collect operator metadata including names, namespaces, owners, operand types, and implementation functions. The function allocates an array of OprInfo structures to store the operator information and uses selectDumpableObject to determine which operators should be included in the dump based on the current dump configuration. This function operates during the schema discovery phase of pg_dump.

## Parameters / Member Variables
- : Archive structure containing connection and dump configuration information
- : Output parameter that receives the total number of operators found

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
- Retrieves all operators including builtin operators; system-defined operators are filtered at dump-out time rather than during collection
- Each operator is assigned a dump ID for dependency tracking
- The function populates OprInfo structures with catalog metadata needed for proper operator recreation during restore
- Memory allocation is done upfront for the entire operator array based on query results