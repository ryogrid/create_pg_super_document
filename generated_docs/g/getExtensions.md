# getExtensions

## Location
[src/bin/pg_dump/pg_dump.c:5772-5846](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L5772-L5846)

## Overview
Reads all extensions from the PostgreSQL system catalogs and returns them as an array of ExtensionInfo structures for pg_dump processing.

## Definition

```c
ExtensionInfo *
getExtensions(Archive *fout, int *numExtensions)
```
## Detailed Description
This function is part of pg_dump's metadata collection process that queries the pg_extension and pg_namespace system catalogs to retrieve information about all installed extensions. Each extension is converted into an ExtensionInfo structure containing the necessary metadata for dumping, including configuration tables and their filter conditions.

The function performs these key operations:
1. Executes a JOIN query between pg_extension and pg_namespace to get complete extension metadata
2. Creates ExtensionInfo structures for each extension with proper dump object initialization
3. Extracts extension properties including name, namespace, version, relocatability, and configuration
4. Determines which extensions should be dumped based on dump options via selectDumpableExtension
5. Handles extension configuration tables (extconfig) and their conditions (extcondition)

## Parameters / Member Variables
- : Archive structure containing dump configuration and output methods
- : Output parameter that receives the total number of extensions found

## Dependencies
- Functions called/Symbols referenced:
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - atooid
  - [AssignDumpId](../A/AssignDumpId.md)
  - [selectDumpableExtension](../s/selectDumpableExtension.md)
  - pg_malloc
  - [pg_strdup](../p/pg_strdup.md)
- Called from (representative examples):
  - [getSchemaData](getSchemaData.md)

## Notes and Other Information
- Joins pg_extension with pg_namespace to get namespace names along with extension data
- Handles extension relocatability as a boolean flag from database
- Stores extension configuration tables and their filter conditions for data dumping
- Uses selectDumpableExtension to respect dump filtering options
- Memory allocation uses pg_malloc for the ExtensionInfo array
- Returns allocated array that must be freed by caller
- Essential for proper extension handling during database dumps and restores