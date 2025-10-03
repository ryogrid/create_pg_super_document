# createBoundaryObjects

## Location
[src/bin/pg_dump/pg_dump.c:18698-18721](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L18698-L18721)

## Overview
Creates dummy DumpableObjects that represent logical boundaries between different sections of a database dump (pre-data and post-data boundaries).

## Definition
```c
static DumpableObject *createBoundaryObjects(void)
```

## Detailed Description
This function allocates and initializes two special DumpableObject instances that serve as logical markers to separate different phases of the database dump process. These boundary objects help organize the dump output into distinct sections:

1. **PRE-DATA BOUNDARY**: Marks the end of schema definitions and the beginning of data dumping
2. **POST-DATA BOUNDARY**: Marks the end of data dumping and the beginning of post-data operations (like constraints, indexes, etc.)

Each boundary object is assigned a unique dump ID and given a descriptive name for identification purposes. These objects don't correspond to actual database objects but serve as organizational markers in the dump dependency graph.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - [pg_malloc](../p/pg_malloc.md)
  - [AssignDumpId](../A/AssignDumpId.md)
  - [pg_strdup](../p/pg_strdup.md)
- Constants used:
  - DO_PRE_DATA_BOUNDARY
  - DO_POST_DATA_BOUNDARY
  - nilCatalogId
- Called from:
  - [main](../m/main.md) (in src/bin/pg_dump/pg_dump.c:996)

## Notes and Other Information
- Static function, only accessible within pg_dump.c
- Returns a pointer to an array of 2 DumpableObject structures
- The boundary objects use nilCatalogId since they don't correspond to actual catalog objects
- Memory allocation uses pg_malloc, which provides error handling for allocation failures
- These objects are crucial for the three-phase dump structure: pre-data, data, and post-data
- The boundary objects participate in dependency sorting to ensure proper dump section organization