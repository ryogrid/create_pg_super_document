# TableScanDesc

## Location
[src/include/access/relscan.h:52-62](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/relscan.h#L52-L62)

## Overview
TableScanDesc is a pointer type definition to TableScanDescData, serving as the standard handle for table scan operations throughout PostgreSQL.

## Definition
```c
typedef struct TableScanDescData *TableScanDesc;
```

## Detailed Description
TableScanDesc is a typedef that creates a pointer type to the TableScanDescData structure. This provides a convenient and consistent interface for passing table scan descriptors throughout the PostgreSQL codebase. The use of this typedef abstracts the implementation details and provides a clean API for table scanning operations. Each backend participating in a table scan maintains its own TableScanDesc in backend-private memory.

## Parameters / Member Variables
This is a pointer typedef, so it does not have direct member variables. It points to a TableScanDescData structure which contains the actual scan state and parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [TableScanDescData](TableScanDescData.md)
- Called from (representative examples):
  - Various table access method functions
  - [Scan](../S/Scan.md) execution nodes
  - System catalog scanning functions

## Notes and Other Information
This typedef is defined in src/include/access/relscan.h (line 52). It provides a standard handle type for table scan operations and is used extensively throughout PostgreSQL's table access layer. The typedef approach allows for potential future changes to the underlying implementation without requiring widespread code modifications. In parallel scanning scenarios, each worker process has its own TableScanDesc instance that references shared parallel scan state.