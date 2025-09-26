# NamedTuplestoreScan

## Location
[src/include/nodes/plannodes.h:651-655](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/plannodes.h#L651-L655)

## Overview
NamedTuplestoreScan represents a plan node for scanning Ephemeral Named Relations (ENRs) in PostgreSQL's query execution tree, providing access to temporary named data sets stored in tuple stores.

## Definition
```c
typedef struct NamedTuplestoreScan
{
    Scan        scan;
    char       *enrname;        /* Name given to Ephemeral Named Relation */
} NamedTuplestoreScan;
```

## Detailed Description
NamedTuplestoreScan is a specialized plan node that handles the scanning of Ephemeral Named Relations (ENRs), which are temporary named data sets that exist only during query execution. It extends the base Scan node to provide functionality for accessing data stored in tuple stores that have been given specific names for reference within a query or transaction context.

This node type is particularly important for advanced PostgreSQL features that require temporary storage of intermediate results with named access. ENRs are used in scenarios such as trigger transitions (NEW TABLE, OLD TABLE), recursive CTEs, and other contexts where temporary data needs to be accessible by name. The node stores the name of the ENR and provides the mechanism to scan through the associated tuple store data.

## Parameters / Member Variables
- `scan`: Base Scan structure containing common scanning information like target lists, qualifications, and plan node metadata
- `enrname`: Character pointer containing the name assigned to the Ephemeral Named Relation for identification and lookup

## Dependencies
- Functions called/Symbols referenced:
  - Scan (base structure)
  
- Called from (representative examples):
  - ExecInitNamedTuplestoreScan (executor initialization)
  - create_namedtuplestorescan_plan (plan creation)
  - make_namedtuplestorescan (plan node construction)
  - set_plan_refs (plan reference setting)

## Notes and Other Information
- Critical for implementing advanced SQL features requiring temporary named data access
- Used extensively in trigger processing for OLD TABLE and NEW TABLE references
- Supports PostgreSQL's implementation of SQL standard transition table features
- Enables efficient access to intermediate results in complex query scenarios
- Part of PostgreSQL's comprehensive support for temporary data management
- Works in conjunction with tuple store mechanisms for efficient data storage and retrieval
- Essential for recursive query processing and advanced analytical operations
- Provides named access to ephemeral data without requiring permanent table creation