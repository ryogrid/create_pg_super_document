# ForeignPath

## Location
src/include/nodes/pathnodes.h: 1869 - 1875

## Overview
ForeignPath represents a potential scan of a foreign table, foreign join, or foreign upper-relation, enabling Foreign Data Wrapper (FDW) extensions to integrate external data sources into PostgreSQL's query planning process.

## Definition

```c
typedef struct ForeignPath
{
	Path		path;
	Path	   *fdw_outerpath;
	List	   *fdw_restrictinfo;
	List	   *fdw_private;
} ForeignPath;
```
## Detailed Description
ForeignPath is a specialized path node used by Foreign Data Wrappers to represent access paths to external data sources. This path type provides a flexible framework for FDW implementations to integrate with PostgreSQL's cost-based optimizer, allowing external data sources to participate in query planning decisions.

The ForeignPath supports various scenarios including simple foreign table scans, foreign joins (where join processing is pushed down to the foreign server), and foreign upper-relations (for aggregate and other upper-level operations). FDWs are responsible for providing all cost estimates and execution details since the core PostgreSQL system cannot determine these for external data sources.

The structure accommodates complex scenarios like foreign joins where restrictinfo clauses can be evaluated as gating conditions, and provides fdw_private storage for FDW-specific execution parameters that need to be passed from planning to execution time.

## Parameters / Member Variables
- : Base Path structure containing common path information including pathtype (T_ForeignScan), cost estimates, row estimates, pathkeys, and parallel execution properties
- : Optional outer path for foreign joins, representing the local side of a join when the foreign side needs access to local data
- : List of RestrictInfo nodes containing join clauses for foreign joins, used to create gating Result plan nodes for pseudoconstant clause evaluation
- : List containing FDW-specific private data about the scan operation, passed from planning to execution phases (should use nodeToString()-compatible format for debugging)

## Dependencies
- Functions called/Symbols referenced:
  - Path (base structure)
  - List (for restrictinfo and private data)
- Called from (representative examples):
  - create_foreignscan_path (creates ForeignPath for base table scans)
  - create_foreign_join_path (creates ForeignPath for foreign joins)
  - create_foreign_upper_path (creates ForeignPath for upper-level operations)
  - create_foreignscan_plan (converts ForeignPath to execution plan)

## Notes and Other Information
- Never called directly from core PostgreSQL - only by FDW implementations
- FDWs must provide all cost estimates since core cannot calculate them for external sources
- Foreign joins currently do not support parameterization (limitation noted in code)
- The fdw_private data should be nodeToString()-compatible for debugging purposes
- Supports both simple foreign scans and complex operations like joins and aggregates
- Used extensively in postgres_fdw and other FDW extensions
- Can specify custom pathtargets different from the relation's default reltarget
- Integrates with PostgreSQL's parallel query execution when foreign sources support it