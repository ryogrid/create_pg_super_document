# Plan

## Location
[src/include/nodes/plannodes.h:119-172](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/plannodes.h#L119-L172)

## Overview
Plan is the abstract base structure for all PostgreSQL execution plan nodes, containing common fields used by all plan node types including cost estimates, parallelization info, and structural data.

## Definition


## Detailed Description
Plan serves as the abstract superclass for all execution plan node types in PostgreSQL. It contains the common data that all plan nodes need, including cost information used by the planner, cardinality estimates, parallelization capabilities, and structural relationships between nodes.

The Plan structure is designed so that all specific plan node types (like SeqScan, HashJoin, etc.) have Plan as their first field, allowing for safe casting between specific plan types and the generic Plan type. This inheritance-like pattern is commonly used throughout PostgreSQL's codebase.

The structure includes cost estimates that guide the planner's decisions, parallel execution metadata, and parameter tracking for efficient plan re-execution. The tree structure is maintained through lefttree and righttree pointers, with additional initPlan nodes for uncorrelated subqueries.

## Parameters / Member Variables
- : Node tag identifying the specific plan node type
- : Estimated cost before returning the first tuple
- : Estimated total cost if all tuples are fetched
- : Planner's estimate of the number of rows this node will produce
- : Average width in bytes of rows produced by this node
- : True if this node can take advantage of parallel execution
- : True if this node is safe to execute in parallel with other nodes
- : True if this node supports asynchronous execution
- : Unique identifier for this node within the plan tree
- : List of expressions to be computed and returned by this node
- : List of qualification conditions (WHERE clauses) applied at this node
- : Left child plan node (primary input for most node types)
- : Right child plan node (used by join nodes, etc.)
- : List of uncorrelated subquery plans that must execute first
- : Set of external PARAM_EXEC parameter IDs affecting this node
- : Set of all PARAM_EXEC parameter IDs affecting this node (external + local)

## Dependencies
- Functions called/Symbols referenced:
  - Cost
  - Cardinality
  - NodeTag
  - [List](../L/List.md)
  - [Bitmapset](../B/Bitmapset.md)

- Called from (representative examples):
  - This is an abstract base structure used by all specific plan node types
  - Referenced through PlannedStmt.planTree
  - Used throughout the executor via generic Plan* pointers
  - Cast to specific plan types (SeqScan*, HashJoin*, etc.) in executor nodes

## Notes and Other Information
- This is an abstract structure - no Plan nodes are directly instantiated
- All concrete plan node types must have Plan as their first field for safe casting
- The cost estimates are used by the planner to choose between alternative plans
- Parameter tracking (extParam/allParam) enables efficient plan re-execution when only parameter values change
- Parallel execution capabilities are determined at planning time and stored in parallel_aware/parallel_safe flags
- The plan tree structure allows for complex nested operations through lefttree/righttree relationships
- initPlan nodes execute once before the main plan tree execution begins