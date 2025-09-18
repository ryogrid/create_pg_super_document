# SubPlanState

## Location
[src/include/nodes/execnodes.h:960-992](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/execnodes.h#L960-L992)

## Overview
SubPlanState represents the execution state for subquery expressions during PostgreSQL query execution, managing both scalar and array subplans with optional hash table optimization for efficient evaluation.

## Definition


## Detailed Description
SubPlanState manages the execution state for subquery expressions such as EXISTS, IN, NOT IN, ANY, and ALL clauses. It coordinates between the outer query and subquery execution, providing optimization through hash table caching when the subquery is expected to be executed multiple times with different parameter values. The structure supports both scalar subqueries that return single values and array subqueries that return multiple rows as arrays.

## Parameters / Member Variables
- : NodeTag identifier for this node type
- : Pointer to the SubPlan expression plan node containing the subquery definition
- : State tree for executing the subselect plan
- : Parent plan node's state tree for accessing outer query context
- : Expression state for combining subplan results with outer expressions (e.g., equality tests)
- : List of expression states for arguments passed to the subplan
- : Cache of the most recently fetched tuple from the subplan
- : Cache of the most recent array result from ARRAY() subplans
- : Tuple descriptor for subselect output after projection
- : Projection info for transforming left-hand side expressions
- : Projection info for transforming subselect output
- : Hash table storing non-null subselect result rows for fast lookup
- : Separate hash table for rows containing null values
- : Flag indicating whether hashtable contains any rows
- : Flag indicating whether hashnulls contains any rows
- : Memory context for hash table storage
- : Temporary memory context for hash table operations
- : Expression context for evaluating inner tuple expressions
- : Number of columns involved in hash table operations
- : Array of column indices used as hash keys
- : Array of OIDs for equality functions used in table comparisons
- : Array of collation OIDs for hash and comparison operations
- : Array of hash function managers for table data types
- : Array of equality function managers for table data types
- : Array of hash function managers for left-hand side data types
- : Array of equality function managers for LHS vs. table comparisons
- : Expression state for equality comparisons between LHS and table

## Dependencies
- Functions called/Symbols referenced:
  - SubPlan
  - [ProjectionInfo](../P/ProjectionInfo.md)
  - [TupleHashTable](../T/TupleHashTable.md)
  - [PlanState](../P/PlanState.md)
  - ExprState
  - HeapTuple
  - [TupleDesc](../T/TupleDesc.md)
  - [MemoryContext](../M/MemoryContext.md)
  - ExprContext
  - [FmgrInfo](../F/FmgrInfo.md)
- Called from (representative examples):
  - [ExecSubPlan](../E/ExecSubPlan.md)
  - [ExecHashSubPlan](../E/ExecHashSubPlan.md)
  - [ExecScanSubPlan](../E/ExecScanSubPlan.md)
  - [ExecInitSubPlan](../E/ExecInitSubPlan.md)
  - [ExecSetParamPlan](../E/ExecSetParamPlan.md)

## Notes and Other Information
SubPlanState implements sophisticated caching mechanisms to optimize repeated subquery evaluation. The dual hash table approach (hashtable for non-nulls, hashnulls for nulls) ensures correct handling of SQL three-valued logic. The structure is particularly important for correlated subqueries where the subplan may be executed many times with different parameter values from the outer query.