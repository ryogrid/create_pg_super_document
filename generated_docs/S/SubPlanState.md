# SubPlanState

## Location
[src/include/nodes/execnodes.h:960-992](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/execnodes.h#L960-L992)

## Overview
SubPlanState represents the execution state for subquery expressions during PostgreSQL query execution, managing both scalar and array subplans with optional hash table optimization for efficient evaluation.

## Definition

```c
typedef struct SubPlanState
{
	NodeTag		type;
	SubPlan    *subplan;		/* expression plan node */
	struct PlanState *planstate;	/* subselect plan's state tree */
	struct PlanState *parent;	/* parent plan node's state tree */
	ExprState  *testexpr;		/* state of combining expression */
	List	   *args;			/* states of argument expression(s) */
	HeapTuple	curTuple;		/* copy of most recent tuple from subplan */
	Datum		curArray;		/* most recent array from ARRAY() subplan */
	/* these are used when hashing the subselect's output: */
	TupleDesc	descRight;		/* subselect desc after projection */
	ProjectionInfo *projLeft;	/* for projecting lefthand exprs */
	ProjectionInfo *projRight;	/* for projecting subselect output */
	TupleHashTable hashtable;	/* hash table for no-nulls subselect rows */
	TupleHashTable hashnulls;	/* hash table for rows with null(s) */
	bool		havehashrows;	/* true if hashtable is not empty */
	bool		havenullrows;	/* true if hashnulls is not empty */
	MemoryContext hashtablecxt; /* memory context containing hash tables */
	MemoryContext hashtempcxt;	/* temp memory context for hash tables */
	ExprContext *innerecontext; /* econtext for computing inner tuples */
	int			numCols;		/* number of columns being hashed */
	/* each of the remaining fields is an array of length numCols: */
	AttrNumber *keyColIdx;		/* control data for hash tables */
	Oid		   *tab_eq_funcoids;	/* equality func oids for table
									 * datatype(s) */
	Oid		   *tab_collations; /* collations for hash and comparison */
	FmgrInfo   *tab_hash_funcs; /* hash functions for table datatype(s) */
	FmgrInfo   *tab_eq_funcs;	/* equality functions for table datatype(s) */
	FmgrInfo   *lhs_hash_funcs; /* hash functions for lefthand datatype(s) */
	FmgrInfo   *cur_eq_funcs;	/* equality functions for LHS vs. table */
	ExprState  *cur_eq_comp;	/* equality comparator for LHS vs. table */
} SubPlanState;
```
## Detailed Description
SubPlanState manages the execution state for subquery expressions such as EXISTS, IN, NOT IN, ANY, and ALL clauses. It coordinates between the outer query and subquery execution, providing optimization through hash table caching when the subquery is expected to be executed multiple times with different parameter values. The structure supports both scalar subqueries that return single values and array subqueries that return multiple rows as arrays.

## Parameters / Member Variables
- `type`: NodeTag identifier for this node type
- `*subplan`: Pointer to the SubPlan expression plan node containing the subquery definition
- `*planstate`: State tree for executing the subselect plan
- `*parent`: Parent plan node's state tree for accessing outer query context
- `*testexpr`: Expression state for combining subplan results with outer expressions (e.g., equality tests)
- `*args`: List of expression states for arguments passed to the subplan
- `curTuple`: Cache of the most recently fetched tuple from the subplan
- `curArray`: Cache of the most recent array result from ARRAY() subplans
- `descRight`: Tuple descriptor for subselect output after projection
- `*projLeft`: Projection info for transforming left-hand side expressions
- `*projRight`: Projection info for transforming subselect output
- `hashtable`: Hash table storing non-null subselect result rows for fast lookup
- `hashnulls`: Separate hash table for rows containing null values
- `havehashrows`: Flag indicating whether hashtable contains any rows
- `havenullrows`: Flag indicating whether hashnulls contains any rows
- `hashtablecxt`: Memory context for hash table storage
- `hashtempcxt`: Temporary memory context for hash table operations
- `*innerecontext`: Expression context for evaluating inner tuple expressions
- `numCols`: Number of columns involved in hash table operations
- `*keyColIdx`: Array of column indices used as hash keys
- `*tab_eq_funcoids`: Array of OIDs for equality functions used in table comparisons
- `*tab_collations`: Array of collation OIDs for hash and comparison operations
- `*tab_hash_funcs`: Array of hash function managers for table data types
- `*tab_eq_funcs`: Array of equality function managers for table data types
- `*lhs_hash_funcs`: Array of hash function managers for left-hand side data types
- `*cur_eq_funcs`: Array of equality function managers for LHS vs. table comparisons
- `*cur_eq_comp`: Expression state for equality comparisons between LHS and table
## Dependencies
- Functions called/Symbols referenced:
  - [SubPlan](SubPlan.md)
  - [ProjectionInfo](../P/ProjectionInfo.md)
  - [TupleHashTable](../T/TupleHashTable.md)
  - [PlanState](../P/PlanState.md)
  - [ExprState](../E/ExprState.md)
  - HeapTuple
  - [TupleDesc](../T/TupleDesc.md)
  - [MemoryContext](../M/MemoryContext.md)
  - [ExprContext](../E/ExprContext.md)
  - [FmgrInfo](../F/FmgrInfo.md)
- Called from (representative examples):
  - [ExecSubPlan](../E/ExecSubPlan.md)
  - [ExecHashSubPlan](../E/ExecHashSubPlan.md)
  - [ExecScanSubPlan](../E/ExecScanSubPlan.md)
  - [ExecInitSubPlan](../E/ExecInitSubPlan.md)
  - [ExecSetParamPlan](../E/ExecSetParamPlan.md)

## Notes and Other Information
SubPlanState implements sophisticated caching mechanisms to optimize repeated subquery evaluation. The dual hash table approach (hashtable for non-nulls, hashnulls for nulls) ensures correct handling of SQL three-valued logic. The structure is particularly important for correlated subqueries where the subplan may be executed many times with different parameter values from the outer query.