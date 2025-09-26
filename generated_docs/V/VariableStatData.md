# VariableStatData

## Location
[src/include/utils/selfuncs.h:87-100](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/selfuncs.h#L87-L100)

## Overview
VariableStatData is a structure that holds statistical information about a variable or expression, used by PostgreSQL's query planner to estimate selectivity and costs during query optimization.

## Definition

```c
typedef struct VariableStatData
{
	Node	   *var;			/* the Var or expression tree */
	RelOptInfo *rel;			/* Relation, or NULL if not identifiable */
	HeapTuple	statsTuple;		/* pg_statistic tuple, or NULL if none */
	/* NB: if statsTuple!=NULL, it must be freed when caller is done */
	void		(*freefunc) (HeapTuple tuple);	/* how to free statsTuple */
	Oid			vartype;		/* exposed type of expression */
	Oid			atttype;		/* actual type (after stripping relabel) */
	int32		atttypmod;		/* actual typmod (after stripping relabel) */
	bool		isunique;		/* matches unique index or DISTINCT clause */
	bool		acl_ok;			/* true if user has SELECT privilege on all
								 * rows from the table or column */
} VariableStatData;
```
## Detailed Description
VariableStatData serves as a container for statistical data gathered about variables and expressions during query planning. It's returned by functions like examine_variable() and examine_simple_variable() which analyze query components to extract relevant statistics from the PostgreSQL system catalogs, particularly pg_statistic. This structure enables the query planner to make informed decisions about selectivity estimation, join ordering, and index usage by providing both the statistical data and metadata about the variable being analyzed.

The structure bridges the gap between abstract query expressions and concrete statistical information stored in the database catalogs, allowing selectivity functions to access histogram data, most common values, and other statistics needed for cost estimation.

## Parameters / Member Variables
- : Pointer to the Var node or expression tree being analyzed
- : Pointer to RelOptInfo structure for the relation, NULL if the relation cannot be identified
- : HeapTuple containing the pg_statistic row for this variable, NULL if no statistics available
- : Function pointer specifying how to free the statsTuple when done (important for memory management)
- : OID of the exposed/visible type of the expression
- : OID of the actual underlying type after stripping any RelabelType nodes
- : Type modifier of the actual type after stripping RelabelType nodes
- : Boolean indicating if this variable matches a unique index or appears in a DISTINCT clause
- : Boolean indicating if the current user has SELECT privilege on all relevant rows/columns

## Dependencies
- Functions called/Symbols referenced:
  - [Node](../N/Node.md) (expression tree node)
  - [RelOptInfo](../R/RelOptInfo.md) (relation optimization info)
  - HeapTuple (tuple from system catalogs)
  - Oid (object identifier type)

- Called from (representative examples):
  - [examine_variable](../e/examine_variable.md)
  - [examine_simple_variable](../e/examine_simple_variable.md)  
  - [get_restriction_variable](../g/get_restriction_variable.md)
  - [get_join_variables](../g/get_join_variables.md)
  - Various selectivity estimation functions (eqsel_internal, scalarineqsel, etc.)

## Notes and Other Information
- The statsTuple field requires careful memory management - it must be freed using the provided freefunc when the caller is done
- A convenience macro ReleaseVariableStats() is provided to safely free the statsTuple
- The distinction between vartype and atttype is important for handling type coercions and RelabelType nodes
- This structure is heavily used throughout PostgreSQL's selectivity estimation subsystem
- The acl_ok field enables security-aware statistics usage, ensuring privilege checks are respected during planning