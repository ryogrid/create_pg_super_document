# SupportRequestSelectivity

## Location
[src/include/nodes/supportnodes.h:91-107](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/supportnodes.h#L91-L107)

## Overview
SupportRequestSelectivity is a structure that enables support functions to provide custom selectivity estimates for boolean-returning functions appearing in WHERE clauses, helping PostgreSQL's query planner make better optimization decisions.

## Definition

```c
typedef struct SupportRequestSelectivity
{
	NodeTag		type;

	/* Input fields: */
	struct PlannerInfo *root;	/* Planner's infrastructure */
	Oid			funcid;			/* function we are inquiring about */
	List	   *args;			/* pre-simplified arguments to function */
	Oid			inputcollid;	/* function's input collation */
	bool		is_join;		/* is this a join or restriction case? */
	int			varRelid;		/* if restriction, RTI of target relation */
	JoinType	jointype;		/* if join, outer join type */
	struct SpecialJoinInfo *sjinfo; /* if outer join, info about join */

	/* Output fields: */
	Selectivity selectivity;	/* returned selectivity estimate */
} SupportRequestSelectivity;
```
## Detailed Description
The SupportRequestSelectivity structure allows PostgreSQL's planner to obtain custom selectivity estimates from support functions for boolean-returning functions used in WHERE clauses. This mechanism unifies the APIs for both restriction and join selectivity estimation into a single request type.

When a support function can provide a selectivity estimate, it stores the value (between 0 and 1 inclusive) in the selectivity field and returns the address of the SupportRequestSelectivity node. If no estimate can be made, the function returns NULL, causing the planner to fall back to a default estimate (traditionally 1/3).

This functionality is particularly important for custom data types and functions that have domain-specific knowledge about data distribution that the standard planner algorithms cannot capture.

## Parameters / Member Variables
**Input fields:**
- : NodeTag identifying this as a SupportRequestSelectivity structure
- : Pointer to PlannerInfo containing planner's infrastructure information
- : OID of the function being analyzed for selectivity
- : List of pre-simplified arguments to the function
- : OID of the function's input collation
- : Boolean indicating whether this is a join selectivity case or a restriction selectivity case
- : If this is a restriction case, the range table index (RTI) of the target relation
- : If this is a join case, specifies the type of outer join
- : If this is an outer join case, provides additional information about the join

**Output fields:**
- : The returned selectivity estimate (must be between 0 and 1 inclusive)

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag
  - PlannerInfo
  - JoinType
  - SpecialJoinInfo
  - Selectivity

- Called from (representative examples):
  - function_selectivity (src/backend/optimizer/util/plancat.c:2037)
  - like_regex_support (src/backend/utils/adt/like_support.c:160)
  - test_support_func (src/test/regress/regress.c:1035)

## Notes and Other Information
- This mechanism applies only to functions returning boolean values that appear at the top level of WHERE clauses
- If the target function is being used as the implementation of an operator, this support function will not be used; instead, the operator's restriction or join estimator is consulted
- The input arguments provided have already been pre-simplified by the planner
- The API unifies both restriction selectivity estimation (single relation) and join selectivity estimation (multiple relations) into one request type
- Support functions should return NULL if they cannot provide a meaningful estimate, allowing the planner to use default selectivity values
- This is part of PostgreSQL's extensible type system, allowing custom data types to provide query optimization hints