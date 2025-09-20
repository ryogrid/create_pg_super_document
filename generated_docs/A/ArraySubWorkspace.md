# ArraySubWorkspace

## Location
[src/backend/utils/adt/arraysubs.c:29-45](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arraysubs.c#L29-L45)

## Overview
ArraySubWorkspace is a workspace structure used during array subscripting operations in PostgreSQL, containing cached type information and converted subscript indices for efficient array element access.

## Definition

```c
typedef struct ArraySubWorkspace
{
	/* Values determined during expression compilation */
	Oid			refelemtype;	/* OID of the array element type */
	int16		refattrlength;	/* typlen of array type */
	int16		refelemlength;	/* typlen of the array element type */
	bool		refelembyval;	/* is the element type pass-by-value? */
	char		refelemalign;	/* typalign of the element type */

	/*
	 * Subscript values converted to integers.  Note that these arrays must be
	 * of length MAXDIM even when dealing with fewer subscripts, because
	 * array_get/set_slice may scribble on the extra entries.
	 */
	int			upperindex[MAXDIM];
	int			lowerindex[MAXDIM];
} ArraySubWorkspace;
```
## Detailed Description
ArraySubWorkspace serves as a specialized workspace structure for PostgreSQL's array subscripting execution framework. It is allocated and populated during the setup phase of array subscript operations and contains both cached type system information and pre-converted subscript indices. The structure is designed to optimize array access operations by pre-computing and storing frequently needed metadata about the array and element types, avoiding repeated lookups during execution.

The workspace is part of the SubscriptingRefState.workspace mechanism, which allows different subscriptable types (arrays, jsonb, etc.) to maintain their own execution-specific data structures. This design provides type-specific optimization while maintaining a common subscripting interface.

## Parameters / Member Variables
- : OID identifying the PostgreSQL type of individual array elements
- : The storage length (typlen) of the array type itself
- : The storage length (typlen) of the array element type
- : Boolean indicating whether array elements are passed by value or by reference
- : Character indicating the alignment requirements (typalign) of the element type
- : Array of converted upper bound indices for subscript operations, sized to maximum dimensions (6)
- : Array of converted lower bound indices for slice operations, sized to maximum dimensions (6)

## Dependencies
- Functions called/Symbols referenced:
  - MAXDIM (constant defining maximum array dimensions as 6)
- Called from (representative examples):
  - [array_subscript_check_subscripts](../a/array_subscript_check_subscripts.md)
  - [array_subscript_fetch](../a/array_subscript_fetch.md)
  - [array_subscript_fetch_slice](../a/array_subscript_fetch_slice.md)
  - [array_subscript_assign](../a/array_subscript_assign.md)
  - [array_subscript_assign_slice](../a/array_subscript_assign_slice.md)
  - [array_subscript_fetch_old](../a/array_subscript_fetch_old.md)
  - [array_subscript_fetch_old_slice](../a/array_subscript_fetch_old_slice.md)
  - [array_exec_setup](../a/array_exec_setup.md)

## Notes and Other Information
The index arrays are always allocated with MAXDIM (6) entries regardless of the actual number of subscripts being used, because internal array manipulation functions like array_get_slice and array_set_slice may write to unused entries. This design choice prioritizes safety and simplicity over memory efficiency. The workspace is allocated using PostgreSQL's memory context system (palloc) and is automatically cleaned up when the query execution context ends.