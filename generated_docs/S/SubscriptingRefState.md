# SubscriptingRefState

## Location
[src/include/executor/execExpr.h:728-755](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/executor/execExpr.h#L728-L755)

## Overview
SubscriptingRefState provides workspace and state management for container subscripting operations (array and JSONB indexing) during expression evaluation, supporting both fetch and assignment operations.

## Definition

```c
typedef struct SubscriptingRefState
{
	bool		isassignment;	/* is it assignment, or just fetch? */

	/* workspace for type-specific subscripting code */
	void	   *workspace;

	/* numupper and upperprovided[] are filled at expression compile time */
	/* at runtime, subscripts are computed in upperindex[]/upperindexnull[] */
	int			numupper;
	bool	   *upperprovided;	/* indicates if this position is supplied */
	Datum	   *upperindex;
	bool	   *upperindexnull;

	/* similarly for lower indexes, if any */
	int			numlower;
	bool	   *lowerprovided;
	Datum	   *lowerindex;
	bool	   *lowerindexnull;

	/* for assignment, new value to assign is evaluated into here */
	Datum		replacevalue;
	bool		replacenull;

	/* if we have a nested assignment, sbs_fetch_old puts old value here */
	Datum		prevvalue;
	bool		prevnull;
} SubscriptingRefState;
```
## Detailed Description
SubscriptingRefState manages the execution state for container subscripting operations in PostgreSQL, handling both simple indexing (e.g., array[1]) and slice operations (e.g., array[1:3]). The structure supports both fetch operations (retrieving values) and assignment operations (setting values).

The state distinguishes between upper and lower indexes to support slice notation where ranges can be specified. For simple indexing, only upper indexes are used. The structure maintains arrays of index values, their null status, and whether each position is provided (to handle sparse index specifications).

For assignment operations, the structure stores both the new value to assign and can preserve the previous value for nested assignments. A type-specific workspace pointer allows different container types (arrays, JSONB) to maintain their own specialized state.

## Parameters / Member Variables
- `isassignment`: Boolean flag indicating whether this is an assignment operation (true) or just a fetch operation (false)
- `*workspace`: Void pointer to type-specific workspace used by container-specific subscripting code
- `numupper`: Number of upper index positions
- `*upperprovided`: Array indicating which upper index positions are explicitly provided
- `*upperindex`: Array of upper index Datum values
- `*upperindexnull`: Array indicating null status for upper indexes
- `numlower`: Number of lower index positions (for slice operations)
- `*lowerprovided`: Array indicating which lower index positions are explicitly provided
- `*lowerindex`: Array of lower index Datum values
- `*lowerindexnull`: Array indicating null status for lower indexes
- `replacevalue`: For assignments, the new Datum value to assign
- `replacenull`: Null flag for the replacement value
- `prevvalue`: Previous Datum value, used in nested assignments
- `prevnull`: Null flag for the previous value
## Dependencies
- Functions called/Symbols referenced:
  - (None - this is a data structure)
- Called from (representative examples):
  - [ExecInitSubscriptingRef](../E/ExecInitSubscriptingRef.md) (expression initialization)
  - [array_subscript_check_subscripts](../a/array_subscript_check_subscripts.md) (array subscript validation)
  - [array_subscript_fetch](../a/array_subscript_fetch.md) (array value retrieval)
  - [array_subscript_assign](../a/array_subscript_assign.md) (array value assignment)
  - [jsonb_subscript_fetch](../j/jsonb_subscript_fetch.md) (JSONB value retrieval)
  - [jsonb_subscript_assign](../j/jsonb_subscript_assign.md) (JSONB value assignment)
  - [ExprEvalStep](../E/ExprEvalStep.md) (used in subscripting evaluation steps)

## Notes and Other Information
- Designed as non-inline data for container operations that require substantial workspace
- Supports both simple indexing (array[1]) and slice operations (array[1:3])
- The upper/lower index distinction enables proper handling of slice notation
- Type-specific workspace allows extensibility for different container types
- Used by both array and JSONB subscripting implementations
- The provided arrays handle sparse index specifications where some positions may be omitted
- Assignment operations can preserve previous values for complex nested assignment scenarios