# SubscriptRoutines

## Location
[src/include/nodes/subscripting.h:158-165](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/subscripting.h#L158-L165)

## Overview
The  struct defines the interface for type-specific subscripting operations in PostgreSQL, providing function pointers and behavior flags that allow data types to customize how array-style subscripting (e.g., ) is handled during parsing and execution.

## Definition

```c
typedef struct SubscriptRoutines
{
	SubscriptTransform transform;	/* parse analysis function */
	SubscriptExecSetup exec_setup;	/* expression compilation function */
	bool		fetch_strict;	/* is fetch SubscriptRef strict? */
	bool		fetch_leakproof;	/* is fetch SubscriptRef leakproof? */
	bool		store_leakproof;	/* is assignment SubscriptRef leakproof? */
} SubscriptRoutines;
```
## Detailed Description
 is the core structure of PostgreSQL's generic type subscripting API, returned by SQL-visible subscript handler functions to define how a particular data type handles subscripting operations. Each data type that supports subscripting (like arrays, jsonb, etc.) provides its own implementation of these routines.

The structure is designed to support both fetch operations () and assignment operations (), with extensible support for slice operations and various strictness/leakproof behaviors. The subscripting framework uses this structure to dispatch to type-specific implementations during both parse analysis and execution phases.

The subscript handler function that returns this structure is declared as  but takes no actual parameters. It typically returns a pointer to a static const variable containing the routine definitions for that type.

## Parameters / Member Variables
- 0 0 0 GtsSurface GtsFace GtsEdge GtsVertex: Function pointer for parse analysis phase that processes subscript expressions, determines result types, and fills in the SubscriptingRef node during parsing
- : Function pointer for expression compilation phase that sets up execution methods and initializes workspace during executor startup
- : Boolean indicating whether fetch operations return NULL if any input (container or subscripts) is NULL
- : Boolean indicating whether fetch operations are leakproof (won't throw data-value-dependent errors)
- : Boolean indicating whether assignment operations are leakproof (assignments commonly throw errors for invalid subscripts)

## Dependencies
- Functions called/Symbols referenced:
  - SubscriptTransform (function pointer type)
  - SubscriptExecSetup (function pointer type)
  - [SubscriptingRef](SubscriptingRef.md) (node type used in subscripting)

- Called from (representative examples):
  - [getSubscriptingRoutines](../g/getSubscriptingRoutines.md) (retrieves subscripting routines for a type)
  - [array_subscript_handler](../a/array_subscript_handler.md) (returns routines for array types)
  - [jsonb_subscript_handler](../j/jsonb_subscript_handler.md) (returns routines for jsonb types)
  - [transformContainerSubscripts](../t/transformContainerSubscripts.md) (uses transform routine during parsing)
  - [ExecInitSubscriptingRef](../E/ExecInitSubscriptingRef.md) (uses exec_setup routine during execution)

## Notes and Other Information
- All SubscriptRefs are expected to be immutable (same inputs always produce same results) and parallel-safe
- The structure supports different behaviors for fetch vs store operations - there is no store_strict flag as null subscripts in assignments would make entire containers NULL
- The exec_setup method can initialize workspace in the SubscriptingRefState for sharing data between execution steps
- Types that don't support assignment need not provide sbs_assign or sbs_fetch_old methods
- The leakproof flags are important for security policies and query optimization, determining whether operations can leak information through error messages
- Located in src/include/nodes/subscripting.h:158-165