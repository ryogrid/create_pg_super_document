# pushJsonbValueScalar

## Location
[src/backend/utils/adt/jsonb_util.c:637-727](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb_util.c#L637-L727)

## Overview
Performs the actual pushing operation for scalar or pseudo-scalar-array values during JSONB construction, handling different token types in the sequential processing workflow.

## Definition


## Detailed Description
This function is the core workhorse of the JSONB construction process, handling the actual manipulation of the parse state stack based on sequential processing tokens. It manages the creation and population of JSONB containers (arrays and objects), handles scalar value insertion, and maintains the hierarchical structure during parsing. The function operates as a state machine, with different behaviors for each token type:

- **WJB_BEGIN_ARRAY/WJB_BEGIN_OBJECT**: Creates new containers and pushes them onto the parse state stack
- **WJB_KEY/WJB_VALUE/WJB_ELEM**: Appends scalar values to the current container
- **WJB_END_ARRAY/WJB_END_OBJECT**: Finalizes containers and pops the parse state stack

The function ensures proper memory allocation for container elements and maintains the rawScalar flag for arrays that represent single scalar values.

## Parameters / Member Variables
- : Double pointer to the current parse state stack, allowing modification of the stack structure
- : The sequential processing token indicating what operation to perform
- : The scalar value to be processed (may be NULL for structural tokens)

## Dependencies
- Functions called/Symbols referenced:
  - [pushState](pushState.md) (creates new parse state level)
  - [appendKey](../a/appendKey.md) (adds key to object)
  - [appendValue](../a/appendValue.md) (adds value to object)
  - [appendElement](../a/appendElement.md) (adds element to array)
  - [uniqueifyJsonbObject](../u/uniqueifyJsonbObject.md) (removes duplicate keys from objects)
  - IsAJsonbScalar (validates scalar values)
  - [palloc](palloc.md) (memory allocation)
  - elog (error reporting)
- Called from (representative examples):
  - [pushJsonbValue](pushJsonbValue.md) (main entry point for JSONB value pushing)

## Notes and Other Information
- This is a static function internal to jsonb_util.c, not exposed in the public API
- Uses assertions extensively to validate input conditions and maintain invariants
- Handles the rawScalar flag for arrays that represent single scalar values in certain contexts
- Memory management is handled through PostgreSQL's memory context system
- Error handling uses PostgreSQL's elog mechanism for unrecognized tokens
- The function maintains the parse state stack structure crucial for nested container handling