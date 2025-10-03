# _copyBitmapset

## Location
[src/backend/nodes/copyfuncs.c:164-176](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/copyfuncs.c#L164-L176)

## Overview
Creates a deep copy of a Bitmapset by delegating to the dedicated Bitmapset copy function .

## Definition

```c
static Bitmapset *
_copyBitmapset(const Bitmapset *from)
```
## Detailed Description
The  function provides a simple wrapper around PostgreSQL's dedicated Bitmapset copying functionality. It serves as an adapter that allows Bitmapsets to participate in the generic node copying system by conforming to the expected function signature while delegating the actual copying work to the specialized  function.

This design maintains consistency with the node copying framework while leveraging the existing, optimized Bitmapset copying logic that handles the internal structure of these specialized data structures used for representing sets of integers efficiently.

## Parameters / Member Variables
- `*from`: Pointer to the source Bitmapset to be copied
## Dependencies
- Functions called/Symbols referenced:
  - [bms_copy](../b/bms_copy.md) (dedicated function for copying Bitmapset structures)
- Called from (representative examples):
  - Part of the node copying system (called indirectly through copyObject)

## Notes and Other Information
- This is a static function, only accessible within copyfuncs.c
- Serves as a thin wrapper to integrate Bitmapsets into the generic node copying framework
- Bitmapsets are specialized data structures used throughout PostgreSQL for efficient set operations on integers
- The function delegates to  which handles the complex internal structure of Bitmapsets including their variable-length word arrays
- This approach maintains the separation of concerns between generic node copying and Bitmapset-specific operations