# multirange_unnest_fctx

## Location
[src/backend/utils/adt/multirangetypes.c:2720-2786](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes.c#L2720-L2786)

## Overview
A private struct definition used as function context for the  set-returning function (SRF) that decomposes a multirange into its individual constituent ranges.

## Definition

```c
int			index;
	} multirange_unnest_fctx;

	FuncCallContext *funcctx;
	multirange_unnest_fctx *fctx;
	MemoryContext oldcontext;

	/* stuff done only on the first call of the function */
	if (SRF_IS_FIRSTCALL())
```
## Detailed Description
The  struct serves as the function context for PostgreSQL's  set-returning function. This struct maintains state across multiple function calls, which is essential for set-returning functions that need to return one result at a time from a collection.

The struct is used to implement the "unnest" operation on multiranges, which takes a multirange containing multiple ranges and returns each individual range as a separate row in the result set. This is analogous to unnesting an array, but for multirange types.

The struct maintains:
- A reference to the multirange being processed
- Type cache information for efficient access to range operations
- An index to track which range within the multirange should be returned next

This design allows the function to be called repeatedly by PostgreSQL's set-returning function infrastructure, with each call returning the next range in sequence until all ranges have been returned.

## Parameters / Member Variables
- : Pointer to the MultirangeType being unnested. This holds the collection of ranges that will be returned one by one
- : Pointer to TypeCacheEntry containing cached type information for the multirange and its underlying range type, used for efficient access to type-specific operations
- : Integer tracking the current position within the multirange (0-based index indicating which range to return next)

## Dependencies
- Functions called/Symbols referenced:
  - Used within  function context
  - Relies on  structure for multirange representation
  - Uses  for type system integration
  - Interacts with PostgreSQL's SRF (Set Returning Function) infrastructure

- Called from (representative examples):
  - : The main function that uses this struct for state management
  - PostgreSQL SRF infrastructure during result set generation

## Notes and Other Information
- This is a private struct definition local to the multirangetypes.c file
- The struct lifetime is managed by PostgreSQL's function call context system
- Memory allocation for this struct occurs in the multi_call_memory_ctx to ensure it persists across function calls
- The struct follows PostgreSQL's standard pattern for set-returning function contexts
- The index field ensures that each range is returned exactly once and in the correct order
- The struct is essential for implementing SQL functions like  on multirange columns
- This design pattern is commonly used in PostgreSQL for functions that need to return multiple values from aggregate or complex types