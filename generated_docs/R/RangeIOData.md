# RangeIOData

## Location
[src/backend/utils/adt/rangetypes.c:50-55](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes.c#L50-L55)

## Overview
RangeIOData is a cache structure used by PostgreSQL's range type I/O functions to store frequently needed information during input/output operations for range data types.

## Definition

```c
typedef struct RangeIOData
{
	TypeCacheEntry *typcache;	/* range type's typcache entry */
	FmgrInfo	typioproc;		/* element type's I/O function */
	Oid			typioparam;		/* element type's I/O parameter */
} RangeIOData;
```
## Detailed Description
RangeIOData serves as a function-local cache (fn_extra) for range type I/O operations in PostgreSQL. When processing range types, the system needs to repeatedly access information about both the range type itself and its element type's I/O functions. Rather than looking up this information on every call, RangeIOData caches the essential data to improve performance.

The structure is allocated in the function's memory context and stored in the FunctionCallInfo's fn_extra field. It is automatically created and populated by the get_range_io_data() function when needed, and reused across multiple calls to the same range I/O function as long as the range type remains the same.

## Parameters / Member Variables
- `*typcache`: Pointer to the TypeCacheEntry for the range type, containing cached metadata about the range type including information about its element type
- `typioproc`: FmgrInfo structure containing the function manager information for the element type's I/O function (input, output, receive, or send)
- `typioparam`: OID parameter passed to the element type's I/O function, typically used for type-specific formatting or parsing parameters
## Dependencies
- Types referenced:
  - [TypeCacheEntry](../T/TypeCacheEntry.md) (from type cache system)
  - [FmgrInfo](../F/FmgrInfo.md) (from function manager)
  - Oid (object identifier type)
- Used by (functions that create/access RangeIOData):
  - [range_in](../r/range_in.md) (src/backend/utils/adt/rangetypes.c:95)
  - [range_out](../r/range_out.md) (src/backend/utils/adt/rangetypes.c:141)
  - [range_recv](../r/range_recv.md) (src/backend/utils/adt/rangetypes.c:183)
  - [range_send](../r/range_send.md) (src/backend/utils/adt/rangetypes.c:265)
  - [get_range_io_data](../g/get_range_io_data.md) (src/backend/utils/adt/rangetypes.c:319)

## Notes and Other Information
- This structure is specifically designed for range I/O functions and stores more cached information than other range functions typically need
- The cache is invalidated and rebuilt if the range type OID changes between function calls
- Memory for RangeIOData is allocated in the function's memory context (fn_mcxt) to ensure proper cleanup
- The structure optimizes performance by avoiding repeated lookups of type cache entries and I/O function information
- Used internally by PostgreSQL's range type system and not typically accessed directly by user code