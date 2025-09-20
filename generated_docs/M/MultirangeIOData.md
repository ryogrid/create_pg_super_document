# MultirangeIOData

## Location
[src/backend/utils/adt/multirangetypes.c:49-54](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes.c#L49-L54)

## Overview
A structure that serves as a cache entry for multirange type input/output functions, storing necessary information for efficient I/O operations on multirange data types.

## Definition

```c
typedef struct MultirangeIOData
{
	TypeCacheEntry *typcache;	/* multirange type's typcache entry */
	FmgrInfo	typioproc;		/* range type's I/O proc */
	Oid			typioparam;		/* range type's I/O parameter */
} MultirangeIOData;
```
## Detailed Description
MultirangeIOData is a caching structure used in PostgreSQL's multirange type system to optimize input/output operations. This structure is stored in the fn_extra field of function call context to avoid repeated lookups of type information during I/O operations. It contains cached information about the multirange type's properties and the underlying range type's I/O procedures, which are essential for parsing and formatting multirange values.

The structure is typically populated once per function call context and reused across multiple invocations, providing a performance optimization for multirange I/O operations by avoiding expensive type system lookups.

## Parameters / Member Variables
- : Pointer to the TypeCacheEntry for the multirange type, containing cached type metadata and operations
- : FmgrInfo structure containing the cached function manager information for the underlying range type's I/O procedure
- : OID parameter used by the range type's I/O procedure, typically specifying additional type-specific behavior

## Dependencies
- Functions called/Symbols referenced:
  - [TypeCacheEntry](../T/TypeCacheEntry.md) (type cache system)
  - [FmgrInfo](../F/FmgrInfo.md) (function manager system)
  - Oid (object identifier system)

- Called from (representative examples):
  - [multirange_in](../m/multirange_in.md)
  - [multirange_out](../m/multirange_out.md)  
  - [multirange_recv](../m/multirange_recv.md)
  - [multirange_send](../m/multirange_send.md)
  - [get_multirange_io_data](../g/get_multirange_io_data.md)

## Notes and Other Information
- This structure is used as a performance optimization in the fn_extra cache mechanism
- Located in src/backend/utils/adt/multirangetypes.c:49-54
- The structure is populated by get_multirange_io_data() function which handles the caching logic
- Essential for efficient multirange type I/O operations by avoiding repeated type system lookups
- Part of PostgreSQL's multirange type implementation introduced to support multiple non-overlapping ranges as a single value