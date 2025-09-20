# SerialIOData

## Location
[src/backend/utils/adt/array_userfuncs.c:30-33](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/array_userfuncs.c#L30-L33)

## Overview
SerialIOData is a structure used for caching element-type data during array aggregation serialization to optimize performance by avoiding repeated catalog lookups.

## Definition

```c
typedef struct SerialIOData
{
	FmgrInfo	typsend;
} SerialIOData;
```
## Detailed Description
SerialIOData is a helper structure specifically designed for the array_agg_serialize function in PostgreSQL's array aggregation implementation. It serves as a cache to store the element type's binary output function information, preventing the need for repeated catalog lookups during the serialization process of array aggregation states.

The structure is allocated in the function's memory context and stored in fcinfo->flinfo->fn_extra for persistence across multiple calls to the same function instance. This caching mechanism significantly improves performance when serializing arrays with by-reference element types, as the type's send function information only needs to be looked up once.

## Parameters / Member Variables
- : FmgrInfo structure containing the cached binary output function information for the array element type, obtained via getTypeBinaryOutputInfo() and initialized with fmgr_info_cxt()

## Dependencies
- Functions called/Symbols referenced:
  - [FmgrInfo](../F/FmgrInfo.md) (PostgreSQL function manager info structure)
- Called from (representative examples):
  - [array_agg_serialize](../a/array_agg_serialize.md) (src/backend/utils/adt/array_userfuncs.c:671, 675, 681, 683)

## Notes and Other Information
- This structure is only used for by-reference element types; by-value types don't require the send function during serialization
- The structure is allocated in the function's memory context to ensure it persists for the lifetime of the function call
- Part of PostgreSQL's array aggregation serialization/deserialization infrastructure for parallel query processing
- The cached information is used with SendFunctionCall() to serialize individual array elements