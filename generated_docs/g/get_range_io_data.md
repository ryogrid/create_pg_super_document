# get_range_io_data

## Location
[src/backend/utils/adt/rangetypes.c:317-376](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes.c#L317-L376)

## Overview
The get_range_io_data function is a static utility function that retrieves and caches I/O metadata needed for range type operations, providing optimized access to element type I/O functions.

## Definition

```c
static RangeIOData *
get_range_io_data(FunctionCallInfo fcinfo, Oid rngtypid, IOFuncSelector func)
```
## Detailed Description
This function manages cached I/O information for range types to optimize repeated I/O operations. It stores a RangeIOData structure in the function call info's fn_extra field, which contains the type cache entry and prepared I/O function information for the range's element type. The function performs lazy initialization - it only creates new cache entries when needed (first call or type change) and reuses existing cache entries for subsequent calls with the same range type. It validates that the provided OID represents a valid range type and ensures the element type has the required I/O functions.

## Parameters / Member Variables
- `fcinfo`: Function call information structure used for caching
- `rngtypid`: OID of the range type for which I/O data is needed
- `func`: IOFuncSelector specifying which I/O function type is needed (input, output, receive, or send)
## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md): Allocates memory for the cache structure
  - [lookup_type_cache](../l/lookup_type_cache.md): Retrieves type cache information for the range type
  - [get_type_io_data](get_type_io_data.md): Obtains I/O function metadata for the element type
  - OidIsValid: Validates that an OID is valid
  - ereport/errcode/errmsg: Error reporting functions
  - [format_type_be](../f/format_type_be.md): Formats type names for error messages
  - [fmgr_info_cxt](../f/fmgr_info_cxt.md): Prepares function call information for the I/O function
- Data structures used:
  - [RangeIOData](../R/RangeIOData.md): Cache structure containing type cache and I/O function info
  - [FunctionCallInfo](../F/FunctionCallInfo.md): PostgreSQL function call information
  - IOFuncSelector: Enumeration specifying I/O function types
  - TYPECACHE_RANGE_INFO: Flag for type cache lookup
- Constants used:
  - IOFunc_receive/IOFunc_send: Enum values for binary I/O functions
  - ERRCODE_UNDEFINED_FUNCTION: Error code for missing functions
- Called from (representative examples):
  - [range_in](../r/range_in.md): Text input function for ranges
  - [range_out](../r/range_out.md): Text output function for ranges  
  - [range_recv](../r/range_recv.md): Binary receive function for ranges
  - [range_send](../r/range_send.md): Binary send function for ranges

## Notes and Other Information
- The function uses PostgreSQL's function call caching mechanism (fn_extra) to avoid repeated expensive lookups
- Cache validation checks both existence and type ID matching to handle cases where the cached data is for a different range type
- Error handling specifically addresses cases where binary I/O functions are not available for the element type
- The cache is allocated in the function's memory context to ensure proper lifetime management
- The function leverages get_type_io_data which provides more information than strictly needed but offers convenience
- Type cache lookup includes TYPECACHE_RANGE_INFO flag to ensure range-specific metadata is available

## Simplified Source

```c
static RangeIOData *
get_range_io_data(FunctionCallInfo fcinfo, Oid rngtypid, IOFuncSelector func) {
    RangeIOData *cache = (RangeIOData *) fcinfo->flinfo->fn_extra;

    // Check if cache exists and matches the required type
    if (cache == NULL || cache->typcache->type_id != rngtypid) {
        Oid typiofunc;

        // Allocate new cache structure
        cache = (RangeIOData *) MemoryContextAlloc(fcinfo->flinfo->fn_mcxt, sizeof(RangeIOData));

        // Get type cache for range type
        cache->typcache = lookup_type_cache(rngtypid, TYPECACHE_RANGE_INFO);
        if (cache->typcache->rngelemtype == NULL)
            elog(ERROR, "type %u is not a range type", rngtypid);

        // Get I/O function info for element type
        get_type_io_data(cache->typcache->rngelemtype->type_id, func,
                        NULL, NULL, NULL, NULL, &cache->typioparam, &typiofunc);

        // Validate I/O function exists
        if (!OidIsValid(typiofunc)) {
            if (func == IOFunc_receive)
                ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION),
                               errmsg("no binary input function available for type %s",
                                     format_type_be(cache->typcache->rngelemtype->type_id))));
            else
                ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION),
                               errmsg("no binary output function available for type %s",
                                     format_type_be(cache->typcache->rngelemtype->type_id))));
        }

        // Prepare function call info
        fmgr_info_cxt(typiofunc, &cache->typioproc, fcinfo->flinfo->fn_mcxt);
        fcinfo->flinfo->fn_extra = (void *) cache;
    }

    return cache;
}
```