# get_multirange_io_data

## Location
[src/backend/utils/adt/multirangetypes.c:416-476](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes.c#L416-L476)

## Overview
Retrieves and caches I/O function information needed for multirange type input/output operations, storing it in function call context for efficient reuse.

## Definition

```c
static MultirangeIOData *
get_multirange_io_data(FunctionCallInfo fcinfo, Oid mltrngtypid, IOFuncSelector func)
```
## Detailed Description
The  function is a caching utility that retrieves and stores I/O function information needed by multirange type I/O functions. It maintains a cache in the function's  field to avoid repeated lookups of type information.

The function performs these operations:
1. Checks if cached data exists and matches the requested multirange type
2. If cache miss or type mismatch:
   - Allocates new MultirangeIOData structure in function memory context
   - Looks up the multirange type cache entry
   - Validates that the type is actually a multirange type
   - Retrieves the underlying range type's I/O function information
   - Validates that required I/O functions exist for the requested operation
   - Caches the function manager information for the I/O function
   - Stores the cache in fn_extra for future use
3. Returns the cached MultirangeIOData structure

This caching mechanism significantly improves performance for repeated I/O operations on the same multirange type.

## Parameters / Member Variables
- `fcinfo`: Function call information structure containing context and caching
- `mltrngtypid`: OID of the multirange type to get I/O data for
- `func`: IOFuncSelector indicating which I/O function type (input, output, receive, send)
## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)
  - [lookup_type_cache](../l/lookup_type_cache.md)
  - TYPECACHE_MULTIRANGE_INFO
  - [get_type_io_data](get_type_io_data.md)  
  - [fmgr_info_cxt](../f/fmgr_info_cxt.md)
  - IOFunc_receive (constant)
  - [format_type_be](../f/format_type_be.md) (in error messages)
- Called from:
  - [multirange_in](../m/multirange_in.md) (src/backend/utils/adt/multirangetypes.c:138)
  - [multirange_out](../m/multirange_out.md) (src/backend/utils/adt/multirangetypes.c:311)
  - [multirange_recv](../m/multirange_recv.md) (src/backend/utils/adt/multirangetypes.c:348)
  - [multirange_send](../m/multirange_send.md) (src/backend/utils/adt/multirangetypes.c:386)

## Notes and Other Information
- Static function used internally by multirange I/O functions
- Uses PostgreSQL's type cache system for efficient type information lookup
- Allocates cache memory in the function's memory context for proper lifetime management
- Validates multirange type integrity and reports specific errors for missing I/O functions
- The cache persists across multiple calls to the same I/O function, improving performance
- Includes comprehensive error handling for invalid types and missing functions
- Leverages get_type_io_data() for underlying range type I/O function resolution

## Simplified Source

```c
static MultirangeIOData *
get_multirange_io_data(FunctionCallInfo fcinfo, Oid mltrngtypid, IOFuncSelector func)
{
    // Check if we have valid cached data
    MultirangeIOData *cache = (MultirangeIOData *) fcinfo->flinfo->fn_extra;

    if (cache == NULL || cache->typcache->type_id != mltrngtypid) {
        // Cache miss or type mismatch - need to rebuild cache

        // Allocate cache structure in function memory context
        cache = (MultirangeIOData *) MemoryContextAlloc(fcinfo->flinfo->fn_mcxt,
                                                       sizeof(MultirangeIOData));

        // Get multirange type information
        cache->typcache = lookup_type_cache(mltrngtypid, TYPECACHE_MULTIRANGE_INFO);
        if (cache->typcache->rngtype == NULL) {
            elog(ERROR, "type %u is not a multirange type", mltrngtypid);
        }

        // Get I/O function information for the underlying range type
        Oid typiofunc;
        int16 typlen;
        bool typbyval;
        char typalign, typdelim;

        get_type_io_data(cache->typcache->rngtype->type_id,
                        func,
                        &typlen, &typbyval, &typalign, &typdelim,
                        &cache->typioparam, &typiofunc);

        // Validate that the required I/O function exists
        if (!OidIsValid(typiofunc)) {
            if (func == IOFunc_receive) {
                ereport(ERROR,
                       (errcode(ERRCODE_UNDEFINED_FUNCTION),
                        errmsg("no binary input function available for type %s",
                               format_type_be(cache->typcache->rngtype->type_id))));
            } else {
                ereport(ERROR,
                       (errcode(ERRCODE_UNDEFINED_FUNCTION),
                        errmsg("no binary output function available for type %s",
                               format_type_be(cache->typcache->rngtype->type_id))));
            }
        }

        // Cache the function manager information
        fmgr_info_cxt(typiofunc, &cache->typioproc, fcinfo->flinfo->fn_mcxt);

        // Store cache for future use
        fcinfo->flinfo->fn_extra = (void *) cache;
    }

    return cache;
}
```