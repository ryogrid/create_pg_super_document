# load_rangetype_info

## Location
[src/backend/utils/cache/typcache.c:914-971](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/typcache.c#L914-L971)

## Overview
A helper function that loads and caches comprehensive information about range types by querying the pg_range catalog and setting up associated function information structures.

## Definition

```c
structs */
	fmgr_info_cxt(cmpFnOid, &typentry->rng_cmp_proc_finfo,
				  CacheMemoryContext);
```
## Detailed Description
This function populates a TypeCacheEntry with detailed information about a range type by consulting the pg_range system catalog. It extracts essential range type properties including the subtype, collation, operator class, and specialized functions (canonical and subdiff), then sets up cached function manager information structures for efficient repeated access.

The function performs several key operations: it looks up the range definition in pg_range, extracts the subtype OID and related operator class information, resolves the comparison function needed for range operations, and initializes fmgrinfo structures for the canonical and subdiff functions if they exist. Finally, it creates a link to the element type's cache entry, which serves as a marker that the range type information has been fully loaded.

This lazy loading approach ensures that expensive range type setup only occurs when the range functionality is actually needed, improving system performance for applications that don't use range types extensively.

## Parameters / Member Variables
- : Pointer to the TypeCacheEntry structure to populate with range type information

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md), ReleaseSysCache (for pg_range catalog access)
  - [get_opclass_family](../g/get_opclass_family.md), get_opclass_input_type, get_opfamily_proc (for operator class resolution)
  - [fmgr_info_cxt](../f/fmgr_info_cxt.md) (for setting up function manager structures)
  - [lookup_type_cache](lookup_type_cache.md) (for loading element type information)
- Called from (representative examples):
  - [lookup_type_cache](lookup_type_cache.md) (when TYPECACHE_RANGE_INFO flag is requested)
  - [cache_range_element_properties](../c/cache_range_element_properties.md) (when analyzing range element properties)

## Notes and Other Information
- This is a static helper function only used within typcache.c
- Requires the type to already be validated as a range type (TYPTYPE_RANGE)
- Sets up function manager info for comparison, canonical, and subdiff functions
- The canonical function is optional (used for normalizing range bounds)
- The subdiff function is optional (used for computing differences between range bounds)
- Creates a circular reference by setting up the element type's cache entry
- Uses CacheMemoryContext for function manager structures to ensure they persist across queries
- The function assumes all required operator class support functions exist

## Simplified Source

```c
// Simplified version of load_rangetype_info
static void
load_rangetype_info(TypeCacheEntry *typentry)
{
    Form_pg_range range_info;
    HeapTuple range_tuple;
    Oid element_type_oid;
    Oid opclass_oid;
    Oid canonical_oid;
    Oid subdiff_oid;
    Oid opfamily_oid;
    Oid opclass_input_type;
    Oid comparison_func_oid;

    // Get range type information from pg_range catalog
    range_tuple = SearchSysCache1(RANGETYPE, ObjectIdGetDatum(typentry->type_id));
    if (!HeapTupleIsValid(range_tuple))
        elog(ERROR, "cache lookup failed for range type %u", typentry->type_id);
    range_info = (Form_pg_range) GETSTRUCT(range_tuple);

    // Extract basic range properties
    element_type_oid = range_info->rngsubtype;
    typentry->rng_collation = range_info->rngcollation;
    opclass_oid = range_info->rngsubopc;
    canonical_oid = range_info->rngcanonical;
    subdiff_oid = range_info->rngsubdiff;

    ReleaseSysCache(range_tuple);

    // Get operator class properties and comparison function
    opfamily_oid = get_opclass_family(opclass_oid);
    opclass_input_type = get_opclass_input_type(opclass_oid);
    typentry->rng_opfamily = opfamily_oid;

    comparison_func_oid = get_opfamily_proc(opfamily_oid, opclass_input_type, opclass_input_type, BTORDER_PROC);
    if (!RegProcedureIsValid(comparison_func_oid))
        elog(ERROR, "missing support function %d(%u,%u) in opfamily %u",
             BTORDER_PROC, opclass_input_type, opclass_input_type, opfamily_oid);

    // Set up cached function manager structs
    fmgr_info_cxt(comparison_func_oid, &typentry->rng_cmp_proc_finfo, CacheMemoryContext);
    if (OidIsValid(canonical_oid))
        fmgr_info_cxt(canonical_oid, &typentry->rng_canonical_finfo, CacheMemoryContext);
    if (OidIsValid(subdiff_oid))
        fmgr_info_cxt(subdiff_oid, &typentry->rng_subdiff_finfo, CacheMemoryContext);

    // Link to element type (marks range info as valid)
    typentry->rngelemtype = lookup_type_cache(element_type_oid, 0);
}
```

Key simplifications made:
- Used more descriptive variable names throughout
- Added clear comments for each major section
- Preserved all error handling as it's essential for function robustness
- Simplified the structure while maintaining all core functionality
- Reduced complexity without losing any essential logic