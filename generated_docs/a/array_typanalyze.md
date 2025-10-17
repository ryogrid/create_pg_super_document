# array_typanalyze

## Location
[src/backend/utils/adt/array_typanalyze.c:98-215](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/array_typanalyze.c#L98-L215)

## Overview
The array_typanalyze function serves as the typanalyze function for array columns during ANALYZE operations, setting up specialized statistics computation for array data types.

## Definition

```c
Datum
array_typanalyze(PG_FUNCTION_ARGS)
```
## Detailed Description
This function is PostgreSQL's specialized type analyzer for array columns during vacuum analyze operations. It first calls the standard typanalyze function to establish basic statistics collection, then enhances it with array-specific analysis capabilities. The function validates that the column is indeed an array type, retrieves necessary type information for the array's element type (including equality operators, comparison procedures, and hash functions), and sets up a custom statistics computation function (compute_array_stats) to handle array-specific statistical analysis.

The function stores all necessary type information in an ArrayAnalyzeExtraData structure, preserving the original standard statistics computation setup while overlaying array-specific analysis capabilities.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: VacAttrStats pointer containing column statistics information and analysis configuration
## Dependencies
- Functions called/Symbols referenced:
  - [std_typanalyze](../s/std_typanalyze.md)
  - [get_base_element_type](../g/get_base_element_type.md)
  - [lookup_type_cache](../l/lookup_type_cache.md)
  - [compute_array_stats](../c/compute_array_stats.md)
  - [VacAttrStats](../V/VacAttrStats.md)
  - ArrayAnalyzeExtraData
- Called from (representative examples):
  - Referenced in pg_type.h catalog definitions

## Notes and Other Information
- Returns false if standard typanalyze fails or if the column is not an array type
- Requires the array element type to have equality operator, comparison procedure, and hash function
- Preserves the original compute_stats function and extra_data for fallback scalar statistics
- The function maintains the minrows setting from std_typanalyze without modification
- Located in src/backend/utils/adt/array_typanalyze.c:98-215

## Simplified Source

```c
Datum array_typanalyze(PG_FUNCTION_ARGS) {
    VacAttrStats *stats = (VacAttrStats *) PG_GETARG_POINTER(0);

    // First run standard type analysis
    if (!std_typanalyze(stats))
        PG_RETURN_BOOL(false);

    // Verify this is actually an array type
    Oid element_typeid = get_base_element_type(stats->attrtypid);
    if (!OidIsValid(element_typeid))
        elog(ERROR, "array_typanalyze invoked for non-array type %u", stats->attrtypid);

    // Get element type information needed for array analysis
    TypeCacheEntry *typentry = lookup_type_cache(element_typeid,
                                                 TYPECACHE_EQ_OPR |
                                                 TYPECACHE_CMP_PROC_FINFO |
                                                 TYPECACHE_HASH_PROC_FINFO);

    // Require all necessary operators and procedures
    if (!OidIsValid(typentry->eq_opr) ||
        !OidIsValid(typentry->cmp_proc_finfo.fn_oid) ||
        !OidIsValid(typentry->hash_proc_finfo.fn_oid))
        PG_RETURN_BOOL(true);

    // Set up array-specific analysis data
    ArrayAnalyzeExtraData *extra_data = (ArrayAnalyzeExtraData *)
        palloc(sizeof(ArrayAnalyzeExtraData));

    extra_data->type_id = typentry->type_id;
    extra_data->eq_opr = typentry->eq_opr;
    extra_data->coll_id = stats->attrcollid;
    extra_data->typbyval = typentry->typbyval;
    extra_data->typlen = typentry->typlen;
    extra_data->typalign = typentry->typalign;
    extra_data->cmp = &typentry->cmp_proc_finfo;
    extra_data->hash = &typentry->hash_proc_finfo;

    // Preserve standard statistics computation for fallback
    extra_data->std_compute_stats = stats->compute_stats;
    extra_data->std_extra_data = stats->extra_data;

    // Replace with array-specific statistics computation
    stats->compute_stats = compute_array_stats;
    stats->extra_data = extra_data;

    PG_RETURN_BOOL(true);
}
```