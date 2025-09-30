# get_sort_group_operators

## Location
[src/backend/parser/parse_oper.c:180-237](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_oper.c#L180-L237)

## Overview
Retrieves the default sorting and grouping operators (<, =, >) for a given datatype, with efficient bulk lookup and hashability detection.

## Definition
```c
void get_sort_group_operators(Oid argtype, bool needLT, bool needEQ, bool needGT, 
                             Oid *ltOpr, Oid *eqOpr, Oid *gtOpr, bool *isHashable)
```

## Detailed Description
get_sort_group_operators efficiently fetches the three fundamental comparison operators for a datatype all at once to reduce lookup overhead. It uses the type cache system to ensure consistent results from matching operator classes. The function supports types that may only have equality operators (hashable but not sortable) and provides detailed error reporting when required operators are missing. It also determines whether the equality operator supports hashing, which is crucial for hash-based operations like hash joins and hash aggregation.

## Parameters / Member Variables
- `argtype`: OID of the datatype to look up operators for
- `needLT`: If true, require less-than operator and error if missing
- `needEQ`: If true, require equality operator and error if missing  
- `needGT`: If true, require greater-than operator and error if missing
- `ltOpr`: Output pointer for less-than operator OID (can be NULL)
- `eqOpr`: Output pointer for equality operator OID (can be NULL)
- `gtOpr`: Output pointer for greater-than operator OID (can be NULL)
- `isHashable`: Output pointer for hashability flag (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [lookup_type_cache](../l/lookup_type_cache.md)
  - TYPECACHE_LT_OPR
  - TYPECACHE_EQ_OPR
  - TYPECACHE_GT_OPR
  - TYPECACHE_HASH_PROC
- Called from (representative examples):
  - [std_typanalyze](../s/std_typanalyze.md)
  - [makeSortGroupClauseForSetOp](../m/makeSortGroupClauseForSetOp.md)
  - [addTargetToSortList](../a/addTargetToSortList.md)
  - [addTargetToGroupList](../a/addTargetToGroupList.md)

## Notes and Other Information
- Results are guaranteed to be exact or binary-compatible matches
- Uses type cache for consistent operator selection from matching opclasses
- Supports partial operator sets (e.g., equality-only for hash-only types)
- Critical for query planning decisions involving sorting, grouping, and set operations
- Provides helpful error messages with hints when operators are missing
- Part of PostgreSQL's type system and query optimization infrastructure

## Simplified Source

```c
void get_sort_group_operators(Oid argtype,
                             bool needLT, bool needEQ, bool needGT,
                             Oid *ltOpr, Oid *eqOpr, Oid *gtOpr,
                             bool *isHashable) {
    // Determine what to cache based on caller needs
    int cache_flags = TYPECACHE_LT_OPR | TYPECACHE_EQ_OPR | TYPECACHE_GT_OPR;
    if (isHashable != NULL) {
        cache_flags |= TYPECACHE_HASH_PROC;
    }

    // Look up operators in type cache for consistency
    TypeCacheEntry *typentry = lookup_type_cache(argtype, cache_flags);

    // Extract operator OIDs and hashability
    Oid lt_opr = typentry->lt_opr;
    Oid eq_opr = typentry->eq_opr;
    Oid gt_opr = typentry->gt_opr;
    bool hashable = OidIsValid(typentry->hash_proc);

    // Check for required operators and report errors
    if ((needLT && !OidIsValid(lt_opr)) || (needGT && !OidIsValid(gt_opr))) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION),
                       errmsg("could not identify an ordering operator for type %s",
                              format_type_be(argtype)),
                       errhint("Use an explicit ordering operator or modify the query.")));
    }

    if (needEQ && !OidIsValid(eq_opr)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION),
                       errmsg("could not identify an equality operator for type %s",
                              format_type_be(argtype))));
    }

    // Return results to caller (if requested)
    if (ltOpr) *ltOpr = lt_opr;
    if (eqOpr) *eqOpr = eq_opr;
    if (gtOpr) *gtOpr = gt_opr;
    if (isHashable) *isHashable = hashable;
}
```