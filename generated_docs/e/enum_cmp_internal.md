# enum_cmp_internal

## Location
[src/backend/utils/adt/enum.c:252-305](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/enum.c#L252-L305)

## Overview
Internal comparison engine for PostgreSQL enum values that handles the core logic for comparing two enum OIDs, with optimizations for common cases and proper handling of enum type metadata.

## Definition

```c
static int
enum_cmp_internal(Oid arg1, Oid arg2, FunctionCallInfo fcinfo)
```
## Detailed Description
The function serves as the common comparison engine for all visible enum comparison functions (except enum_eq and enum_ne which can directly compare OIDs). It implements a three-tier comparison strategy:

1. **Fast equality check**: Returns 0 immediately if both OIDs are equal
2. **Even-numbered OID optimization**: For even-numbered OIDs (which have known correct ordering), performs direct numeric comparison without consulting metadata
3. **Full metadata lookup**: For odd-numbered OIDs or mixed cases, looks up the enum type information and delegates to compare_values_of_enum()

The function uses caching via fcinfo->flinfo->fn_extra to avoid repeated type cache lookups for the same enum type.

## Parameters / Member Variables
- `arg1`: First enum value OID to compare
- `arg2`: Second enum value OID to compare
- `fcinfo`: Function call information structure containing metadata and caching context
## Dependencies
- Functions called/Symbols referenced:
  - [FunctionCallInfo](../F/FunctionCallInfo.md) (structure type)
  - Form_pg_enum (structure type)
  - [lookup_type_cache](../l/lookup_type_cache.md)
  - [compare_values_of_enum](../c/compare_values_of_enum.md)
  - [SearchSysCache1](../S/SearchSysCache1.md), HeapTupleIsValid, ReleaseSysCache (system catalog access)
- Called from:
  - [enum_lt](enum_lt.md)
  - [enum_le](enum_le.md)
  - [enum_ge](enum_ge.md)
  - [enum_gt](enum_gt.md)
  - [enum_smaller](enum_smaller.md)
  - [enum_larger](enum_larger.md)
  - [enum_cmp](enum_cmp.md)

## Notes and Other Information
- The function contains an important optimization: even-numbered OIDs are assumed to have correct relative ordering, allowing direct numeric comparison
- Includes assertion checking to ensure fcinfo->flinfo is available even when taking fast-path exits
- Error handling for invalid enum OIDs with appropriate error codes
- Uses PostgreSQL's type cache system for efficient metadata lookup and caching

## Simplified Source

```c
static int enum_cmp_internal(Oid arg1, Oid arg2, FunctionCallInfo fcinfo) {
    TypeCacheEntry *type_cache;

    Assert(fcinfo->flinfo != NULL);

    // Fast path: equal OIDs are equal
    if (arg1 == arg2)
        return 0;

    // Optimization: even-numbered OIDs have correct ordering
    if ((arg1 & 1) == 0 && (arg2 & 1) == 0) {
        if (arg1 < arg2)
            return -1;
        else
            return 1;
    }

    // Get or create type cache entry
    type_cache = (TypeCacheEntry *) fcinfo->flinfo->fn_extra;
    if (type_cache == NULL) {
        // Look up enum type from first argument
        HeapTuple enum_tuple = SearchSysCache1(ENUMOID, ObjectIdGetDatum(arg1));
        if (!HeapTupleIsValid(enum_tuple)) {
            ereport(ERROR, "invalid internal value for enum");
        }

        Form_pg_enum enum_data = (Form_pg_enum) GETSTRUCT(enum_tuple);
        Oid enum_type_oid = enum_data->enumtypid;
        ReleaseSysCache(enum_tuple);

        // Cache the type information for future calls
        type_cache = lookup_type_cache(enum_type_oid, 0);
        fcinfo->flinfo->fn_extra = (void *) type_cache;
    }

    // Delegate to type cache comparison function
    return compare_values_of_enum(type_cache, arg1, arg2);
}
```