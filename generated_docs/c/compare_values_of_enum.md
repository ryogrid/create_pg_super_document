# compare_values_of_enum

## Location
src/backend/utils/cache/typcache.c: 2477 - 2549

## Overview
compare_values_of_enum compares two members of an enum type and returns their relative ordering, using cached enum data and optimizations for performance.

## Definition
int compare_values_of_enum(TypeCacheEntry *tcache, Oid arg1, Oid arg2)

## Detailed Description
This function provides a comprehensive comparison mechanism for enum values, implementing both fast and slow path algorithms depending on the characteristics of the values being compared. The function returns a standard comparison result: negative if arg1 < arg2, zero if equal, and positive if arg1 > arg2.

The function employs a two-tier strategy:
1. **Fast path**: If both enum values are known to be sortable by direct OID comparison (tracked via enum_known_sorted), it performs a simple OID comparison
2. **Slow path**: For values that require logical ordering, it looks up their actual sort positions in the enum definition

The function handles cache management automatically, loading enum data on first use and refreshing it when enum values are not found (indicating the enum has been modified). The cache refresh strategy is conservative but correct - it only refreshes when necessary, as enum value reordering is not supported, making cached comparisons reliable even when new values are added.

## Parameters / Member Variables
- `tcache`: Pointer to TypeCacheEntry containing cached type information and enum data
- `arg1`: First enum value OID to compare
- `arg2`: Second enum value OID to compare

## Dependencies
- Functions called/Symbols referenced:
  - [load_enum_cache_data](../l/load_enum_cache_data.md)
  - [enum_known_sorted](../e/enum_known_sorted.md)
  - [find_enumitem](../f/find_enumitem.md)
  - elog
  - [format_type_be](../f/format_type_be.md)
  - [TypeCacheEnumData](../T/TypeCacheEnumData.md)
  - [EnumItem](../E/EnumItem.md)
- Called from (representative examples):
  - [enum_cmp_internal](../e/enum_cmp_internal.md)

## Notes and Other Information
- Automatically handles cache initialization and refresh when enum definitions change
- Optimizes for the common case where enum values can be compared by OID directly
- Does not account for the special even/odd OID rule as that case is handled elsewhere
- Cache is only refreshed when unknown values are encountered, not proactively
- Generates errors for corrupt data when enum values cannot be found after cache refresh
- Part of PostgreSQL's enum type system providing efficient comparison operations