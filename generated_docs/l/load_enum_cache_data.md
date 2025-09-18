# load_enum_cache_data

## Location
[src/backend/utils/cache/typcache.c:2550-2704](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/typcache.c#L2550-L2704)

## Overview
load_enum_cache_data loads or reloads cached enum type information from the system catalog, including enum values, sort order data, and optimization bitmaps for efficient comparisons.

## Definition
static void load_enum_cache_data(TypeCacheEntry *tcache)

## Detailed Description
This function performs a comprehensive loading of enum type metadata from the pg_enum system catalog into the type cache. It implements a sophisticated caching strategy that optimizes for both space and performance.

The function operates in several phases:
1. **Validation**: Confirms the type is actually an enum type
2. **Data Collection**: Scans pg_enum to collect all enum values and their sort orders
3. **Sorting**: Orders the collected items by OID for efficient access
4. **Optimization Analysis**: Creates a bitmap identifying which enum values can be compared directly by OID rather than requiring sort order lookups
5. **Memory Management**: Transfers all data to permanent cache memory context

The bitmap optimization is particularly sophisticated - it identifies the longest subsequence of enum values that maintain their sort order when compared by OID. This allows the comparison functions to use fast OID comparisons for many cases while falling back to explicit sort order comparisons only when necessary.

The function uses a heuristic approach to find the optimal bitmap, trading some accuracy for performance by identifying subsets that are correctly sorted even if they don't exactly match the original enum definition order.

## Parameters / Member Variables
- `tcache`: Pointer to TypeCacheEntry that will receive the loaded enum data

## Dependencies
- Functions called/Symbols referenced:
  - ereport, errcode, errmsg, format_type_be (error handling)
  - [palloc](../p/palloc.md), repalloc, pfree (memory management)
  - [ScanKeyInit](../S/ScanKeyInit.md), table_open, systable_beginscan, systable_getnext, systable_endscan, table_close (catalog access)
  - qsort, enum_oid_cmp (sorting)
  - [bms_make_singleton](../b/bms_make_singleton.md), bms_add_member, bms_copy, bms_free (bitmap operations)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md) (memory context management)
  - [TypeCacheEnumData](../T/TypeCacheEnumData.md), EnumItem, Form_pg_enum (data structures)
- Called from (representative examples):
  - [compare_values_of_enum](../c/compare_values_of_enum.md)

## Notes and Other Information
- This is a static function, only accessible within typcache.c
- Uses working memory during collection phase to minimize cache memory leaks on errors
- Implements sophisticated bitmap optimization for OID-based comparisons
- Handles dynamic memory expansion as enum values are discovered
- Creates optimization bitmaps with a practical size limit (8192 OID offset)
- Part of PostgreSQL's enum type caching system providing efficient enum operations
- Replaces any existing cached data when reloading
- Memory allocation strategy minimizes fragmentation in CacheMemoryContext