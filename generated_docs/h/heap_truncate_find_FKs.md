# heap_truncate_find_FKs

## Location
src/backend/catalog/heap.c: 3249 - 3375

## Overview
Finds relations having foreign keys that reference any of the given relations, used during TRUNCATE operations to identify dependent tables that need to be checked.

## Definition
```c
List *heap_truncate_find_FKs(List *relationIds)
```

## Detailed Description
This function scans the pg_constraint system catalog to find all foreign key constraints that reference any of the tables specified in the input list. It performs a comprehensive search that handles partitioned table hierarchies by following parent constraint relationships. The function ensures that all dependent tables are discovered, including those connected through partitioned table inheritance chains.

The function performs two main phases:
1. **Initial scan**: Scans all foreign key constraints in pg_constraint to find direct references to the input relations
2. **Parent constraint processing**: For partitioned tables, follows parent constraint relationships to ensure all partitions and their dependencies are discovered

The function automatically restarts the scan if new referenced relations are discovered during parent constraint processing, ensuring complete coverage of all dependency chains.

## Parameters / Member Variables
- `relationIds`: List of relation OIDs for which to find foreign key references

## Dependencies
- Functions called/Symbols referenced:
  - [list_copy](../l/list_copy.md)
  - table_open
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [systable_endscan](../s/systable_endscan.md)
  - table_close
  - [list_member_oid](../l/list_member_oid.md)
  - lappend_oid
  - [list_append_unique_oid](../l/list_append_unique_oid.md)
  - [list_free](../l/list_free.md)
  - [list_sort](../l/list_sort.md)
  - [list_oid_cmp](../l/list_oid_cmp.md)
  - [list_deduplicate_oid](../l/list_deduplicate_oid.md)
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - GETSTRUCT
- Called from (representative examples):
  - [heap_truncate_check_FKs](heap_truncate_check_FKs.md)
  - [ExecuteTruncateGuts](../E/ExecuteTruncateGuts.md)

## Notes and Other Information
- Requires appropriate locks on all input relations to ensure stable results
- Uses sequential scan of pg_constraint due to lack of index on confrelid
- Returns a sorted, deduplicated list excluding input relations
- Handles partitioned table hierarchies by following conparentid relationships
- The restart mechanism ensures all dependency chains are fully traversed
- Results are sorted by OID to ensure consistent behavior in regression tests