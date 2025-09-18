# tbm_calculate_entries

## Location
[src/backend/nodes/tidbitmap.c:1542-1558](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/tidbitmap.c#L1542-L1558)

## Overview
Estimates the number of hashtable entries that can fit within a specified memory budget for TID bitmap operations.

## Definition


## Detailed Description
This function calculates the maximum number of hash table entries that can be accommodated within a given memory limit for TID bitmap operations. The calculation considers the memory overhead of PagetableEntry structures plus two additional Pointer-sized entries per hashtable entry (accounting for arrays created during iteration readout). The function applies both safety and sanity limits: capping the result at INT_MAX - 1 to prevent integer overflow, and ensuring a minimum of 16 entries for basic functionality.

## Parameters / Member Variables
- `maxbytes`: Maximum memory budget in bytes for the hashtable

## Dependencies
- Functions called/Symbols referenced:
  - [PagetableEntry](../P/PagetableEntry.md) (struct type)
  - Pointer (type)
  - Min (macro)
  - Max (macro)
  - INT_MAX (constant)
- Called from (representative examples):
  - [tbm_create](tbm_create.md)
  - [compute_bitmap_pages](../c/compute_bitmap_pages.md)

## Notes and Other Information
- Used for memory-conscious planning of TID bitmap hash tables
- Critical for query planner cost estimation in compute_bitmap_pages
- Applies conservative memory accounting including iteration overhead
- Ensures reasonable bounds with minimum 16 entries and INT_MAX - 1 maximum
- Key component for managing memory usage in bitmap index scans and parallel operations