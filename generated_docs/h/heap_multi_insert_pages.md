# heap_multi_insert_pages

## Location
src/backend/access/heap/heapam.c: 2277 - 2308

## Overview
heap_multi_insert_pages is a static helper function for heap_multi_insert() that computes the number of entire pages required to insert the remaining heap tuples, used to determine how much a relation needs to be extended.

## Definition


## Detailed Description
This function calculates how many pages will be needed to accommodate the remaining tuples in a multi-insert operation. It simulates the insertion process by iterating through the tuples starting from the 'done' index and tracking available space on each page. When a tuple cannot fit on the current page, it increments the page count and starts a new page. The calculation includes space for both the tuple data and its item identifier, ensuring accurate page count estimation for relation extension.

## Parameters / Member Variables
- `heaptuples`: Array of HeapTuple pointers containing the tuples to be inserted
- `done`: Index of the first tuple that hasn't been processed yet
- `ntuples`: Total number of tuples in the heaptuples array
- `saveFreeSpace`: Amount of free space to preserve on each page

## Dependencies
- Functions called/Symbols referenced:
  - SizeOfPageHeaderData (constant for page header size)
  - [ItemIdData](../I/ItemIdData.md) (struct for item identifier size calculation)
  - MAXALIGN (macro for alignment calculation)
- Called from:
  - [heap_multi_insert](heap_multi_insert.md)

## Notes and Other Information
- This is a static function, only accessible within heapam.c
- The function accounts for both tuple data size and ItemIdData overhead
- Uses MAXALIGN to ensure proper tuple alignment on pages
- The calculation helps optimize relation extension by determining exact page requirements upfront
- Returns the number of pages needed starting from 1 (assumes at least one page is needed)