# PageClearAllVisible

## Location
src/include/storage/bufpage.h: 437 - 444

## Overview
PageClearAllVisible clears the PD_ALL_VISIBLE flag from a page header, indicating that not all tuples on the page are visible to everyone.

## Definition
static inline void PageClearAllVisible(Page page)

## Detailed Description
PageClearAllVisible is an inline function that removes the PD_ALL_VISIBLE flag from a page's header flags field. This function is called when modifications are made to a page that invalidate the assumption that all tuples on the page are visible to all transactions. This typically happens during insert, update, or delete operations that add new tuples or modify existing ones in ways that make them not immediately visible to all transactions.

The function uses a bitwise AND operation with the complement of PD_ALL_VISIBLE to clear only that specific flag bit while preserving all other flags in the page header. This ensures that the visibility map will be updated accordingly to reflect that the page is no longer all-visible.

## Parameters / Member Variables
- page: A pointer to the page (Page type) whose PD_ALL_VISIBLE flag should be cleared

## Dependencies
- Functions called/Symbols referenced:
  - PageHeader (type cast)
  - PD_ALL_VISIBLE (flag constant 0x0004)
- Called from (representative examples):
  - mask_page_hint_bits
  - heap_insert
  - heap_multi_insert
  - heap_delete
  - heap_update
  - heap_xlog_delete
  - heap_xlog_insert
  - heap_xlog_multi_insert
  - heap_xlog_update
  - lazy_scan_prune

## Notes and Other Information
- This is an inline function defined in the header file for performance
- Called frequently during heap modification operations
- Critical for maintaining visibility map accuracy
- Used in both normal operations and WAL replay functions
- Ensures that pages with new or modified tuples are not incorrectly marked as all-visible
- The function uses bitwise operations to efficiently clear only the target flag bit
- Often triggers subsequent visibility map updates to clear the corresponding visibility map bit
- Essential for correctness of index-only scans and vacuum optimizations