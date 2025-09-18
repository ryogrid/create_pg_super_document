# PageSetAllVisible

## Location
src/include/storage/bufpage.h: 432 - 436

## Overview
PageSetAllVisible sets the PD_ALL_VISIBLE flag on a page header, marking that all tuples on the page are visible to everyone.

## Definition
static inline void PageSetAllVisible(Page page)

## Detailed Description
PageSetAllVisible is an inline function that sets the PD_ALL_VISIBLE flag in a page's header flags field. This function is used to mark pages where all tuples are committed and visible to all current and future transactions. Setting this flag is an important optimization that allows the vacuum process and visibility map management to identify pages that can be safely marked as all-visible in the visibility map.

The function uses a bitwise OR operation to set the PD_ALL_VISIBLE flag bit while preserving all other existing flags in the page header. This operation is typically performed after vacuum operations determine that all tuples on a page are indeed visible to all transactions.

## Parameters / Member Variables
- page: A pointer to the page (Page type) whose PD_ALL_VISIBLE flag should be set

## Dependencies
- Functions called/Symbols referenced:
  - PageHeader (type cast)
  - PD_ALL_VISIBLE (flag constant 0x0004)
- Called from (representative examples):
  - [heap_multi_insert](../h/heap_multi_insert.md)
  - [heap_xlog_visible](../h/heap_xlog_visible.md)
  - [heap_xlog_multi_insert](../h/heap_xlog_multi_insert.md)
  - [lazy_scan_new_or_empty](../l/lazy_scan_new_or_empty.md)
  - [lazy_scan_prune](../l/lazy_scan_prune.md)
  - [lazy_vacuum_heap_page](../l/lazy_vacuum_heap_page.md)

## Notes and Other Information
- This is an inline function defined in the header file for performance
- Used primarily by vacuum operations and WAL replay functions
- Critical for maintaining the visibility map which enables index-only scans
- The flag is set when vacuum determines all tuples on a page are visible to all transactions
- Often called in conjunction with visibility map updates
- Used during both normal vacuum operations and WAL recovery
- The function uses bitwise OR to safely set the flag without affecting other flags