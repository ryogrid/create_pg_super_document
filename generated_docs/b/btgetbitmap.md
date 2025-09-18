# btgetbitmap

## Location
src/backend/access/nbtree/nbtree.c: 266 - 311

## Overview
The btgetbitmap function performs a bitmap index scan by collecting all matching heap tuple IDs from a B-tree index and adding them to a TID bitmap structure.

## Definition


## Detailed Description
The btgetbitmap function implements bitmap index scanning for B-tree indexes, which is an efficient way to collect large numbers of matching tuples for later heap access. Instead of returning tuples one at a time like btgettuple, this function collects all heap tuple IDs (TIDs) that match the scan conditions and adds them to a bitmap data structure. This approach is particularly efficient for queries that need to access many tuples, as it allows for optimized heap access patterns and can be combined with bitmaps from other indexes.

The function performs complete scans in forward direction only, handling array key scenarios by iterating through multiple primitive scans. It directly accesses the current scan position to efficiently extract heap TIDs without the overhead of full tuple construction, making it faster than tuple-at-a-time scanning for bulk operations.

## Parameters / Member Variables
- : IndexScanDesc containing the scan state and parameters
- : TIDBitmap structure where matching heap tuple IDs will be added

## Dependencies
- Functions called/Symbols referenced:
  - _bt_first (initializes scan and gets first tuple)
  - _bt_next (advances scan to next tuple)
  - _bt_start_prim_scan (starts new primitive scan for array keys)
  - tbm_add_tuples (adds heap TIDs to the bitmap)
  - BTScanOpaque, IndexScanDesc, TIDBitmap (type definitions)
  - ForwardScanDirection (constant for scan direction)
- Called from (representative examples):
  - bthandler (registered as amgetbitmap callback)
  - Bitmap index scan nodes in query execution
  - Multi-index bitmap operations (BitmapOr, BitmapAnd nodes)

## Notes and Other Information
- Returns the count of tuples added to the bitmap as an int64
- Always scans in forward direction regardless of the original scan direction
- More efficient than btgettuple for bulk tuple collection
- Supports array key scans through primitive scan iteration
- Does not construct full index tuples, only extracts heap TIDs
- Essential for bitmap heap scan optimization in PostgreSQL query execution
- Part of PostgreSQL's bitmap scan infrastructure for efficient multi-tuple access
- The bitmap can later be used to access heap tuples in physical order for better I/O performance