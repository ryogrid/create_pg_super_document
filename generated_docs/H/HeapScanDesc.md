# HeapScanDesc

## Location
src/include/access/heapam.h: 109 - 113

## Overview
HeapScanDesc is a typedef for a pointer to HeapScanDescData structure, serving as the standard handle for heap table scan operations throughout PostgreSQL.

## Definition
```c
typedef struct HeapScanDescData *HeapScanDesc;
```

## Detailed Description
HeapScanDesc is the primary interface type used for heap table scanning operations in PostgreSQL. It provides an opaque handle to the underlying HeapScanDescData structure, allowing functions to pass around scan descriptors without exposing the internal implementation details. This typedef enables a clean API separation between the scan interface and its implementation, supporting various scan types including sequential scans, parallel scans, bitmap scans, TID range scans, and streaming reads.

## Parameters / Member Variables
- This is a typedef that points to HeapScanDescData structure
- All actual state and member variables are contained in the referenced HeapScanDescData structure

## Dependencies
- Functions called/Symbols referenced:
  - [HeapScanDescData](HeapScanDescData.md)
- Called from (representative examples):
  - [heap_beginscan](../h/heap_beginscan.md)
  - [heap_endscan](../h/heap_endscan.md)
  - [heap_rescan](../h/heap_rescan.md)
  - [heap_getnext](../h/heap_getnext.md)
  - [heap_getnextslot](../h/heap_getnextslot.md)
  - [heapgettup](../h/heapgettup.md)
  - [heapgettup_pagemode](../h/heapgettup_pagemode.md)
  - [heap_set_tidrange](../h/heap_set_tidrange.md)
  - [heap_getnextslot_tidrange](../h/heap_getnextslot_tidrange.md)
  - [heapam_scan_bitmap_next_block](../h/heapam_scan_bitmap_next_block.md)
  - [heapam_scan_bitmap_next_tuple](../h/heapam_scan_bitmap_next_tuple.md)
  - [heapam_index_build_range_scan](../h/heapam_index_build_range_scan.md)
  - [heapam_index_validate_scan](../h/heapam_index_validate_scan.md)

## Notes and Other Information
- This is the standard public interface type for heap scan operations
- Used extensively throughout the heap access method implementation
- Provides type safety and encapsulation for scan descriptor passing
- The actual functionality resides in the HeapScanDescData structure it points to
- Commonly used in table AM (Access Method) handler functions
- Essential for all heap-based scanning operations including sequential, bitmap, parallel, and analysis scans