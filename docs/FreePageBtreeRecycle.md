# FreePageBtreeRecycle

## Overview
Manages the efficient recycling of B-tree pages within PostgreSQL's Free Page Manager by adding unused pages to a specialized recycling linked list rather than immediately returning them to the general free page pool. This sophisticated memory management optimization reduces allocation overhead during intensive B-tree operations by maintaining a readily available supply of properly formatted B-tree pages that can be quickly repurposed for future tree expansion needs.

## Definition
```c
static void FreePageBtreeRecycle(FreePageManager *fpm, Size pageno)
```

## Detailed Description
FreePageBtreeRecycle implements an advanced memory recycling mechanism within PostgreSQL's Free Page Manager, designed to optimize B-tree performance through intelligent page reuse strategies. When B-tree pages become unnecessary (typically during consolidation or tree restructuring operations), this function converts the page into a recycling list node rather than deallocating it immediately, preserving the investment in page allocation and initialization. The function carefully transforms the target page into a FreePageSpanLeader structure with appropriate magic number marking, single-page size specification, and proper linked list integration at the head of the recycling chain. The implementation handles all necessary pointer manipulations using relative pointer storage to maintain cross-process compatibility in shared memory environments, including updating both forward and backward links to preserve doubly-linked list integrity. The operation concludes by updating the recycling list head pointer and incrementing the recycling counter for accurate bookkeeping and monitoring purposes.

## Parameters / Member Variables
- `fpm`: Pointer to the FreePageManager structure containing the recycling list infrastructure and metadata, provides access to the btree_recycle list head and counter
- `pageno`: Page number of the B-tree page to be recycled, must be a valid page-aligned address within the Free Page Manager's managed memory segment

## Dependencies
- **Functions called/Symbols referenced**:
  - `fpm_segment_base` - Macro calculating the base address of the shared memory segment from the FreePageManager's self-reference for pointer resolution
  - `relptr_access` - Converts relative pointers to absolute addresses within the shared memory segment, enabling access to existing recycling list nodes
  - `fpm_page_to_pointer` - Converts page number to absolute pointer address within the memory segment for direct page structure access
  - `FREE_PAGE_SPAN_LEADER_MAGIC` - Magic number constant identifying recycled pages as valid span leader structures for consistency checking
  - `relptr_store` - Stores relative pointer values while handling NULL cases and ensuring proper cross-process pointer encoding
  - `FreePageSpanLeader` - Structure type repurposed for recycling list management, providing necessary linked list pointers and metadata
- **Called from (representative examples)**:
  - `FreePageBtreeCleanup` - Uses page recycling during B-tree cleanup operations when pages become unnecessary after consolidation
  - `FreePageBtreeRemovePage` - Calls recycling when removing pages from the B-tree structure during tree shrinking operations
  - `FreePageManagerPutInternal` - Leverages recycling during complex memory management operations involving B-tree restructuring

## Notes & Other Information
The recycling mechanism represents a sophisticated optimization that significantly improves PostgreSQL's memory management performance during intensive B-tree operations. By maintaining recycled pages in a ready-to-use state, the system avoids the overhead of repeated allocation and initialization cycles that would otherwise occur during frequent B-tree modifications. The reuse of FreePageSpanLeader structures for recycling demonstrates efficient code reuse - these structures naturally contain the doubly-linked list pointers needed for recycling queue management. The function's careful handling of linked list operations ensures that the recycling chain remains consistent even under concurrent access patterns. Performance benefits are most pronounced during workloads involving frequent B-tree growth and shrinkage, such as large batch memory operations or dynamic allocation patterns. The page-to-pointer conversion and magic number assignment ensure that recycled pages maintain proper structural integrity for future reuse. Thread safety relies on the Free Page Manager's broader locking mechanisms to prevent race conditions during linked list manipulation. The recycling counter increment provides valuable monitoring capabilities for system administrators and debugging scenarios, enabling tracking of recycling efficiency and utilization patterns.