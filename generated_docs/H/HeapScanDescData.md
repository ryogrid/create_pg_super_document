# HeapScanDescData

## Location
src/include/access/heapam.h: 53 - 108

## Overview
HeapScanDescData is a structure that maintains the complete state and context information for heap table scans, supporting various scan types including sequential, parallel, bitmap, and streaming scans.

## Definition
```c
typedef struct HeapScanDescData
{
    TableScanDescData rs_base;          /* AM independent part of the descriptor */
    
    /* state set up at initscan time */
    BlockNumber rs_nblocks;             /* total number of blocks in rel */
    BlockNumber rs_startblock;          /* block # to start at */
    BlockNumber rs_numblocks;           /* max number of blocks to scan */
    
    /* scan current state */
    bool        rs_inited;              /* false = scan not init'd yet */
    OffsetNumber rs_coffset;            /* current offset # in non-page-at-a-time mode */
    BlockNumber rs_cblock;              /* current block # in scan, if any */
    Buffer      rs_cbuf;                /* current buffer in scan, if any */
    
    BufferAccessStrategy rs_strategy;   /* access strategy for reads */
    HeapTupleData rs_ctup;              /* current tuple in scan, if any */
    
    /* For scans that stream reads */
    ReadStream *rs_read_stream;
    ScanDirection rs_dir;               /* scan direction */
    BlockNumber rs_prefetch_block;
    
    /* For parallel scans */
    ParallelBlockTableScanWorkerData *rs_parallelworkerdata;
    
    /* For bitmap scans "skip fetch" optimization */
    Buffer      rs_vmbuffer;
    int         rs_empty_tuples_pending;
    
    /* page-at-a-time mode and bitmap scans */
    int         rs_cindex;              /* current tuple's index in vistuples */
    int         rs_ntuples;             /* number of visible tuples on page */
    OffsetNumber rs_vistuples[MaxHeapTuplesPerPage];  /* their offsets */
} HeapScanDescData;
```

## Detailed Description
HeapScanDescData serves as the comprehensive state descriptor for heap table scanning operations. It extends the generic TableScanDescData with heap-specific functionality and maintains all necessary context for different types of scans. The structure supports sequential scans, parallel worker scans, bitmap scans with visibility map optimizations, and streaming reads for improved I/O performance. It tracks the current scan position, manages buffer pins, and handles scan direction changes while supporting various optimization strategies like prefetching and skip-fetch for all-visible blocks.

## Parameters / Member Variables
- `rs_base`: Generic table scan descriptor containing AM-independent information
- `rs_nblocks`: Total number of blocks in the relation being scanned
- `rs_startblock`: Starting block number for the scan operation
- `rs_numblocks`: Maximum number of blocks to scan (InvalidBlockNumber means scan entire relation)
- `rs_inited`: Initialization flag indicating whether the scan has been properly set up
- `rs_coffset`: Current tuple offset within the current block for non-page-at-a-time scanning
- `rs_cblock`: Current block number being scanned
- `rs_cbuf`: Buffer containing the current block being scanned (with pin held if valid)
- `rs_strategy`: Buffer access strategy optimized for the specific scan type
- `rs_ctup`: Current tuple data for the tuple being examined
- `rs_read_stream`: Read stream object for streaming I/O operations
- `rs_dir`: Current scan direction (forward or backward)
- `rs_prefetch_block`: Block number for read-ahead prefetching
- `rs_parallelworkerdata`: Page allocation data for parallel scan workers (NULL for non-parallel scans)
- `rs_vmbuffer`: Visibility map buffer for bitmap scan optimizations
- `rs_empty_tuples_pending`: Count of NULL-filled tuples to return for skipped all-visible blocks
- `rs_cindex`: Current tuple index within the rs_vistuples array
- `rs_ntuples`: Number of visible tuples on the current page
- `rs_vistuples`: Array of offsets for visible tuples on the current page

## Dependencies
- Functions called/Symbols referenced:
  - [TableScanDescData](../T/TableScanDescData.md)
  - [BufferAccessStrategy](../B/BufferAccessStrategy.md)
  - [HeapTupleData](HeapTupleData.md)
  - [ReadStream](../R/ReadStream.md)
  - ScanDirection
  - [ParallelBlockTableScanWorkerData](../P/ParallelBlockTableScanWorkerData.md)
  - MaxHeapTuplesPerPage
- Called from (representative examples):
  - [heap_beginscan](../h/heap_beginscan.md)
  - [HeapScanDesc](HeapScanDesc.md)

## Notes and Other Information
- When rs_cbuf is not InvalidBuffer, the scan holds a pin on that buffer
- rs_numblocks is usually InvalidBlockNumber, meaning "scan whole relation"
- The structure supports both tuple-at-a-time and page-at-a-time scanning modes
- Bitmap scans can use the "skip fetch" optimization for all-visible blocks
- Read streams are allocated at scan start and reset on rescan or direction changes
- The visibility map buffer (rs_vmbuffer) is used for bitmap scan optimizations
- Array rs_vistuples stores offsets of visible tuples for efficient page processing