# GinScanEntryData

## Location
src/include/access/gin_private.h: 336 - 367

## Overview
GinScanEntryData represents a specific index search condition extracted from GIN scan queries, containing query information, current scan state, and result data structures.

## Definition
```c
typedef struct GinScanEntryData
{
    /* query key and other information from extractQueryFn */
    Datum       queryKey;
    GinNullCategory queryCategory;
    bool        isPartialMatch;
    Pointer     extra_data;
    StrategyNumber strategy;
    int32       searchMode;
    OffsetNumber attnum;
    
    /* Current page in posting tree */
    Buffer      buffer;
    
    /* current ItemPointer to heap */
    ItemPointerData curItem;
    
    /* for a partial-match or full-scan query, we accumulate all TIDs here */
    TIDBitmap  *matchBitmap;
    TBMIterator *matchIterator;
    TBMIterateResult *matchResult;
    
    /* used for Posting list and one page in Posting tree */
    ItemPointerData *list;
    int         nlist;
    OffsetNumber offset;
    
    bool        isFinished;
    bool        reduceResult;
    uint32      predictNumberResult;
    GinBtreeData btree;
} GinScanEntryData;
```

## Detailed Description
GinScanEntryData encapsulates all the state and data needed to process a single index search condition during a GIN index scan. Each entry represents a specific search condition that was extracted from qualifier expressions by the extractQueryFn. Multiple GinScanKeyData structures can reference the same GinScanEntryData when identical search conditions are found, providing memory efficiency.

The structure maintains several types of state information: query-related data from the extract function, current position tracking in posting trees and lists, result accumulation for partial matches and full scans, and completion status. For partial-match or full-scan queries, the structure accumulates all matching TIDs (tuple identifiers) in a bitmap structure for efficient processing.

The design supports both posting list processing (for smaller result sets) and posting tree traversal (for larger result sets), with appropriate data structures and state tracking for each case.

## Parameters / Member Variables
- `queryKey`: The extracted query key datum from extractQueryFn
- `queryCategory`: Category classification for null handling
- `isPartialMatch`: True if this represents a partial match condition
- `extra_data`: Additional data pointer from extractQueryFn
- `strategy`: Strategy number for this search condition
- `searchMode`: Search mode flags controlling scan behavior
- `attnum`: Attribute number this condition applies to
- `buffer`: Current buffer containing posting tree page
- `curItem`: Current item pointer to heap tuple
- `matchBitmap`: TID bitmap for accumulating partial-match/full-scan results
- `matchIterator`: Iterator for traversing the match bitmap
- `matchResult`: Current result from bitmap iteration
- `list`: Array of item pointers for posting list processing
- `nlist`: Number of items in the posting list
- `offset`: Current offset within posting list or page
- `isFinished`: True when this entry has completed scanning
- `reduceResult`: Flag indicating whether to reduce result set
- `predictNumberResult`: Predicted number of results for optimization
- `btree`: B-tree traversal data for posting tree navigation

## Dependencies
- Functions called/Symbols referenced:
  - GinNullCategory (null category enumeration)
  - Pointer (generic pointer type)
  - StrategyNumber (strategy number type)
  - [TIDBitmap](../T/TIDBitmap.md) (tuple ID bitmap structure)
  - [TBMIterator](../T/TBMIterator.md) (bitmap iterator)
  - TBMIterateResult (bitmap iteration result)
  - [GinBtreeData](GinBtreeData.md) (B-tree navigation data)
- Called from (representative examples):
  - [ginFillScanEntry](../g/ginFillScanEntry.md) (src/backend/access/gin/ginscan.c:101)
  - Referenced by GinScanEntry typedef

## Notes and Other Information
- Defined in src/include/access/gin_private.h:336-367
- Core data structure for individual search conditions in GIN scans
- Supports both efficient posting list processing and posting tree traversal
- The matchBitmap mechanism enables efficient accumulation of results for complex queries
- State tracking allows for incremental scanning and proper resource management
- The predictNumberResult field enables query optimization based on estimated result sizes
- Buffer management ensures proper handling of posting tree pages during traversal
- Designed to be shared efficiently across multiple scan keys when identical conditions exist