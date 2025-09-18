# GinScanOpaqueData

## Location
src/include/access/gin_private.h: 369 - 384

## Overview
GinScanOpaqueData serves as the top-level scan state structure for GIN index operations, coordinating multiple scan keys and entries with memory management and query state tracking.

## Definition
```c
typedef struct GinScanOpaqueData
{
    MemoryContext tempCtx;
    GinState    ginstate;
    
    GinScanKey  keys;           /* one per scan qualifier expr */
    uint32      nkeys;
    
    GinScanEntry *entries;      /* one per index search condition */
    uint32      totalentries;
    uint32      allocentries;   /* allocated length of entries[] */
    
    MemoryContext keyCtx;       /* used to hold key and entry data */
    
    bool        isVoidRes;      /* true if query is unsatisfiable */
} GinScanOpaqueData;
```

## Detailed Description
GinScanOpaqueData is the master control structure for GIN index scan operations, maintaining all the state information needed to coordinate complex index scans across multiple qualifier expressions and search conditions. This structure serves as the "opaque" scan state that is stored in IndexScanDesc for GIN access method operations.

The structure organizes scan state into two main levels: scan keys (representing individual qualifier expressions from the query) and scan entries (representing specific index search conditions extracted from those expressions). This two-level organization allows for efficient handling of queries where multiple qualifier expressions may generate identical search conditions, enabling sharing and optimization.

Memory management is handled through separate contexts for temporary operations and persistent key/entry data, ensuring proper cleanup and efficient memory usage during long-running scans. The structure also tracks whether the query is unsatisfiable (isVoidRes), allowing for early termination of impossible queries.

## Parameters / Member Variables
- `tempCtx`: Memory context for temporary allocations during scan operations
- `ginstate`: GIN index state information and configuration
- `keys`: Array of GinScanKey structures, one per scan qualifier expression
- `nkeys`: Number of scan keys in the keys array
- `entries`: Array of GinScanEntry pointers, one per unique index search condition
- `totalentries`: Total number of active entries in the entries array
- `allocentries`: Allocated size of the entries array (may be larger than totalentries)
- `keyCtx`: Memory context used to hold key and entry data structures
- `isVoidRes`: Boolean flag indicating if the query is unsatisfiable

## Dependencies
- Functions called/Symbols referenced:
  - GinState (index state structure)
  - GinScanKey (scan key pointer type)
  - GinScanEntry (scan entry pointer type)
- Called from (representative examples):
  - ginbeginscan (src/backend/access/gin/ginscan.c:36)
  - Referenced by GinScanOpaque typedef

## Notes and Other Information
- Defined in src/include/access/gin_private.h:369-384
- Top-level coordination structure for GIN index scan operations
- Maintains clear separation between qualifier expressions (keys) and search conditions (entries)
- Supports dynamic allocation and reallocation of entry arrays for flexible query handling
- Memory context management ensures proper cleanup and prevents memory leaks during complex scans
- The isVoidRes optimization allows for early detection and handling of impossible queries
- Used as the opaque state in PostgreSQL's IndexScanDesc structure for GIN access method
- Enables efficient sharing of identical search conditions across multiple qualifier expressions
- Coordinates with the GIN state structure to access index configuration and operational parameters