# GinScanKeyData

## Location
[src/include/access/gin_private.h:268-334](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/gin_private.h#L268-L334)

## Overview
GinScanKeyData represents a single GIN index qualifier expression containing search conditions, consistency functions, and match state information for efficient index scanning.

## Definition
```c
typedef struct GinScanKeyData
{
    /* Real number of entries in scanEntry[] (always > 0) */
    uint32      nentries;
    /* Number of entries that extractQueryFn and consistentFn know about */
    uint32      nuserentries;
    
    /* array of GinScanEntry pointers, one per extracted search condition */
    GinScanEntry *scanEntry;
    
    /* Required and additional entries for tuple matching */
    GinScanEntry *requiredEntries;
    int         nrequired;
    GinScanEntry *additionalEntries;
    int         nadditional;
    
    /* array of check flags, reported to consistentFn */
    GinTernaryValue *entryRes;
    bool        (*boolConsistentFn) (GinScanKey key);
    GinTernaryValue (*triConsistentFn) (GinScanKey key);
    FmgrInfo   *consistentFmgrInfo;
    FmgrInfo   *triConsistentFmgrInfo;
    Oid         collation;
    
    /* other data needed for calling consistentFn */
    Datum       query;
    /* NB: these three arrays have only nuserentries elements! */
    Datum      *queryValues;
    GinNullCategory *queryCategories;
    Pointer    *extra_data;
    StrategyNumber strategy;
    int32       searchMode;
    OffsetNumber attnum;
    
    bool        excludeOnly;
    
    /* Match status data */
    ItemPointerData curItem;
    bool        curItemMatches;
    bool        recheckCurItem;
    bool        isFinished;
} GinScanKeyData;
```

## Detailed Description
GinScanKeyData is the core structure that describes a single GIN index qualifier expression during index scans. Each qualifier expression from a query is processed to extract one or more specific index search conditions, which are represented by GinScanEntryData structures. The design allows for efficient handling of identical search conditions that may be requested by multiple qualifier expressions by merging them into unique GinScanEntry objects.

The structure maintains two important entry counts: nentries (the real number of entries in scanEntry[]) and nuserentries (the number that extractQueryFn returned and that consistentFn knows about). The "user" entries must come first in the scanEntry array.

The structure supports both required and additional entries for sophisticated matching logic. Required entries must have at least one present for a tuple to match, while additional entries are needed by the consistent function but are not sufficient alone to satisfy the qualifier.

## Parameters / Member Variables
- `nentries`: Real number of entries in scanEntry[] array (always greater than 0)
- `nuserentries`: Number of entries that extractQueryFn and consistentFn know about
- `scanEntry`: Array of GinScanEntry pointers, one per extracted search condition
- `requiredEntries`: Array of GinScanEntry pointers that must be present for tuple matching
- `nrequired`: Number of required entries
- `additionalEntries`: Array of additional GinScanEntry pointers needed by consistent function
- `nadditional`: Number of additional entries
- `entryRes`: Array of check flags reported to consistentFn
- `boolConsistentFn`: Boolean consistency function pointer
- `triConsistentFn`: Ternary consistency function pointer
- `consistentFmgrInfo`: Function manager info for consistency function
- `triConsistentFmgrInfo`: Function manager info for ternary consistency function
- `collation`: Collation OID for the scan
- `query`: Query datum for consistentFn
- `queryValues`: Array of query values (nuserentries elements)
- `queryCategories`: Array of null categories (nuserentries elements)
- `extra_data`: Array of extra data pointers (nuserentries elements)
- `strategy`: Strategy number for the scan
- `searchMode`: Search mode flags
- `attnum`: Attribute number being scanned
- `excludeOnly`: True if this is an exclude-only scan key
- `curItem`: Current item pointer being tested
- `curItemMatches`: True if current item passes consistentFn test
- `recheckCurItem`: Recheck flag for current item
- `isFinished`: True if all input entry streams are finished

## Dependencies
- Functions called/Symbols referenced:
  - [GinScanEntry](GinScanEntry.md) (scan entry pointers)
  - GinTernaryValue (ternary logic values)
  - [GinScanKey](GinScanKey.md) (self-reference for function pointers)
  - GinNullCategory (null category classification)
  - Pointer (generic pointer type)
  - StrategyNumber (strategy number type)
- Called from (representative examples):
  - [ginNewScanKey](../g/ginNewScanKey.md) (src/backend/access/gin/ginscan.c:287)
  - Various GIN scan functions that allocate and initialize scan keys

## Notes and Other Information
- Defined in src/include/access/gin_private.h:268-334
- Central data structure for GIN index scan operations
- Supports sophisticated matching logic with required/additional entry classifications
- The excludeOnly flag handles special cases where scan keys cannot enumerate all matching tuples on their own
- Match status tracking allows for efficient incremental scanning
- The design optimizes memory usage by sharing identical search conditions across multiple scan keys
- Function pointers enable pluggable consistency checking for different data types and operators