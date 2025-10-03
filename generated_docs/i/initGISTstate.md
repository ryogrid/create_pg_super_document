# initGISTstate

## Location
[src/backend/access/gist/gist.c:1532-1659](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gist.c#L1532-L1659)

## Overview
Initializes and populates a GISTSTATE structure with cached access method information and support function details for efficient GiST index operations.

## Definition

```c
unionFn[i]),
					   index_getprocinfo(index, i + 1, GIST_UNION_PROC),
					   scanCxt);
```
## Detailed Description
initGISTstate creates and initializes a comprehensive GISTSTATE structure that serves as a performance optimization cache for GiST index operations. The function establishes a dedicated memory context for the GISTSTATE and systematically populates it with cached function manager information for all index access method support procedures.

The initialization process involves creating tuple descriptors for both leaf and non-leaf pages, with the non-leaf descriptor excluding INCLUDE attributes since they don't participate in tree navigation. For each indexed key attribute, the function retrieves and caches function manager information for all GiST support procedures including consistent, union, penalty, picksplit, and equal functions. Optional procedures like compress, decompress, distance, and fetch are cached only when provided by the operator class.

The function also handles collation information appropriately, using index-specific collations when available or defaulting to standard collation for support functions that require collation context. INCLUDE attributes are handled separately with invalidated function OIDs since they don't require operator class support.

## Parameters / Member Variables
- : Relation pointer representing the GiST index for which the GISTSTATE is being initialized

## Dependencies
- Functions called/Symbols referenced:
  - AllocSetContextCreate (memory context creation for GISTSTATE lifecycle)
  - [CreateTupleDescCopyConstr](../C/CreateTupleDescCopyConstr.md) (tuple descriptor creation for non-leaf pages)
  - IndexRelationGetNumberOfKeyAttributes (key attribute count determination)
  - [index_getprocinfo](index_getprocinfo.md) (cached function manager info retrieval)
  - [index_getprocid](index_getprocid.md) (support procedure OID lookup)
  - [fmgr_info_copy](../f/fmgr_info_copy.md) (function manager information copying)
  - INDEX_MAX_KEYS (maximum key attribute validation)
  - Various GIST_*_PROC constants (support procedure type identification)
- Called from (representative examples):
  - [gistinsert](../g/gistinsert.md) (tuple insertion operations)
  - [gistbuild](../g/gistbuild.md) (index construction)
  - [gistbeginscan](../g/gistbeginscan.md) (scan initialization)

## Notes and Other Information
- Creates dedicated memory context for GISTSTATE lifecycle management
- Validates index attribute count against INDEX_MAX_KEYS to prevent array overflow
- Distinguishes between required and optional support procedures for operator classes
- Handles INCLUDE attributes separately since they don't require operator class support
- Provides collation context for support functions that require collation information
- Critical for GiST performance by avoiding repeated function lookups during operations
- The returned GISTSTATE serves as a performance cache for the lifetime of index operations
- Caller responsible for setting appropriate tempCxt if different from scanCxt is needed

## Simplified Source

```c
GISTSTATE *
initGISTstate(Relation index)
{
    GISTSTATE *giststate;
    MemoryContext scanCxt;
    MemoryContext oldCxt;

    // Validate index attribute count to prevent array overflow
    if (index->rd_att->natts > INDEX_MAX_KEYS)
        elog(ERROR, "numberOfAttributes %d > %d",
             index->rd_att->natts, INDEX_MAX_KEYS);

    // Create dedicated memory context for GISTSTATE
    scanCxt = AllocSetContextCreate(CurrentMemoryContext, "GiST scan context",
                                   ALLOCSET_DEFAULT_SIZES);
    oldCxt = MemoryContextSwitchTo(scanCxt);

    // Allocate and initialize basic GISTSTATE structure
    giststate = (GISTSTATE *) palloc(sizeof(GISTSTATE));
    giststate->scanCxt = scanCxt;
    giststate->tempCxt = scanCxt;
    giststate->leafTupdesc = index->rd_att;

    // Create truncated tuple descriptor for non-leaf pages (excludes INCLUDE attributes)
    giststate->nonLeafTupdesc = CreateTupleDescCopyConstr(index->rd_att);
    giststate->nonLeafTupdesc->natts = IndexRelationGetNumberOfKeyAttributes(index);

    // Cache support function information for each key attribute
    for (int i = 0; i < IndexRelationGetNumberOfKeyAttributes(index); i++)
    {
        // Cache required support functions
        fmgr_info_copy(&(giststate->consistentFn[i]),
                      index_getprocinfo(index, i + 1, GIST_CONSISTENT_PROC), scanCxt);
        fmgr_info_copy(&(giststate->unionFn[i]),
                      index_getprocinfo(index, i + 1, GIST_UNION_PROC), scanCxt);
        fmgr_info_copy(&(giststate->penaltyFn[i]),
                      index_getprocinfo(index, i + 1, GIST_PENALTY_PROC), scanCxt);
        fmgr_info_copy(&(giststate->picksplitFn[i]),
                      index_getprocinfo(index, i + 1, GIST_PICKSPLIT_PROC), scanCxt);
        fmgr_info_copy(&(giststate->equalFn[i]),
                      index_getprocinfo(index, i + 1, GIST_EQUAL_PROC), scanCxt);

        // Cache optional support functions (compress, decompress, distance, fetch)
        if (OidIsValid(index_getprocid(index, i + 1, GIST_COMPRESS_PROC)))
            fmgr_info_copy(&(giststate->compressFn[i]),
                          index_getprocinfo(index, i + 1, GIST_COMPRESS_PROC), scanCxt);
        else
            giststate->compressFn[i].fn_oid = InvalidOid;

        // (Similar handling for decompress, distance, fetch functions...)

        // Set collation for support functions
        if (OidIsValid(index->rd_indcollation[i]))
            giststate->supportCollation[i] = index->rd_indcollation[i];
        else
            giststate->supportCollation[i] = DEFAULT_COLLATION_OID;
    }

    // Invalidate function OIDs for INCLUDE attributes (no opclass support needed)
    for (int i = IndexRelationGetNumberOfKeyAttributes(index); i < index->rd_att->natts; i++)
    {
        giststate->consistentFn[i].fn_oid = InvalidOid;
        giststate->unionFn[i].fn_oid = InvalidOid;
        // (Similar invalidation for all other function arrays...)
        giststate->supportCollation[i] = InvalidOid;
    }

    MemoryContextSwitchTo(oldCxt);
    return giststate;
}
```