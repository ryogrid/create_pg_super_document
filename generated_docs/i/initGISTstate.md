# initGISTstate

## Location
src/backend/access/gist/gist.c: 1532 - 1659

## Overview
Initializes and populates a GISTSTATE structure with cached access method information and support function details for efficient GiST index operations.

## Definition


## Detailed Description
initGISTstate creates and initializes a comprehensive GISTSTATE structure that serves as a performance optimization cache for GiST index operations. The function establishes a dedicated memory context for the GISTSTATE and systematically populates it with cached function manager information for all index access method support procedures.

The initialization process involves creating tuple descriptors for both leaf and non-leaf pages, with the non-leaf descriptor excluding INCLUDE attributes since they don't participate in tree navigation. For each indexed key attribute, the function retrieves and caches function manager information for all GiST support procedures including consistent, union, penalty, picksplit, and equal functions. Optional procedures like compress, decompress, distance, and fetch are cached only when provided by the operator class.

The function also handles collation information appropriately, using index-specific collations when available or defaulting to standard collation for support functions that require collation context. INCLUDE attributes are handled separately with invalidated function OIDs since they don't require operator class support.

## Parameters / Member Variables
- : Relation pointer representing the GiST index for which the GISTSTATE is being initialized

## Dependencies
- Functions called/Symbols referenced:
  - AllocSetContextCreate (memory context creation for GISTSTATE lifecycle)
  - CreateTupleDescCopyConstr (tuple descriptor creation for non-leaf pages)
  - IndexRelationGetNumberOfKeyAttributes (key attribute count determination)
  - index_getprocinfo (cached function manager info retrieval)
  - index_getprocid (support procedure OID lookup)
  - fmgr_info_copy (function manager information copying)
  - INDEX_MAX_KEYS (maximum key attribute validation)
  - Various GIST_*_PROC constants (support procedure type identification)
- Called from (representative examples):
  - gistinsert (tuple insertion operations)
  - gistbuild (index construction)
  - gistbeginscan (scan initialization)

## Notes and Other Information
- Creates dedicated memory context for GISTSTATE lifecycle management
- Validates index attribute count against INDEX_MAX_KEYS to prevent array overflow
- Distinguishes between required and optional support procedures for operator classes
- Handles INCLUDE attributes separately since they don't require operator class support
- Provides collation context for support functions that require collation information
- Critical for GiST performance by avoiding repeated function lookups during operations
- The returned GISTSTATE serves as a performance cache for the lifetime of index operations
- Caller responsible for setting appropriate tempCxt if different from scanCxt is needed