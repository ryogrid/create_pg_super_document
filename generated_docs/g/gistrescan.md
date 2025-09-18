# gistrescan

## Location
src/backend/access/gist/gistscan.c: 127 - 348

## Overview
Reinitializes or restarts a GiST index scan with potentially new scan keys and order-by conditions, managing memory contexts and preparing the search queue for traversal.

## Definition


## Detailed Description
This function handles the reinitialization of an existing GiST index scan, which can occur either as the initial scan setup (called after gistbeginscan) or when restarting a scan with different parameters. It implements sophisticated memory management using multiple contexts to optimize for the common case of single rescans while handling multiple rescans efficiently. The function processes scan keys by replacing operator functions with consistent functions, handles ORDER BY clauses by setting up distance functions, and prepares index-only scan infrastructure when needed.

The function creates a pairing heap-based priority queue for organizing search items, properly handling memory context switches to ensure all allocations are in the correct lifetime scope. It also validates scan keys for NULL handling and sets up function caching mechanisms to preserve performance across multiple scans.

## Parameters / Member Variables
- : The IndexScanDesc structure representing the ongoing index scan
- : Array of new scan key conditions (WHERE clause predicates)
- : Number of scan keys (ignored, uses scan->numberOfKeys instead)
- : Array of ORDER BY expressions for distance-based queries
- : Number of ORDER BY expressions (ignored, uses scan->numberOfOrderBys instead)

## Dependencies
- Functions called/Symbols referenced:
  - AllocSetContextCreate
  - MemoryContextReset
  - RelationGetNumberOfAttributes
  - IndexRelationGetNumberOfKeyAttributes
  - CreateTemplateTupleDesc
  - TupleDescInitEntry
  - pairingheap_allocate
  - pairingheap_GISTSearchItem_cmp
  - fmgr_info_copy
  - get_func_rettype
  - palloc
  - memmove
  - pfree
- Called from (representative examples):
  - gisthandler

## Notes and Other Information
- Implements a three-tier memory context strategy: first scan uses scanCxt, second scan creates queueCxt, subsequent scans reset queueCxt
- Supports both regular scans and index-only scans with proper tuple descriptor setup
- Handles NULL scan keys according to SK_SEARCHNULL/SK_SEARCHNOTNULL flags
- Preserves function extra data (fn_extra) across rescans for performance
- Distance functions must return float8 regardless of the original ordering operator's return type
- The nkeys and norderbys parameters are ignored in favor of the counts stored in the IndexScanDesc structure