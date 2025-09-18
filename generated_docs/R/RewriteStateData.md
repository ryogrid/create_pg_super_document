# RewriteStateData

## Location
src/backend/access/heap/rewriteheap.c: 130 - 153

## Overview
RewriteStateData is a struct that encapsulates the complete state associated with a heap rewrite operation in PostgreSQL, providing an opaque interface to manage the transformation of data from a source heap to a destination heap.

## Definition
```c
typedef struct RewriteStateData
{
    Relation        rs_old_rel;         /* source heap */
    Relation        rs_new_rel;         /* destination heap */
    BulkWriteState *rs_bulkstate;       /* writer for the destination */
    BulkWriteBuffer rs_buffer;          /* page currently being built */
    BlockNumber     rs_blockno;         /* block where page will go */
    bool            rs_logical_rewrite; /* do we need to do logical rewriting */
    TransactionId   rs_oldest_xmin;     /* oldest xmin used by caller to determine tuple visibility */
    TransactionId   rs_freeze_xid;      /* Xid that will be used as freeze cutoff point */
    TransactionId   rs_logical_xmin;    /* Xid that will be used as cutoff point for logical rewrites */
    MultiXactId     rs_cutoff_multi;    /* MultiXactId that will be used as cutoff point for multixacts */
    MemoryContext   rs_cxt;             /* for hash tables and entries and tuples in them */
    XLogRecPtr      rs_begin_lsn;       /* XLogInsertLsn when starting the rewrite */
    HTAB           *rs_unresolved_tups; /* unmatched A tuples */
    HTAB           *rs_old_new_tid_map; /* unmatched B tuples */
    HTAB           *rs_logical_mappings;/* logical remapping files */
    uint32          rs_num_rewrite_mappings; /* # in memory mappings */
} RewriteStateData;
```

## Detailed Description
RewriteStateData serves as the central control structure for PostgreSQL heap rewrite operations, which occur during operations like ALTER TABLE that require rebuilding table storage. The struct maintains all necessary state information including source and destination relations, transaction visibility cutoffs, bulk write management, and mapping tables for tuple transformation. It supports both physical and logical rewriting scenarios, with the logical rewrite functionality being particularly important for maintaining consistency during operations that need to preserve logical relationships between tuples across the rewrite process.

## Parameters / Member Variables
- `rs_old_rel`: Source heap relation being rewritten
- `rs_new_rel`: Destination heap relation where rewritten data is stored  
- `rs_bulkstate`: Bulk write state manager for efficient destination writing
- `rs_buffer`: Current page buffer being constructed during the rewrite
- `rs_blockno`: Block number where the current page will be written
- `rs_logical_rewrite`: Boolean flag indicating whether logical rewriting is required
- `rs_oldest_xmin`: Oldest transaction ID used by caller to determine tuple visibility
- `rs_freeze_xid`: Transaction ID cutoff point for tuple freezing operations
- `rs_logical_xmin`: Transaction ID cutoff point specifically for logical rewrites
- `rs_cutoff_multi`: MultiXactId cutoff point for handling multixact visibility
- `rs_cxt`: Memory context for managing hash tables, entries, and tuples
- `rs_begin_lsn`: WAL log sequence number recorded at rewrite operation start
- `rs_unresolved_tups`: Hash table tracking unmatched A tuples during rewrite
- `rs_old_new_tid_map`: Hash table mapping old to new tuple identifiers for unmatched B tuples
- `rs_logical_mappings`: Hash table managing logical remapping file information
- `rs_num_rewrite_mappings`: Count of in-memory mapping structures

## Dependencies
- Functions called/Symbols referenced:
  - [BulkWriteState](../B/BulkWriteState.md)
  - BulkWriteBuffer
  - MultiXactId
  - [HTAB](../H/HTAB.md)
- Called from (representative examples):
  - [begin_heap_rewrite](../b/begin_heap_rewrite.md)
  - [RewriteState](RewriteState.md) (typedef alias)

## Notes and Other Information
This structure is intentionally opaque to users of the rewrite facility, encapsulating complex state management details. The dual hash table system (rs_unresolved_tups and rs_old_new_tid_map) handles the challenging problem of mapping tuples between old and new heap structures when rewrites occur in multiple passes. The logical rewrite functionality is crucial for maintaining referential integrity and supporting features like logical replication during major table restructuring operations.