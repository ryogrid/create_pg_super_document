# heap_page_prune_and_freeze

## Location
[src/backend/access/heap/pruneheap.c:350-916](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/pruneheap.c#L350-L916)

## Overview
heap_page_prune_and_freeze is the core function that performs heap page maintenance by pruning dead tuples, repairing fragmentation, and optionally freezing tuples to advance relation-level transaction ID horizons.

## Definition
void heap_page_prune_and_freeze(Relation relation, Buffer buffer, GlobalVisState *vistest, int options, struct VacuumCutoffs *cutoffs, PruneFreezeResult *presult, PruneReason reason, OffsetNumber *off_loc, TransactionId *new_relfrozen_xid, MultiXactId *new_relmin_mxid)

## Detailed Description
This function implements the comprehensive heap page maintenance strategy used by both opportunistic pruning and VACUUM operations. The function operates in two main phases:

**Phase 1: Analysis and Planning**
- Scans all tuples to determine their HTSV (heap tuple satisfies vacuum) status
- Builds lists of root items (HOT chain starts) and heap-only items for processing
- Determines visibility cutoffs and conflict horizons for WAL replay safety
- Plans freeze operations when the HEAP_PAGE_PRUNE_FREEZE option is set

**Phase 2: Execution**
- Processes HOT chains to prune dead tuples and redirect pointers
- Handles orphaned heap-only tuples from aborted transactions
- Applies freeze plans to advance relfrozenxid/relminmxid when required
- Updates page metadata (pd_prune_xid) and clears the page full flag
- Generates WAL records for replication and recovery

The function supports both required freezing (to advance relation horizons) and opportunistic freezing (when generating FPIs anyway). It carefully tracks visibility and frozen status to help callers update the visibility map appropriately.

## Parameters / Member Variables
- : The heap relation being processed
- : Page buffer (caller must hold pin and cleanup lock)
- : Global visibility state for determining tuple liveness
- : Bit flags controlling behavior (MARK_UNUSED_NOW, FREEZE)
- : Freeze cutoffs established by VACUUM (required if freezing)
- : Output structure for statistics and dead item offsets
- : Reason for pruning (for WAL logging and debugging)
- : Offset location for error callback context
- : Updated oldest XID on relation (if freezing)
- : Updated oldest MultiXactId on relation (if freezing)

## Dependencies
- Functions called/Symbols referenced:
  - [BufferGetPage](../B/BufferGetPage.md), BufferGetBlockNumber
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md), PageGetItemId, PageGetItem
  - [heap_prune_satisfies_vacuum](heap_prune_satisfies_vacuum.md)
  - [heap_prune_chain](heap_prune_chain.md)
  - [heap_page_prune_execute](heap_page_prune_execute.md)
  - [heap_freeze_prepared_tuples](heap_freeze_prepared_tuples.md)
  - [HeapTupleHeaderAdvanceConflictHorizon](../H/HeapTupleHeaderAdvanceConflictHorizon.md)
  - [log_heap_prune_and_freeze](../l/log_heap_prune_and_freeze.md)
  - Various ItemId and HeapTupleHeader manipulation functions
- Called from (representative examples):
  - [heap_page_prune_opt](heap_page_prune_opt.md)
  - [lazy_scan_prune](../l/lazy_scan_prune.md)

## Notes and Other Information
- Processes tuples in reverse offset order for better CPU cache performance
- Uses a two-phase approach (plan then execute) to minimize time in critical sections
- Carefully manages WAL conflict horizons for hot standby safety
- Handles complex visibility scenarios including aborted HOT updates
- Supports both required and opportunistic freezing strategies
- Updates caller-provided statistics for pgstat reporting
- Does not update FSM - caller's responsibility to manage free space map
- Critical sections protect against errors during page modifications

## Simplified Source
```c
void
heap_page_prune_and_freeze(Relation relation, Buffer buffer,
                           GlobalVisState *vistest, int options,
                           struct VacuumCutoffs *cutoffs,
                           PruneFreezeResult *presult,
                           PruneReason reason, OffsetNumber *off_loc,
                           TransactionId *new_relfrozen_xid,
                           MultiXactId *new_relmin_mxid)
{
    Page page = BufferGetPage(buffer);
    PruneState prstate;
    bool do_freeze, do_prune, do_hint;

    // Initialize pruning state and options
    prstate.vistest = vistest;
    prstate.freeze = (options & HEAP_PAGE_PRUNE_FREEZE) != 0;
    prstate.mark_unused_now = (options & HEAP_PAGE_PRUNE_MARK_UNUSED_NOW) != 0;
    prstate.cutoffs = cutoffs;

    // Initialize counters and arrays
    prstate.nredirected = prstate.ndead = prstate.nunused = prstate.nfrozen = 0;
    prstate.nroot_items = prstate.nheaponly_items = 0;

    // Phase 1: Scan all tuples and determine their visibility status
    OffsetNumber maxoff = PageGetMaxOffsetNumber(page);
    for (OffsetNumber offnum = maxoff; offnum >= FirstOffsetNumber; offnum--) {
        ItemId itemid = PageGetItemId(page, offnum);

        // Skip unused and dead items
        if (!ItemIdIsUsed(itemid) || ItemIdIsDead(itemid)) {
            // Record unchanged or mark for cleanup
            continue;
        }

        if (ItemIdIsRedirected(itemid)) {
            // Root of HOT chain
            prstate.root_items[prstate.nroot_items++] = offnum;
            continue;
        }

        // Get tuple visibility status
        HeapTupleHeader htup = (HeapTupleHeader) PageGetItem(page, itemid);
        prstate.htsv[offnum] = heap_prune_satisfies_vacuum(&prstate, tuple, buffer);

        // Classify as root or heap-only item
        if (!HeapTupleHeaderIsHeapOnly(htup))
            prstate.root_items[prstate.nroot_items++] = offnum;
        else
            prstate.heaponly_items[prstate.nheaponly_items++] = offnum;
    }

    // Phase 2: Process HOT chains
    for (int i = prstate.nroot_items - 1; i >= 0; i--) {
        OffsetNumber offnum = prstate.root_items[i];
        if (!prstate.processed[offnum]) {
            heap_prune_chain(page, blockno, maxoff, offnum, &prstate);
        }
    }

    // Phase 3: Process orphaned heap-only tuples
    for (int i = prstate.nheaponly_items - 1; i >= 0; i--) {
        OffsetNumber offnum = prstate.heaponly_items[i];
        if (!prstate.processed[offnum] &&
            prstate.htsv[offnum] == HEAPTUPLE_DEAD) {
            // Remove dead heap-only tuples not in chains
            heap_prune_record_unused(&prstate, offnum, true);
        }
    }

    // Determine what operations to perform
    do_prune = (prstate.nredirected > 0 || prstate.ndead > 0 || prstate.nunused > 0);
    do_freeze = prstate.freeze && (prstate.pagefrz.freeze_required ||
                                  should_freeze_opportunistically(&prstate));
    do_hint = (page_prune_xid_changed(&prstate) || PageIsFull(page));

    // Phase 4: Apply changes within critical section
    START_CRIT_SECTION();

    if (do_hint) {
        // Update page metadata
        ((PageHeader) page)->pd_prune_xid = prstate.new_prune_xid;
        PageClearFull(page);
    }

    if (do_prune) {
        // Execute planned pruning operations
        heap_page_prune_execute(buffer, false, prstate.redirected, prstate.nredirected,
                               prstate.nowdead, prstate.ndead,
                               prstate.nowunused, prstate.nunused);
    }

    if (do_freeze) {
        // Apply freeze plans
        heap_freeze_prepared_tuples(buffer, prstate.frozen, prstate.nfrozen);
    }

    if (do_prune || do_freeze) {
        MarkBufferDirty(buffer);

        // Generate WAL record for replication
        if (RelationNeedsWAL(relation)) {
            log_heap_prune_and_freeze(relation, buffer, conflict_xid,
                                     true, reason, /* freeze/prune details */);
        }
    }

    END_CRIT_SECTION();

    // Copy results back to caller
    presult->ndeleted = prstate.ndeleted;
    presult->nfrozen = prstate.nfrozen;
    presult->all_visible = determine_all_visible(&prstate);
    presult->all_frozen = determine_all_frozen(&prstate);

    // Update relation-level XIDs if freezing occurred
    if (prstate.freeze && presult->nfrozen > 0) {
        *new_relfrozen_xid = prstate.pagefrz.FreezePageRelfrozenXid;
        *new_relmin_mxid = prstate.pagefrz.FreezePageRelminMxid;
    }
}
```