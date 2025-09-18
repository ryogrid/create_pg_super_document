# heap_page_prune_and_freeze

## Location
src/backend/access/heap/pruneheap.c: 350 - 916

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