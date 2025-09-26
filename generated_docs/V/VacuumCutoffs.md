# VacuumCutoffs

## Location
src/include/commands/vacuum.h: 246 - 284

## Overview
VacuumCutoffs is an immutable structure that holds the transaction ID and multixact ID cutoff values established at the beginning of each VACUUM operation.

## Definition


## Detailed Description
VacuumCutoffs encapsulates the critical transaction ID and multixact ID thresholds that govern VACUUM's behavior throughout the entire operation. These cutoff values are computed once at the start of VACUUM and remain constant during the operation to ensure consistency.

The structure serves multiple purposes:
1. **Visibility Determination**: OldestXmin and OldestMxact determine which tuples are visible to any running transaction
2. **Freezing Decisions**: FreezeLimit and MultiXactCutoff determine when XIDs/MXIDs must be frozen or removed
3. **Statistics Updates**: OldestXmin and OldestMxact provide the most recent values that can be stored in pg_class
4. **Snapshot Consistency**: Immutable values ensure consistent decisions across all pages processed

The cutoffs are carefully calculated based on current system state, active transactions, and configuration parameters to balance vacuum effectiveness with transaction visibility requirements.

## Parameters / Member Variables

### Existing pg_class Fields:
- : The relation's frozen transaction ID from pg_class at VACUUM start
- : The relation's minimum multixact ID from pg_class at VACUUM start

### Visibility Cutoffs:
- : Transaction ID below which tuples deleted by committed transactions are considered DEAD (not RECENTLY_DEAD)
- : Multixact ID below which multixacts are not visible to any running transaction

### Freezing Cutoffs:
- : Transaction ID below which all XIDs are frozen or removed during page processing
- : Multixact ID below which all multixact IDs are removed from Xmax during page processing

## Dependencies
- Functions called/Symbols referenced:
  - TransactionId (transaction identifier type)
  - MultiXactId (multixact identifier type)

- Called from (representative examples):
  - vacuum_get_cutoffs (src/backend/commands/vacuum.c:1084)
  - heap_prepare_freeze_tuple (src/backend/access/heap/heapam.c:7010)
  - heap_freeze_tuple (src/backend/access/heap/heapam.c:7388)
  - heap_tuple_should_freeze (src/backend/access/heap/heapam.c:7843)
  - heap_page_prune_and_freeze (src/backend/access/heap/pruneheap.c:353)
  - FreezeMultiXactId (src/backend/access/heap/heapam.c:6660)

## Notes and Other Information
- Structure is immutable once established to ensure consistent VACUUM behavior
- OldestXmin and OldestMxact are also the maximum values that can be stored in pg_class after VACUUM
- Cutoff calculations consider running transactions, autovacuum settings, and wraparound prevention
- FreezeLimit is typically much older than OldestXmin to avoid unnecessary freezing
- MultiXactCutoff works similarly to FreezeLimit but for multixact IDs
- Critical for preventing transaction ID wraparound while maintaining correct MVCC visibility
- Used throughout the vacuum infrastructure including heap access methods and page pruning
- Values are computed by vacuum_get_cutoffs() at the start of each VACUUM operation
- Essential for coordinating freezing decisions across all components of the vacuum process