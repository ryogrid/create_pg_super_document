# Component: Free Space Map (FSM)

[Top: ../README.md](../../README.md)

## Overview

The Free Space Map records, per heap page, an 8-bit category indicating
"approximately how many bytes of free space this page has". It is used by the
heap-extension fast path: when an INSERT needs space, FSM tells us a page
that probably has room.

The FSM lives in a separate fork: `FSM_FORKNUM`.

Crucially, the FSM is a **hint**: it is allowed to be wrong. An FSM lookup
may return a page that is actually full (or no longer exists), and the heap
extension code copes by re-checking and falling back. Because of this, FSM
updates do not need WAL in the strict sense — a bogus FSM will be corrected
by VACUUM.

For comparison: the VM and CLOG are durable; the FSM is rebuildable.

## Data model

- 1 byte per heap page (256 categories of free space).
- Categories are computed by `fsm_space_avail_to_cat(avail_bytes)`:
  category = `avail_bytes / FSM_CAT_STEP` clamped to [0, 255].
- `FSM_CATEGORIES = 256`, `MaxFSMRequestSize = MaxHeapTupleSize`.

## Three-level tree

The FSM is stored as a *tree of pages* in the FSM fork:

```
Level 2 (root)        : 1 page
Level 1 (intermediate): SlotsPerFSMPage pages         (typically ~3650)
Level 0 (leaves)      : SlotsPerFSMPage^2 pages       (~13.3 million)
                        each leaf covers 1 heap block
```

`SlotsPerFSMPage` is computed at compile time:

```c
#define NodesPerPage (BLCKSZ - SizeOfFSMPageHeader)
#define SlotsPerFSMPage (NodesPerPage / 2 + 1)
                       /* roughly (BLCKSZ - 24) / 2 / 2 because the
                          binary heap doubles internal nodes per leaf */
```

A heap of N blocks needs `N / SlotsPerFSMPage` leaves, plus a small number of
internal pages, plus the root.

The closed-form mapping from a "logical leaf number" to a "physical block
number in the FSM fork" is:

```
y = n + (n / F + 1) + (n / F^2 + 1) + ... + 1
where F = SlotsPerFSMPage
```

Implemented in `fsm_logical_to_physical(level, logicalNum)`.

## Page-internal binary heap

Each FSM page contains a binary heap of `NodesPerPage` slots:

```
fp_nodes[0]                                = max(fp_nodes[1], fp_nodes[2])
fp_nodes[1], fp_nodes[2]                   internal nodes
fp_nodes[NodesPerPage - SlotsPerFSMPage .. NodesPerPage - 1] = leaves
```

The binary-heap invariant: every parent is the max of its children. So the
root holds the maximum free-space category across all children of this page.

`fsm_search_avail(buf, minvalue)` walks from root downward, picking the left
child if it satisfies `>= minvalue` (depth-first, biased left). Returns
the slot index of the chosen leaf, or -1 if even the root < minvalue.

`fsm_set_avail(buf, slot, newvalue)` sets the leaf and walks up, recomputing
parent nodes until the parent is unchanged.

`fp_next_slot` is a separate field in the page header: a round-robin pointer
that spreads concurrent inserters across leaves of the same page (so two
backends inserting concurrently don't both get block 0). Updated by
`fsm_search_avail`.

## Search

### GetPageWithFreeSpace  (importance 0.85, Tier 1)

**Signature** (`freespace.c`):
```c
BlockNumber GetPageWithFreeSpace(Relation rel, Size spaceNeeded);
```

The top-level entry point. Called by `RelationGetBufferForTuple` (hio.c).

**Logic**:
1. Compute `min_cat = fsm_space_needed_to_cat(spaceNeeded)`.
2. `fsm_search(rel, min_cat)` — top-down tree walk.
3. Returns the heap block number, or `InvalidBlockNumber` if no page has
   that much free space.

**Performance**: O(tree depth) = O(log N / log SlotsPerFSMPage) = at most 3
FSM page reads per search for any reasonable relation size.

### fsm_search  (importance 0.70)

```c
static BlockNumber fsm_search(Relation rel, uint8 min_cat);
```

Recursive descent:

1. Start at the root page (level 2).
2. `fsm_search_avail(rootbuf, min_cat)` returns a slot index, or -1.
3. If -1: return InvalidBlockNumber.
4. Compute the child page's logical address from the slot.
5. Read the child page (level 1).
6. Recurse: `fsm_search_avail` on level 1.
7. Recurse: `fsm_search_avail` on level 0 (leaf).
8. The leaf slot maps to a heap block number via
   `fsm_get_heap_blk(level0_pageno, slot)`.

If at any level the child page is missing or the requested-cat is no longer
available (concurrent updaters race), return InvalidBlockNumber and let the
caller try `RecordAndGetPageWithFreeSpace` instead.

### fsm_search_avail

The page-internal walk. Reads `fp_nodes[0]`; if `< minvalue`, returns -1.
Else walks down, picking the left or right child based on which has
`>= minvalue` (preferring left on tie). Updates `fp_next_slot` to spread
subsequent searches.

### fp_next_slot (round-robin)

Within one FSM page, `fp_next_slot` advances on each successful search.
The next search starts the binary-heap descent biased toward
`fp_next_slot`. This avoids two inserters always landing on the same leaf.

## Update

### RecordPageWithFreeSpace  (importance 0.78)

```c
void RecordPageWithFreeSpace(Relation rel, BlockNumber heapBlk,
                             Size spaceAvail);
```

Called by heap.c after VACUUM, after a successful tuple insert, after page
prune. Updates the FSM with the new free-space estimate.

**Logic**:
1. `cat = fsm_space_avail_to_cat(spaceAvail)`.
2. Find the leaf page covering `heapBlk` via `fsm_get_location`.
3. Read the leaf page.
4. `fsm_set_avail(buf, slot, cat)` — updates the leaf and walks up the
   page-internal heap.
5. `fsm_propagate_change(rel, leafBlk)` — if the leaf's root value changed,
   update the parent leaf in the level-1 page; recurse up.

The propagation is bounded at 3 (root → mid → leaf), so cost is at most 3
FSM page reads + 3 FSM page writes per update.

**No WAL emitted**: FSM updates use `MarkBufferDirtyHint`, which may emit
`XLOG_FPI_FOR_HINT` only if checksums are on or wal_log_hints is on. Otherwise,
the FSM is "free" — no WAL traffic.

### RecordAndGetPageWithFreeSpace

Combined update + search. Used by hio.c when a candidate block was full:
"this block actually has X bytes free; please give me the next block with Y
bytes". Saves one FSM walk.

### fsm_set_and_search

Internal helper that combines `fsm_set_avail` with a walk: after setting a
leaf value, while we have the page locked, also do a fresh search starting
from this leaf's bin to find a sibling slot ≥ minvalue.

## Vacuum reconciliation

### FreeSpaceMapVacuum

```c
void FreeSpaceMapVacuum(Relation rel);
```

Called from `vacuum_rel` after the heap pass. Walks the entire FSM tree from
leaves up, recomputing parent values. This corrects any drift between the
actual heap free space and the FSM's hint.

**Logic**:
1. Read every leaf page; compute the max of its slots → root value.
2. Read every level-1 page; recompute its slots from children → root value.
3. Update the level-2 (true root) similarly.

**Performance**: O(FSM size). For a large relation with millions of FSM
leaves, this is non-trivial and is one reason VACUUM can be slow.

### FreeSpaceMapVacuumRange

```c
void FreeSpaceMapVacuumRange(Relation rel, BlockNumber start, BlockNumber end);
```

A bounded variant; used by `lazy_vacuum_heap_rel` to repair only a range of
blocks.

### fsm_vacuum_page

Internal helper that walks one FSM page's binary heap and recomputes the
node values from the leaves up.

## Categories

```c
#define FSM_CAT_STEP  (MaxFSMRequestSize / FSM_CATEGORIES)
                       /* MaxFSMRequestSize ~ MaxHeapTupleSize */
```

`fsm_space_avail_to_cat(bytes)`:
```c
return Min(bytes / FSM_CAT_STEP, FSM_CATEGORIES - 1);
```

`fsm_space_needed_to_cat(bytes)`:
```c
return (bytes + FSM_CAT_STEP - 1) / FSM_CAT_STEP;   /* round up */
```

## "FSM is just a hint" contract

The contract:

1. The FSM may report a page that is actually full. Caller must re-check
   under the heap-page lock; if no space, use `RecordAndGetPageWithFreeSpace`
   to update the FSM and try again.
2. The FSM may underreport free space: a page may have more space than the
   FSM's category suggests. Worst case: insert goes to a different page.
3. The FSM is allowed to disappear: `RBM_ZERO_ON_ERROR` is used when reading
   FSM pages, so a corrupt FSM page is silently re-zeroed and treated as
   "no free space anywhere on this leaf". VACUUM repairs.
4. The FSM does not have full-page-image protection. Hint-bit changes
   under data checksums emit `XLOG_FPI_FOR_HINT` for the FSM page (same
   path as for ordinary buffers).

### XLogRecordPageWithFreeSpace (heap-extension special case)

When heap_extend creates a new page, the FSM update for that page is recorded
via `XLogRecordPageWithFreeSpace` so a standby has the same hint. This is
the *only* WAL-touching FSM API.

### fsm_does_block_exist

```c
bool fsm_does_block_exist(Relation rel, BlockNumber blkno);
```

Quick check whether the FSM-recommended block is still within the relation
size. Used by hio.c after `GetPageWithFreeSpace` to avoid trying to read
past EOF.

## hio.c integration

### RelationGetBufferForTuple  (importance 0.70)

`hio.c`:
```c
Buffer RelationGetBufferForTuple(Relation rel, Size len,
                                 Buffer otherBuffer, int options,
                                 BulkInsertState bistate, Buffer *vmbuffer,
                                 Buffer *vmbuffer_other);
```

The "give me a buffer to insert into" entry point. Walkthrough:

1. Compute `targetBlock` from FSM:
   `GetPageWithFreeSpace(rel, len + saveFreeSpace)`.
2. If targetBlock is InvalidBlockNumber and the relation has any blocks, try
   the last block (often has space due to recent inserts).
3. Pin VM for `targetBlock` via `visibilitymap_pin` (the deadlock-avoidance
   pin-before-lock dance — see component_visibility_map.md).
4. Read and exclusive-lock the heap buffer.
5. If the page does not have room (concurrent insert filled it):
   - `RecordAndGetPageWithFreeSpace(rel, targetBlock, actualFree, len)`.
   - Goto 1 (with the new candidate).
6. If still no candidate (FSM exhausted, last block full): extend the
   relation by one block, get the new buffer.
7. Return the buffer + targetBlock.

The function manages **both** vmbuffer and vmbuffer_other so a multi-block
insert can release pins in the right order. It also calls
`visibilitymap_clear` on `targetBlock`'s VM bit (since the new tuple breaks
all-visible).

## indexfsm.c — the simpler index FSM

Indexes have a much simpler FSM: just 0/1 (full / has-free-space). Tracked
in a separate fork; APIs:

- `GetFreeIndexPage(rel)` — returns a free index page or InvalidBlockNumber.
- `RecordFreeIndexPage(rel, blkno)` — mark page free.
- `RecordUsedIndexPage(rel, blkno)` — mark page used.
- `IndexFreeSpaceMapVacuum(rel)` — recompute the index FSM from scratch.

The index FSM uses the same freespace.c machinery internally (1-byte
categories with only values 0 and 255).

## Persistence invariants (deep dive)

1. **FSM data is never trusted across a crash for correctness**. VACUUM
   re-establishes consistency.
2. **No FSM-specific WAL records**. The only FSM data that lands in WAL is
   the implicit hint emitted by `MarkBufferDirtyHint` when checksums or
   wal_log_hints are on (XLOG_FPI_FOR_HINT). Even then, the data is treated
   as a hint — replay restores the page contents but does not require them
   to match the heap.
3. **Truncation is logged**. `RelationTruncate` emits XLOG_SMGR_TRUNCATE,
   whose redo calls `FreeSpaceMapPrepareTruncateRel`. So FSM-fork length
   tracks heap-fork length.

## Cross-references

- `component_visibility_map.md` — pin-before-lock cooperation.
- `component_persistence_and_wal_records.md` — XLOG_FPI_FOR_HINT,
  XLOG_SMGR_TRUNCATE.

## Source references

- `src/backend/storage/freespace/README` — design rationale
- `src/backend/storage/freespace/freespace.c::GetPageWithFreeSpace`
- `src/backend/storage/freespace/freespace.c::fsm_search`
- `src/backend/storage/freespace/freespace.c::RecordPageWithFreeSpace`
- `src/backend/storage/freespace/freespace.c::RecordAndGetPageWithFreeSpace`
- `src/backend/storage/freespace/freespace.c::FreeSpaceMapVacuum`
- `src/backend/storage/freespace/freespace.c::FreeSpaceMapVacuumRange`
- `src/backend/storage/freespace/freespace.c::FreeSpaceMapPrepareTruncateRel`
- `src/backend/storage/freespace/freespace.c::XLogRecordPageWithFreeSpace`
- `src/backend/storage/freespace/freespace.c::fsm_does_block_exist`
- `src/backend/storage/freespace/fsmpage.c::fsm_search_avail`
- `src/backend/storage/freespace/fsmpage.c::fsm_set_avail`
- `src/backend/storage/freespace/fsmpage.c::fsm_rebuild_page`
- `src/backend/access/heap/hio.c::RelationGetBufferForTuple`
- `src/backend/storage/freespace/indexfsm.c`
