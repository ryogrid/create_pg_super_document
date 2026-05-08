# Component: Visibility Map (VM)

[Top: ../README.md](../../README.md)

## Overview

The Visibility Map records, for each heap page, two bits:

| Bit | Constant                       | Meaning                                                     |
|-----|--------------------------------|-------------------------------------------------------------|
| 0   | `VISIBILITYMAP_ALL_VISIBLE = 0x01` | Every tuple on this page is visible to every snapshot. |
| 1   | `VISIBILITYMAP_ALL_FROZEN = 0x02`  | Every tuple on this page has been frozen (xmin/xmax old enough that nobody cares). |

These bits enable two huge optimizations:

- **Index-only scans**: when the index says "this row matches", the heap page
  needs to be visited only if the page's `ALL_VISIBLE` bit is unset.
- **Anti-wraparound vacuum skipping**: a page with `ALL_FROZEN` set can be
  skipped entirely by aggressive vacuum.

The VM lives in a separate fork of the heap relation: `VM_FORKNUM`.

## Page format

```
HEAPBLOCKS_PER_BYTE = 4                                              (visibilitymapdefs.h)
HEAPBLOCKS_PER_PAGE = (BLCKSZ - SizeOfPageHeaderData) * HEAPBLOCKS_PER_BYTE
                    = (8192 - 24) * 4
                    = 32672
```

So one VM page (8 KiB minus standard page header) covers 32,672 heap pages, or
roughly 256 MiB of heap. Each pair of bits resides in one byte (4 heap pages
per VM byte):

```
byte i bits 0..1: heap block 4i + 0
byte i bits 2..3: heap block 4i + 1
byte i bits 4..5: heap block 4i + 2
byte i bits 6..7: heap block 4i + 3
```

The layout is in `src/include/access/visibilitymapdefs.h`.

## API

### visibilitymap_get_status  (importance 0.78)

```c
uint8 visibilitymap_get_status(Relation rel, BlockNumber heapBlk, Buffer *vmbuf);
```

Returns the 2-bit value for the given heap block. Hot path:

1. Compute `mapBlock = HEAPBLK_TO_MAPBLOCK(heapBlk)`,
   `mapByte = HEAPBLK_TO_MAPBYTE(heapBlk)`,
   `mapOffset = HEAPBLK_TO_OFFSET(heapBlk)` (the bit-pair shift).
2. If `*vmbuf` is invalid or pinned to a different page, release & re-pin via
   `vm_readbuf(rel, mapBlock, false /* extend? */)`.
3. Read `byte = ((char *) BufferGetPage(*vmbuf))[SizeOfPageHeaderData + mapByte]`.
4. Return `(byte >> mapOffset) & VISIBILITYMAP_VALID_BITS`.

Caller is responsible for releasing `*vmbuf` when done. The pinning model
allows the caller to keep the VM page in memory across many heap-page checks
(e.g., for a sequential index-only scan).

**Performance**: One pin (cold) or zero (warm + same VM page) plus one byte
read.

### visibilitymap_set  (importance 0.82, Tier 1)

**Signature** (`visibilitymap.c`):
```c
void visibilitymap_set(Relation rel, BlockNumber heapBlk,
                       Buffer heapBuf, XLogRecPtr recptr,
                       Buffer vmBuf, TransactionId cutoff_xid,
                       uint8 flags);
```

**Logic**:

1. Pin and exclusive-lock the VM buffer (`LockBuffer(vmBuf, BUFFER_LOCK_EXCLUSIVE)`).
2. Compute `byte` and `mapOffset` as above.
3. Read the current value: `cur = (page[byte] >> mapOffset) & VALID_BITS`.
4. If `cur` already covers `flags`, release & return (no-op).
5. Else: `page[byte] |= (flags << mapOffset)`.
6. **LSN handshake**: set `PageSetLSN(page, recptr)` if `recptr` is greater
   than the current page's LSN. This is the "high-water mark" — the VM page
   LSN must not regress below the youngest tuple it claims visible.
7. `MarkBufferDirty(vmBuf)`.
8. If the heap page is also given (`heapBuf`) and `XLogRecPtrIsInvalid(recptr)`,
   we must emit `XLOG_HEAP2_VISIBLE` ourselves (the caller did not). The
   record carries `xl_heap_visible { cutoff_xid, flags }` plus a registered
   buffer for the heap page (so PD_ALL_VISIBLE can be replayed) and one for
   the VM page.
9. Release the buffer lock.

**The LSN-aware contract** (this is the most subtle part of vacuum
correctness): when vacuum decides "page P is all-visible at LSN L", the VM
bit should not be set on a page-image that has LSN < L. Otherwise a crash
followed by recovery could leave VM saying "all visible" while the heap page
has *un*visible tuples (because its LSN was further forward and the VM bit
is set). `visibilitymap_set` therefore takes `recptr` and forces VM page LSN
≥ heap page LSN.

### visibilitymap_clear  (importance 0.85, Tier 1)

```c
bool visibilitymap_clear(Relation rel, BlockNumber heapBlk,
                         Buffer vmbuf, uint8 flags);
```

Clears one or both bits. Returns true if any bit was actually changed.

**Logic**:
1. Lock vmbuf exclusive.
2. `cur_byte = page[byteno]`.
3. `mask = (flags << mapOffset)`.
4. `if (cur_byte & mask) { page[byteno] &= ~mask; MarkBufferDirty(vmbuf); changed = true; }`.
5. **No WAL is emitted here.** The clear is implicit in the heap WAL record's
   redo (heap_xlog_insert / update / delete / lock all clear the VM bit).
6. Release lock.

Why no WAL: the heap mutation that triggered the clear emits its own WAL
record. On replay, that record's redo function calls `visibilitymap_clear`
again — clearing again is idempotent. So we save one WAL record per heap
mutation.

### visibilitymap_pin  (importance 0.70)

```c
void visibilitymap_pin(Relation rel, BlockNumber heapBlk, Buffer *vmbuf);
```

Just pins the VM page covering `heapBlk`. If `*vmbuf` is invalid or the wrong
page, releases and re-pins. **Crucially**, this can extend the VM relation if
needed (`vm_extend`) — it never returns an invalid buffer.

`visibilitymap_pin_ok(blk, vmbuf)`: a quick check whether `vmbuf` is
already covering `blk`.

### visibilitymap_count

```c
BlockNumber visibilitymap_count(Relation rel, BlockNumber *all_frozen);
```

Counts all-visible and all-frozen heap pages by iterating the VM. Used by
`pg_class.relallvisible` updates and by `pgstattuple`.

### visibilitymap_prepare_truncate

```c
BlockNumber visibilitymap_prepare_truncate(Relation rel, BlockNumber nheapblocks);
```

Called from `RelationTruncate` (storage.c). Decides how many VM pages must be
truncated to match the new heap size, then returns the new VM page count
(actual smgrtruncate happens afterward).

## The pin-before-lock deadlock-avoidance protocol

The Tier 1 invariant: **always pin the VM buffer before acquiring an
exclusive lock on the heap buffer**.

Why: vacuum holds exclusive locks on heap pages while computing all-visible.
If vacuum needs to set the VM bit, it pins the VM page. If a concurrent
inserter holds the VM page pinned (say, with a hint-clear pending) and is
waiting for the heap lock, deadlock.

The protocol is:

```
inserter:
  1. visibilitymap_pin(rel, target_blk, &vmbuf)   /* pin VM first */
  2. LockBuffer(heap_buf, BUFFER_LOCK_EXCLUSIVE)   /* lock heap second */
  3. ... insert / clear VM bit if needed ...
  4. LockBuffer(heap_buf, BUFFER_LOCK_UNLOCK)
  5. ReleaseBuffer(vmbuf)
```

Vacuum follows the same order: pin VM, lock heap. Both lock-acquisition
patterns are pin → lock, never lock → pin. Since pin acquisition is
non-blocking (it just bumps a refcount), no deadlock arises.

The `GetVisibilityMapPins(rel, buf, otherbuf, blk1, blk2, vmbuf1, vmbuf2)`
helper in hio.c enforces this for two-block-candidate insert cases.

## Read paths

### Index-only scans

The executor's `IndexOnlyNext` (`src/backend/executor/nodeIndexonlyscan.c`)
calls `visibilitymap_get_status(heapRel, heapBlk, &vmbuf)`. If
`ALL_VISIBLE` is set, it skips the heap fetch entirely, using only the
index entry's data.

### VACUUM

`lazy_scan_heap` (`vacuumlazy.c`) consults the VM at the start of each
heap-page scan:

- If `ALL_VISIBLE` is set and we are not in aggressive mode: skip.
- If `ALL_FROZEN` is set: skip even in aggressive mode (no anti-wraparound
  freezing needed).

After scanning a page, vacuum calls `visibilitymap_set` if the page is
now all-visible / all-frozen.

## Storage details

### vm_readbuf

```c
static Buffer vm_readbuf(Relation rel, BlockNumber blkno, bool extend);
```

Returns a pinned buffer for the VM page `blkno`. If `extend = true` and the
VM relation is shorter than blkno+1 pages, calls `vm_extend(rel, blkno+1)`.

### vm_extend

Extends the VM relation by zeroing more pages. Each new VM page is initialized
via `PageInit` and written via `smgrextend` (no WAL — extending the VM does
not need to be replayed; the heap-side fork extension is the trigger and a
standby's vm_extend will run when it replays the heap-extension record).

## Why VM page writes do not normally need full-page images

The data in a VM page is two bits per heap block. A torn write that mixes
old and new data still produces *valid* bits — just possibly stale
ALL_VISIBLE / ALL_FROZEN claims. Vacuum will re-clear/re-set on the next
pass. Therefore, an FPI is overkill for VM under normal circumstances.

The exception: when `wal_log_hints` or data checksums are on, a torn page
could corrupt the page header (PageSetLSN, checksum). For those cases,
`MarkBufferDirtyHint` may emit `XLOG_FPI_FOR_HINT`.

`XLOG_HEAP2_VISIBLE` itself includes a full-page image of the VM page only
when the registered buffer's LSN says it needs FPI (i.e., the previous
checkpoint's redo pointer is older than the page's last write). In typical
high-throughput workloads, this means the FPI is emitted once per
checkpoint cycle.

## XLOG_HEAP2_VISIBLE  (info 0x40)

```c
typedef struct xl_heap_visible
{
    TransactionId cutoff_xid;
    uint8         flags;
} xl_heap_visible;
```

Emitted by `visibilitymap_set` (or by the caller, when the caller wants
to combine the heap-side and VM-side under one record).

Replay (`heap_xlog_visible`):
1. If the heap buffer is registered and full-page-image-included, restore
   the heap page (PD_ALL_VISIBLE bit).
2. If the VM buffer is registered:
   - If full-page-image: restore the page.
   - Else: open the VM page, set the bits at the appropriate offset, set LSN.

The heap and VM updates use the same WAL record, ensuring atomic replay.

## VM bit-clear is implicit in heap WAL

The reason: bit-clear is "less restrictive" than bit-set. If a heap mutation
WAL record arrives and the VM bit is already clear, no harm. If the VM bit
is still set, the redo function clears it — restoring the safety invariant.

So `heap_xlog_insert`, `heap_xlog_update`, `heap_xlog_delete`,
`heap_xlog_lock`, `heap_xlog_multi_insert` all call `visibilitymap_clear`
during their redo paths.

## Persistence invariants (deep dive)

1. **VM page LSN ≥ youngest-tuple LSN**. Enforced by `visibilitymap_set`
   (high-water mark protocol). A lower LSN would let a re-replay skip the
   VM page write, leaving stale ALL_VISIBLE.
2. **VM bit-set is durable**. `XLOG_HEAP2_VISIBLE` is the only record type
   that turns a VM bit on; replay always replays it.
3. **VM bit-clear is durable through the heap WAL**. The redo function for
   the heap mutation does the clear; if redo was lost, the heap mutation
   itself was lost, and the VM bit is still consistent with the heap.
4. **VM bit-set may not happen until WAL is flushed**. Vacuum's
   `lazy_scan_heap` calls `XLogFlush(recptr)` for the
   `XLOG_HEAP2_VISIBLE` record before allowing the page's PD_ALL_VISIBLE
   to be relied upon.

## Cross-references

- `component_persistence_and_wal_records.md` — XLOG_HEAP2_VISIBLE.
- `component_free_space_map.md` — pin-before-lock interaction in hio.c.
- `wal_record_catalog/heap_visible_records.md`.

## Source references

- `src/include/access/visibilitymapdefs.h` — VM bit constants
- `src/include/access/visibilitymap.h` — public API
- `src/backend/access/heap/visibilitymap.c::visibilitymap_get_status`
- `src/backend/access/heap/visibilitymap.c::visibilitymap_set`
- `src/backend/access/heap/visibilitymap.c::visibilitymap_clear`
- `src/backend/access/heap/visibilitymap.c::visibilitymap_pin`
- `src/backend/access/heap/visibilitymap.c::visibilitymap_count`
- `src/backend/access/heap/visibilitymap.c::visibilitymap_prepare_truncate`
- `src/backend/access/heap/visibilitymap.c::vm_readbuf`
- `src/backend/access/heap/visibilitymap.c::vm_extend`
- `src/backend/access/heap/heapam.c::heap_xlog_visible` (search for XLOG_HEAP2_VISIBLE)
- `src/backend/access/heap/vacuumlazy.c` — visibilitymap_set call sites
- `src/include/access/heapam_xlog.h:62` — XLOG_HEAP2_VISIBLE info byte
