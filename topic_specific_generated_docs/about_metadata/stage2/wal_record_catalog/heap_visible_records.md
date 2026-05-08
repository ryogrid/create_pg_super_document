# WAL Record Catalog: Heap Visible (XLOG_HEAP2_VISIBLE)

`RM_HEAP2_ID = "Heap2"`, redo: `heap2_redo` → `heap_xlog_visible`
(`src/backend/access/heap/heapam.c`).

## XLOG_HEAP2_VISIBLE  (info 0x40)

- **Header**: `heapam_xlog.h:62`.
- **Payload**:
  ```c
  typedef struct xl_heap_visible
  {
      TransactionId cutoff_xid;
      uint8         flags;          /* VISIBILITYMAP_ALL_VISIBLE / ALL_FROZEN */
  } xl_heap_visible;
  ```

  Two registered buffers:
  - **block 0**: the heap page (for setting `PD_ALL_VISIBLE`).
  - **block 1**: the VM page (for setting the bit).

- **Emitter**: `lazy_scan_heap` (`vacuumlazy.c`) when vacuum determines a
  heap page is now all-visible / all-frozen and calls `visibilitymap_set`.

- **Redo**: `heap_xlog_visible`:
  1. If the heap buffer registration is present and the page was
     full-page-image-included, restore the heap page (which sets
     `PD_ALL_VISIBLE`).
  2. If the heap buffer is *not* full-page-image, set
     `PageHeader::pd_flags |= PD_ALL_VISIBLE` directly.
  3. If the VM buffer registration is present:
     - If full-page-image: restore the VM page.
     - Else: open the VM page, set `byte[mapByte] |= (flags << mapOffset)`,
       set page LSN.

- **Makes durable**: simultaneous "this heap page is all-visible" hint
  on both the heap-page header and the VM page.

- **Full-page image**: conditional. The VM page's FPI is included when:
  - The VM page's last LSN < the most recent checkpoint's redo pointer
    (the standard torn-page protection).
  - Or `wal_log_hints` / data checksums are enabled.
  In high-throughput workloads, the VM page receives an FPI roughly once
  per checkpoint cycle; subsequent visibilitymap_set calls in the same
  cycle log only the bit changes.

- **Standby effects**: heap page's `PD_ALL_VISIBLE` set and VM bits set.
  Index-only scans on the standby can take advantage of these bits.

## VM bit-clear is implicit

There is no `XLOG_HEAP2_VM_CLEAR` record. Bit clears happen as a side
effect of:
- `XLOG_HEAP_INSERT` / `_UPDATE` / `_DELETE` / `_LOCK`
- `XLOG_HEAP2_MULTI_INSERT`

Each of those records' redo function calls `visibilitymap_clear` for
the affected heap block. This piggyback saves one WAL record per heap
mutation.

Why is bit-clear safe to piggyback while bit-set is not? Because:
- Bit-set is a *new* assertion ("nothing un-visible on this page");
  it must be durable in its own right, and the corresponding
  `cutoff_xid` must be remembered for recovery.
- Bit-clear is a *retraction* ("might be un-visible now"); over-clearing
  is safe (just causes a heap fetch), so no special record is needed.

## XLogRecPtrIsInvalid path

A caller of `visibilitymap_set` that passes `XLogRecPtrIsInvalid(recptr) =
true` is asking the function to emit `XLOG_HEAP2_VISIBLE` itself. The
function then constructs the record, registers both buffers, and calls
XLogInsert.

A caller that passes a valid `recptr` is signaling "I just emitted the
heap-side WAL record; please align my VM page LSN to it without emitting
another WAL record". Used by the case where a heap-page operation already
established the LSN and we are catching VM up.

## Cross-references

- `component_visibility_map.md` — full VM design.
- `component_persistence_and_wal_records.md` — heap WAL records that
  implicitly clear VM bits.
