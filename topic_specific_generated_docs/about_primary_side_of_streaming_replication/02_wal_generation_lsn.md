# Chapter 2: WAL Generation and LSN Assignment

<- [Previous: Architecture Overview](01_architecture_overview.md) | [Index](index.md) | [Next: WAL Persistence](03_wal_persistence.md) ->

---

## Overview

This chapter details the WAL record insertion process, focusing on how Log Sequence Numbers (LSNs) are assigned to records. The LSN assignment is a critical operation that must be atomic and efficient, as it occurs on every WAL-generating operation in PostgreSQL.

**Key insight:** PostgreSQL uses a "byte position" abstraction internally to simplify atomic reservation, then converts to XLogRecPtr (LSN) format after releasing the spinlock. This minimizes spinlock hold time to only a few CPU cycles.

**Related Diagrams:**
- [Figure 2: LSN Assignment Sequence](diagrams/02_lsn_assignment_sequence.mermaid) - Detailed XLogInsertRecord flow

---

## Processing Flow

The WAL insertion flow proceeds through these steps:

```
XLogInsert()
    |
    v
XLogRecordAssemble() -----> Prepare record data, check FPW state
    |
    v
XLogInsertRecord()
    |
    +---> WALInsertLockAcquire() -----> Acquire one of 8 locks
    |
    +---> ReserveXLogInsertLocation() --> LSN ASSIGNMENT (spinlock)
    |         |
    |         +---> CurrBytePos/PrevBytePos update under insertpos_lck
    |         +---> Convert to XLogRecPtr (outside spinlock)
    |
    +---> CopyXLogRecordToWAL() -----> Copy to WAL buffer
    |
    +---> WALInsertLockRelease()
```

---

## Implementation Details

### XLogInsertRecord Function

**Location:** `src/backend/access/transam/xlog.c:750`

**Signature:**
```c
XLogRecPtr
XLogInsertRecord(XLogRecData *rdata,
                 XLogRecPtr fpw_lsn,
                 uint8 flags,
                 int num_fpi,
                 bool topxid_included)
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `rdata` | XLogRecData* | Chain of record data chunks |
| `fpw_lsn` | XLogRecPtr | LSN for full-page write decision |
| `flags` | uint8 | XLOG_MARK_UNIMPORTANT, etc. |
| `num_fpi` | int | Number of full-page images |
| `topxid_included` | bool | Whether top-level XID is in record |

**Return Value:** End position (LSN) of the inserted record, or InvalidXLogRecPtr if retry needed.

#### Step-by-Step Internal Logic

**Step 1: Record Type Classification**
```c
// xlog.c:767-774
/* Does this record type require special handling? */
if (unlikely(rechdr->xl_rmid == RM_XLOG_ID))
{
    if (info == XLOG_SWITCH)
        class = WALINSERT_SPECIAL_SWITCH;
    else if (info == XLOG_CHECKPOINT_REDO)
        class = WALINSERT_SPECIAL_CHECKPOINT;
}
```
Special records like XLOG_SWITCH require all insertion locks.

**Step 2: Enter Critical Section**
```c
// xlog.c:821
START_CRIT_SECTION();
```
No memory allocation or error throwing allowed from here.

**Step 3: Acquire Insertion Lock**
```c
// xlog.c:825
WALInsertLockAcquire();
```
Acquires one of NUM_XLOGINSERT_LOCKS (default 8) using round-robin selection. See [WALInsertLockAcquire](#walinsertlockacquire-function) for details.

**Step 4: Check FPW State**
```c
// xlog.c:843-861
if (RedoRecPtr != Insert->RedoRecPtr)
{
    Assert(RedoRecPtr < Insert->RedoRecPtr);
    RedoRecPtr = Insert->RedoRecPtr;
}
doPageWrites = (Insert->fullPageWrites || Insert->runningBackups > 0);

if (doPageWrites &&
    (!prevDoPageWrites ||
     (fpw_lsn != InvalidXLogRecPtr && fpw_lsn <= RedoRecPtr)))
{
    /* Oops, need to recompute with full-page images */
    WALInsertLockRelease();
    END_CRIT_SECTION();
    return InvalidXLogRecPtr;
}
```
If full-page write state changed since record assembly, caller must retry.

**Step 5: Reserve Space (LSN Assignment)**
```c
// xlog.c:867-868
ReserveXLogInsertLocation(rechdr->xl_tot_len, &StartPos, &EndPos,
                          &rechdr->xl_prev);
```
**This is where the LSN is actually assigned.** See [ReserveXLogInsertLocation](#reservexloginsertlocation-function) for detailed analysis.

**Step 6: Calculate Final CRC**
```c
// xlog.c:915-918
rdata_crc = rechdr->xl_crc;
COMP_CRC32C(rdata_crc, rechdr, offsetof(XLogRecord, xl_crc));
FIN_CRC32C(rdata_crc);
rechdr->xl_crc = rdata_crc;
```
CRC is computed after xl_prev is set (it was unknown before reservation).

**Step 7: Copy Record to Buffer**
```c
// xlog.c:924-926
CopyXLogRecordToWAL(rechdr->xl_tot_len,
                    class == WALINSERT_SPECIAL_SWITCH, rdata,
                    StartPos, EndPos, insertTLI);
```
See [CopyXLogRecordToWAL](#copyxlogrecordtowal-function) for buffer copy logic.

#### Lock Acquisition/Release Points

| Point | Lock | Operation |
|-------|------|-----------|
| Line 825 | WALInsertLocks[MyLockNo] | Acquire (LW_EXCLUSIVE) |
| Line 867 | insertpos_lck | Acquire/Release (spinlock in ReserveXLogInsertLocation) |
| Line 949+ | WALInsertLocks[MyLockNo] | Release |

---

### ReserveXLogInsertLocation Function

**Location:** `src/backend/access/transam/xlog.c:1109`

This is the most critical function for LSN assignment. It uses a clever "usable byte position" abstraction.

**Signature:**
```c
static pg_attribute_always_inline void
ReserveXLogInsertLocation(int size, XLogRecPtr *StartPos, XLogRecPtr *EndPos,
                          XLogRecPtr *PrevPtr)
```

#### Source Code Analysis

```c
// xlog.c:1109-1154
static pg_attribute_always_inline void
ReserveXLogInsertLocation(int size, XLogRecPtr *StartPos, XLogRecPtr *EndPos,
                          XLogRecPtr *PrevPtr)
{
    XLogCtlInsert *Insert = &XLogCtl->Insert;
    uint64      startbytepos;
    uint64      endbytepos;
    uint64      prevbytepos;

    size = MAXALIGN(size);

    /* All (non xlog-switch) records should contain data. */
    Assert(size > SizeOfXLogRecord);

    /*
     * The duration the spinlock needs to be held is minimized by minimizing
     * the calculations that have to be done while holding the lock. The
     * current tip of reserved WAL is kept in CurrBytePos, as a byte position
     * that only counts "usable" bytes in WAL, that is, it excludes all WAL
     * page headers. The mapping between "usable" byte positions and physical
     * positions (XLogRecPtrs) can be done outside the locked region.
     */
    SpinLockAcquire(&Insert->insertpos_lck);

    startbytepos = Insert->CurrBytePos;
    endbytepos = startbytepos + size;
    prevbytepos = Insert->PrevBytePos;
    Insert->CurrBytePos = endbytepos;
    Insert->PrevBytePos = startbytepos;

    SpinLockRelease(&Insert->insertpos_lck);

    /* Convert byte positions to XLogRecPtr outside the lock */
    *StartPos = XLogBytePosToRecPtr(startbytepos);
    *EndPos = XLogBytePosToEndRecPtr(endbytepos);
    *PrevPtr = XLogBytePosToRecPtr(prevbytepos);

    /* Verify conversions are consistent */
    Assert(XLogRecPtrToBytePos(*StartPos) == startbytepos);
    Assert(XLogRecPtrToBytePos(*EndPos) == endbytepos);
    Assert(XLogRecPtrToBytePos(*PrevPtr) == prevbytepos);
}
```

#### Key Design Insights

**Byte Position Abstraction:**

The key optimization is using "usable byte positions" that exclude page header overhead:

- `CurrBytePos` counts only "usable" bytes (excludes page headers)
- Simple arithmetic: `CurrBytePos += size`
- Conversion to XLogRecPtr happens **outside** spinlock
- This minimizes spinlock hold time to approximately 4 memory operations

**Spinlock Hold Time Analysis:**

The spinlock section performs only:
1. Read CurrBytePos
2. Read PrevBytePos
3. Write CurrBytePos = old + size
4. Write PrevBytePos = old CurrBytePos

This takes only a few CPU cycles, minimizing contention even under extremely high transaction rates.

**Byte Position to XLogRecPtr Conversion:**

The conversion functions (`XLogBytePosToRecPtr`, `XLogBytePosToEndRecPtr`) account for page headers:

| Function | Purpose |
|----------|---------|
| `XLogBytePosToRecPtr()` | Converts usable byte position to record start LSN |
| `XLogBytePosToEndRecPtr()` | Converts usable byte position to record end LSN |
| `XLogRecPtrToBytePos()` | Converts LSN back to usable byte position |

**Cross-reference:** See [Appendix B: Glossary](appendix_glossary.md#xlogrecptr) for XLogRecPtr format details.

---

### CopyXLogRecordToWAL Function

**Location:** `src/backend/access/transam/xlog.c` (around line 1250)

Copies the assembled record to the reserved buffer space.

#### Key Behavior

```c
// Simplified logic from CopyXLogRecordToWAL
while (rdata != NULL)
{
    /* Get buffer pointer for current position */
    currpos = GetXLogBuffer(CurrPos, tli);

    /* Handle page boundary crossing */
    if (freespace < rdata_len)
    {
        /* Update insertingAt to let others know our progress */
        WALInsertLockUpdateInsertingAt(CurrPos);

        /* Initialize next page if needed */
        currpos = GetXLogBuffer(CurrPos, tli);
    }

    memcpy(currpos, rdata_data, rdata_len);
    rdata = rdata->next;
}
```

The `insertingAt` variable in the WALInsertLock allows `WaitXLogInsertionsToFinish()` to track progress of concurrent inserters. This is essential for [XLogFlush()](03_wal_persistence.md#xlogflush-function) to know when it's safe to write.

**Cross-reference:** See [Figure 4: WAL Buffer States](diagrams/04_wal_buffer_state.mermaid) for buffer lifecycle.

---

### WALInsertLockAcquire Function

**Location:** `src/backend/access/transam/xlog.c:1372`

```c
// xlog.c:1372-1411
static void
WALInsertLockAcquire(void)
{
    bool        immed;
    static int  lockToTry = -1;

    if (lockToTry == -1)
        lockToTry = MyProcNumber % NUM_XLOGINSERT_LOCKS;
    MyLockNo = lockToTry;

    immed = LWLockAcquire(&WALInsertLocks[MyLockNo].l.lock, LW_EXCLUSIVE);
    if (!immed)
    {
        /* Try another lock next time to distribute load */
        lockToTry = (lockToTry + 1) % NUM_XLOGINSERT_LOCKS;
    }
}
```

**Lock Distribution Strategy:**

- Each backend remembers which lock it last used
- Initial lock is based on `MyProcNumber % NUM_XLOGINSERT_LOCKS`
- If lock wasn't immediately available, try a different one next time
- Results in natural load balancing across all 8 locks (default)

This strategy prevents lock convoy effects where all backends queue on the same lock.

---

## Diagrams

### Figure 2: LSN Assignment Sequence

**Location:** [diagrams/02_lsn_assignment_sequence.mermaid](diagrams/02_lsn_assignment_sequence.mermaid)

This diagram shows:
- The exact point where LSN is assigned (ReserveXLogInsertLocation)
- Spinlock acquire/release boundaries
- Byte position to XLogRecPtr conversion timing
- The relationship between WALInsertLock and insertpos_lck

---

## Configuration Parameters

| Parameter | Default | Impact |
|-----------|---------|--------|
| `wal_buffers` | -1 (auto) | Size of WAL buffer cache. Larger buffers reduce wait for buffer allocation. |
| `wal_compression` | off | Compress full-page images. Reduces WAL size but increases CPU usage. |
| `full_page_writes` | on | Write full pages after checkpoint. Disabling risks partial-write corruption. |

**Cross-reference:** See [Appendix C: Configuration Parameters](appendix_config_params.md) for complete parameter documentation.

---

## Key Takeaways

1. **Atomic LSN assignment**: LSN assignment happens atomically under the `insertpos_lck` spinlock. The spinlock hold time is minimized to only 4 memory operations.

2. **Byte position abstraction**: The "usable byte position" abstraction allows simple arithmetic during reservation, with complex page-header calculations happening outside the critical section.

3. **Multiple insertion locks**: NUM_XLOGINSERT_LOCKS (default 8) allows concurrent WAL insertion by multiple backends. Lock selection uses round-robin with adaptive load balancing.

4. **Page header transparency**: Page header overhead is handled transparently by the byte-to-LSN conversion functions, making the insertion logic simpler.

5. **Full-page write validation**: FPW state changes (from checkpoints or backups) require record re-assembly. The function returns InvalidXLogRecPtr to signal the caller to retry.

6. **Progress tracking**: The `insertingAt` variable enables progress tracking without additional locking, essential for coordinating with [XLogFlush()](03_wal_persistence.md).

7. **Continuation records**: Records spanning page boundaries are handled automatically. The `xl_prev` field links records together for recovery.

---

## Related Sections

- **Next:** [Chapter 3: WAL Persistence](03_wal_persistence.md) - How WAL is written and fsynced
- **Architecture:** [Chapter 1: XLogCtlInsert Structure](01_architecture_overview.md#xlogctlinsert)
- **Glossary:** [Appendix B: XLogRecPtr](appendix_glossary.md#xlogrecptr)

---

## Navigation

<- [Previous: Architecture Overview](01_architecture_overview.md) | [Index](index.md) | [Next: WAL Persistence](03_wal_persistence.md) ->
