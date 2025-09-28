# WALReadFromBuffers

## Location
[src/backend/access/transam/xlog.c:1750-1859](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L1750-L1859)

## Overview
Reads WAL data directly from in-memory WAL buffers when available, providing high-performance access to recently generated WAL data without requiring disk I/O.

## Definition
Size WALReadFromBuffers(char *dstbuf, XLogRecPtr startptr, Size count, TimeLineID tli)

## Detailed Description
WALReadFromBuffers provides a lock-free mechanism to read Write-Ahead Log (WAL) data directly from PostgreSQL's in-memory WAL buffers. This function is designed for high-performance scenarios where recently written WAL data needs to be accessed quickly, such as during WAL streaming replication.

The function implements a careful lock-free algorithm that uses atomic operations and memory barriers to ensure data consistency. It performs double verification of buffer end pointers before and after data copying to detect if a page was evicted during the read operation. If any verification fails, the function terminates early and returns only the successfully copied data.

The function will only operate on the current timeline and refuses to read during recovery, ensuring that callers receive only valid, current WAL data.

## Parameters / Member Variables
- : Destination buffer where the read WAL data will be stored
- : Starting WAL position (XLogRecPtr) from which to begin reading
- : Maximum number of bytes to read from the WAL buffers
- : Timeline ID used as a safety check to prevent reading from historical timelines

## Dependencies
- Functions called/Symbols referenced:
  - [RecoveryInProgress](../R/RecoveryInProgress.md)
  - [GetWALInsertionTimeLine](../G/GetWALInsertionTimeLine.md)
  - XLogRecPtrIsInvalid
  - [pg_atomic_read_u64](../p/pg_atomic_read_u64.md)
  - XLogRecPtrToBufIdx
  - pg_read_barrier
- Called from (representative examples):
  - [XLogSendPhysical](../X/XLogSendPhysical.md) (WAL sender process)

## Notes and Other Information
- Returns 0 immediately if recovery is in progress or if the timeline doesn't match the current WAL insertion timeline
- Uses no locks but employs atomic operations and memory barriers for thread safety
- May return fewer bytes than requested if some WAL data has been evicted from buffers
- Callers must ensure they don't read beyond LogwrtResult.Write position
- The double verification approach ensures that evicted pages are detected reliably
- Critical for WAL streaming replication performance as it avoids disk I/O for recent WAL data

## Simplified Source

```c
// Simplified version of WALReadFromBuffers
Size WALReadFromBuffers(char *dstbuf, XLogRecPtr startptr, Size count, TimeLineID tli) {
    char *pdst = dstbuf;
    XLogRecPtr recptr = startptr;
    XLogRecPtr inserted;
    Size nbytes = count;

    // Only read from current timeline, not during recovery
    if (RecoveryInProgress() || tli != GetWALInsertionTimeLine())
        return 0;

    Assert(!XLogRecPtrIsInvalid(startptr));

    // Ensure requested data is within inserted WAL range
    inserted = pg_atomic_read_u64(&XLogCtl->logInsertResult);
    if (startptr + count > inserted)
        ereport(ERROR, (errmsg("cannot read past end of generated WAL")));

    // Lock-free loop through WAL buffer pages
    while (nbytes > 0) {
        uint32 offset = recptr % XLOG_BLCKSZ;
        int idx = XLogRecPtrToBufIdx(recptr);
        XLogRecPtr expectedEndPtr = recptr + (XLOG_BLCKSZ - offset);
        XLogRecPtr endptr;
        const char *page;
        const char *psrc;
        Size npagebytes;

        // First verification: check correct page is present
        endptr = pg_atomic_read_u64(&XLogCtl->xlblocks[idx]);
        if (expectedEndPtr != endptr)
            break;  // Page evicted, stop reading

        // Calculate source and bytes to copy from this page
        page = XLogCtl->pages + idx * (Size) XLOG_BLCKSZ;
        psrc = page + offset;
        npagebytes = Min(nbytes, XLOG_BLCKSZ - offset);

        // Memory barrier before data copy
        pg_read_barrier();

        // Copy data from WAL buffer
        memcpy(pdst, psrc, npagebytes);

        // Memory barrier after data copy
        pg_read_barrier();

        // Second verification: ensure page wasn't evicted during copy
        endptr = pg_atomic_read_u64(&XLogCtl->xlblocks[idx]);
        if (expectedEndPtr != endptr)
            break;  // Page evicted during copy, stop reading

        // Update position for next iteration
        pdst += npagebytes;
        recptr += npagebytes;
        nbytes -= npagebytes;
    }

    return pdst - dstbuf;  // Return bytes successfully copied
}
```

Key simplifications made:
- Maintained lock-free double verification algorithm for data consistency
- Preserved memory barriers essential for correct ordering
- Simplified error handling while keeping essential safety checks
- Focused on core loop structure and buffer management