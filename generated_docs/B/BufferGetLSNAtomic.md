# BufferGetLSNAtomic

## Location
[src/backend/storage/buffer/bufmgr.c:3974-4020](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L3974-L4020)

## Overview
BufferGetLSNAtomic retrieves the Log Sequence Number (LSN) of a buffer page atomically using buffer header locking when necessary for concurrent access safety.

## Definition
```c
XLogRecPtr BufferGetLSNAtomic(Buffer buffer)
```

## Detailed Description
BufferGetLSNAtomic provides a thread-safe way to retrieve the LSN of a buffer page. The function implements an optimization where it uses a fast path for cases where locking is not required (when XLog hint bits are not needed or for local buffers). For cases where concurrent access safety is required, it acquires the buffer header lock before reading the LSN and then releases it. This ensures that the LSN read is consistent even when other processes might be modifying the page concurrently.

## Parameters / Member Variables
- `buffer`: The Buffer identifier from which to retrieve the LSN atomically

## Dependencies
- Functions called/Symbols referenced:
  - [BufferGetPage](BufferGetPage.md)
  - XLogHintBitIsNeeded
  - BufferIsLocal
  - [PageGetLSN](../P/PageGetLSN.md)
  - BufferIsPinned (assertion only)
  - [GetBufferDescriptor](../G/GetBufferDescriptor.md)
  - [LockBufHdr](../L/LockBufHdr.md)
  - [UnlockBufHdr](../U/UnlockBufHdr.md)
  - [BufferDesc](BufferDesc.md) (type)
- Called from (representative examples):
  - [gistdoinsert](../g/gistdoinsert.md)
  - [gistFindPath](../g/gistFindPath.md)
  - [SetHintBits](../S/SetHintBits.md)
  - [_bt_readpage](../b/_bt_readpage.md)
  - [XLogSaveBufferForHint](../X/XLogSaveBufferForHint.md)

## Notes and Other Information
- Uses a fast path when XLog hint bits are not needed or for local buffers, avoiding unnecessary locking overhead
- Caller must hold a buffer pin on the buffer being queried
- Essential for WAL (Write-Ahead Logging) operations where LSN consistency is critical
- Widely used in index operations (GiST, B-tree) and heap visibility operations
- The atomic nature prevents reading inconsistent LSN values during concurrent page modifications

## Simplified Source

```c
XLogRecPtr
BufferGetLSNAtomic(Buffer buffer)
{
    char *page = BufferGetPage(buffer);
    BufferDesc *buf_hdr;
    XLogRecPtr lsn;
    uint32 buf_state;

    // Fast path: no locking needed for local buffers or when hint bits disabled
    if (!XLogHintBitIsNeeded() || BufferIsLocal(buffer))
        return PageGetLSN(page);

    // Verify buffer validity and pin
    Assert(BufferIsValid(buffer));
    Assert(BufferIsPinned(buffer));

    // Get LSN atomically with buffer header lock
    buf_hdr = GetBufferDescriptor(buffer - 1);
    buf_state = LockBufHdr(buf_hdr);
    lsn = PageGetLSN(page);
    UnlockBufHdr(buf_hdr, buf_state);

    return lsn;
}
```