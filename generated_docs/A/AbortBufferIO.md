# AbortBufferIO

## Location
[src/backend/storage/buffer/bufmgr.c:5626-5667](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L5626-L5667)

## Overview
AbortBufferIO cleans up active buffer I/O operations after an error occurs, setting appropriate error flags and reporting multiple failure warnings.

## Definition
```c
static void AbortBufferIO(Buffer buffer)
```

## Detailed Description
This function handles the cleanup of buffer I/O operations when errors occur, ensuring that the buffer manager maintains a consistent state even after failures. It performs different actions depending on whether the buffer contains valid data:

**For invalid buffers (read operations that failed):**
- Simply validates the buffer state and unlocks without additional processing
- Assumes the buffer is not dirty since the read never completed

**For valid buffers (write operations that failed):**
- Checks for repeated failures and issues warnings for potential permanent errors
- Generates detailed error messages including the relation path and block number
- Always sets the BM_IO_ERROR flag to mark the failure

The function delegates the actual I/O termination to TerminateBufferIO, ensuring consistent cleanup procedures across all I/O completion scenarios.

## Parameters / Member Variables
- `buffer`: The Buffer handle for which I/O is being aborted

## Dependencies
- Functions called/Symbols referenced:
  - [BufferDesc](../B/BufferDesc.md)
  - [GetBufferDescriptor](../G/GetBufferDescriptor.md)
  - [LockBufHdr](../L/LockBufHdr.md)
  - [UnlockBufHdr](../U/UnlockBufHdr.md)
  - BM_IO_IN_PROGRESS
  - BM_TAG_VALID
  - BM_VALID
  - BM_DIRTY
  - BM_IO_ERROR
  - relpathperm
  - [BufTagGetRelFileLocator](../B/BufTagGetRelFileLocator.md)
  - [BufTagGetForkNum](../B/BufTagGetForkNum.md)
  - [TerminateBufferIO](../T/TerminateBufferIO.md)
- Called from (representative examples):
  - BufferIsPinned
  - [ResOwnerReleaseBufferIO](../R/ResOwnerReleaseBufferIO.md)

## Notes and Other Information
- Assumes all LWLocks have been released but buffer pins are still held
- Always sets BM_IO_ERROR flag regardless of whether the error was I/O-related
- Does not remove buffer I/O from resource owner tracking (handled by resource owner cleanup)
- Issues WARNING-level log messages for repeated I/O failures on the same buffer
- Includes detailed error reporting with relation path and block number for debugging
- Uses buffer tag information to generate meaningful error messages
- Part of PostgreSQL's error recovery and cleanup infrastructure
- The buffer must be pinned and have I/O in progress when this function is called
- Handles both read and write I/O failure scenarios with appropriate state management

## Simplified Source
```c
static void AbortBufferIO(Buffer buffer) {
    BufferDesc *buf_hdr = GetBufferDescriptor(buffer - 1);
    uint32 buf_state;

    // Lock buffer header and validate I/O state
    buf_state = LockBufHdr(buf_hdr);

    if (!(buf_state & BM_VALID)) {
        // Handle failed read operation - buffer invalid
        UnlockBufHdr(buf_hdr, buf_state);
    } else {
        // Handle failed write operation - buffer valid but dirty
        UnlockBufHdr(buf_hdr, buf_state);

        // Report warning for repeated failures
        if (buf_state & BM_IO_ERROR) {
            char *path = relpathperm(BufTagGetRelFileLocator(&buf_hdr->tag),
                                   BufTagGetForkNum(&buf_hdr->tag));
            ereport(WARNING,
                   (errcode(ERRCODE_IO_ERROR),
                    errmsg("could not write block %u of %s",
                           buf_hdr->tag.blockNum, path),
                    errdetail("Multiple failures --- write error might be permanent.")));
            pfree(path);
        }
    }

    // Terminate I/O and set error flag
    TerminateBufferIO(buf_hdr, false, BM_IO_ERROR, false);
}
```