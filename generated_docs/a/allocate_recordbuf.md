# allocate_recordbuf

## Location
[src/backend/access/transam/xlogreader.c:190-206](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogreader.c#L190-L206)

## Overview
This function allocates or reallocates the readRecordBuf in an XLogReaderState to accommodate WAL records of at least the specified length.

## Definition
```c
static void allocate_recordbuf(XLogReaderState *state, uint32 reclength)
```

## Detailed Description
`allocate_recordbuf` manages the dynamic allocation of the record buffer used to store complete WAL records during reading operations. The function calculates an appropriate buffer size by rounding up the requested length to the next XLOG_BLCKSZ boundary and ensuring a minimum size of 5 * Max(BLCKSZ, XLOG_BLCKSZ). This sizing strategy avoids frequent small reallocations and provides sufficient space for most normal WAL records. If a buffer already exists, it is freed before allocating the new buffer.

## Parameters / Member Variables
- `state`: Pointer to XLogReaderState structure containing the buffer to be allocated
- `reclength`: Minimum required length for the record buffer in bytes

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md) (PostgreSQL memory allocation)
  - [pfree](../p/pfree.md) (PostgreSQL memory deallocation)
  - XLOG_BLCKSZ (WAL block size constant)
  - BLCKSZ (database block size constant)
  - Max (maximum value macro)
- Called from (representative examples):
  - [XLogReaderAllocate](../X/XLogReaderAllocate.md) (initial allocation with size 0)
  - [XLogDecodeNextRecord](../X/XLogDecodeNextRecord.md) (when larger buffer is needed)

## Notes and Other Information
- This is a static function, only callable within xlogreader.c
- The rounding to XLOG_BLCKSZ boundaries optimizes memory usage and alignment
- Minimum buffer size prevents frequent reallocations for typical record sizes
- The function should only be called after validating record headers to avoid allocation with invalid sizes
- Large commit or abort records may require buffers larger than the minimum size
- Always frees the existing buffer before allocating a new one to prevent memory leaks
- Updates both the buffer pointer and size field in the state structure atomically