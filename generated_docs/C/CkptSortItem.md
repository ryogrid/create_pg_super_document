# CkptSortItem

## Location
[src/include/storage/buf_internals.h:376-383](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/buf_internals.h#L376-L383)

## Overview
The  structure is used to sort and organize buffers by file during checkpoint operations in PostgreSQL for optimal I/O ordering.

## Definition

```c
typedef struct CkptSortItem
{
	Oid			tsId;
	RelFileNumber relNumber;
	ForkNumber	forkNum;
	BlockNumber blockNum;
	int			buf_id;
} CkptSortItem;
```
## Detailed Description
The  structure is specifically designed to optimize checkpoint operations by providing a way to sort buffers according to their physical file location. During checkpoints, PostgreSQL needs to write out dirty buffers to disk, and doing so in file order rather than random order can significantly improve I/O performance.

The structure contains all the necessary information to identify both the logical location of a buffer (tablespace, relation, fork, block) and its physical buffer ID. This allows the checkpoint process to sort buffers by their file locations and then write them out in an order that minimizes disk seek times.

Since this structure is allocated per buffer in shared memory, it is kept as small as possible to minimize memory overhead while providing the essential information needed for efficient checkpoint ordering.

## Parameters / Member Variables
- `tsId`: The OID of the tablespace containing the relation
- `relNumber`: The file number of the relation within the database
- `forkNum`: The fork number indicating which fork of the relation (main, FSM, VM, etc.)
- `blockNum`: The block number within the fork
- `buf_id`: The buffer ID that corresponds to this sort item for mapping back to the actual buffer
## Dependencies
- Functions called/Symbols referenced:
  - Oid (type for tablespace ID)
  - [RelFileNumber](../R/RelFileNumber.md) (type for relation identification)
  - [ForkNumber](../F/ForkNumber.md) (type for fork identification)
  - BlockNumber (type for block identification)
- Called from (representative examples):
  - [BufferSync](../B/BufferSync.md) (for checkpoint buffer sorting)
  - [ckpt_buforder_comparator](../c/ckpt_buforder_comparator.md) (for comparing and sorting items)
  - [InitBufferPool](../I/InitBufferPool.md) (for initialization of sort structures)
  - [BufferShmemSize](../B/BufferShmemSize.md) (for shared memory size calculations)

## Notes and Other Information
- Allocated per buffer in shared memory during system initialization
- Structure size is kept minimal to reduce memory footprint
- Used primarily during checkpoint operations to optimize I/O patterns
- Global array CkptBufferIds contains instances of this structure for all buffers
- Enables sequential I/O patterns during checkpoints, improving performance
- Critical for efficient checkpoint processing in high-volume systems
- The sorting helps reduce random disk access patterns during checkpoint writes