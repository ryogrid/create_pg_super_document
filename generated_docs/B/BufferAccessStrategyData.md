# BufferAccessStrategyData

## Location
[src/backend/storage/buffer/freelist.c:72-92](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/freelist.c#L72-L92)

## Overview
BufferAccessStrategyData is a private struct that manages a ring of shared buffers for reuse, implementing PostgreSQL's buffer access strategy mechanism to optimize buffer allocation patterns for specific workloads.

## Definition

```c
typedef struct BufferAccessStrategyData
{
	/* Overall strategy type */
	BufferAccessStrategyType btype;
	/* Number of elements in buffers[] array */
	int			nbuffers;

	/*
	 * Index of the "current" slot in the ring, ie, the one most recently
	 * returned by GetBufferFromRing.
	 */
	int			current;

	/*
	 * Array of buffer numbers.  InvalidBuffer (that is, zero) indicates we
	 * have not yet selected a buffer for this ring slot.  For allocation
	 * simplicity this is palloc'd together with the fixed fields of the
	 * struct.
	 */
	Buffer		buffers[FLEXIBLE_ARRAY_MEMBER];
}			BufferAccessStrategyData;
```
## Detailed Description
BufferAccessStrategyData implements a ring buffer strategy for PostgreSQL's shared buffer management. This structure represents a circular list of buffers that can be reused in a predictable pattern, which is particularly useful for operations that access large amounts of data sequentially (like table scans, bulk loads, or VACUUM operations).

The ring buffer approach prevents these operations from flooding the entire shared buffer pool and evicting useful cached data. Instead, they cycle through a limited set of buffers, allowing other concurrent operations to maintain their working sets in the shared buffers.

The struct uses a flexible array member to store the actual buffer identifiers, with the memory allocated in a single palloc call that includes both the fixed fields and the variable-length buffer array.

## Parameters / Member Variables
- `btype`: The type of buffer access strategy (e.g., BAS_NORMAL, BAS_BULKREAD, BAS_BULKWRITE, BAS_VACUUM)
- `nbuffers`: The total number of buffer slots in the ring array
- `current`: Index pointing to the most recently used slot in the ring buffer; used to track position for the next buffer allocation
- `buffers[FLEXIBLE_ARRAY_MEMBER]`: Flexible array containing Buffer identifiers; InvalidBuffer (0) indicates an uninitialized slot
## Dependencies
- Types referenced:
  - BufferAccessStrategyType
  - Buffer
  - FLEXIBLE_ARRAY_MEMBER
- Created by:
  - [GetAccessStrategyWithSize](../G/GetAccessStrategyWithSize.md)
- Used as:
  - [BufferAccessStrategy](BufferAccessStrategy.md) (typedef pointer to this struct)

## Notes and Other Information
- This is currently the only implementation of buffer access strategy in PostgreSQL, though the design allows for future extensions
- The structure is allocated with palloc0() to ensure all buffer slots start as InvalidBuffer (zero)
- Ring size is typically limited to 1/8th of the total shared buffers to prevent monopolizing the buffer pool
- The ring buffer mechanism is essential for maintaining cache locality during large sequential operations
- Located in src/backend/storage/buffer/freelist.c:72-92