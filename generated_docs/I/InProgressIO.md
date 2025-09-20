# InProgressIO

## Location
[src/backend/storage/aio/read_stream.c:100-104](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/aio/read_stream.c#L100-L104)

## Overview
InProgressIO is a simple structure that tracks individual asynchronous read operations that have been initiated but not yet completed within PostgreSQL's read stream mechanism.

## Definition

```c
typedef struct InProgressIO
{
	int16		buffer_index;
	ReadBuffersOperation op;
} InProgressIO;
```
## Detailed Description
The InProgressIO structure serves as a lightweight tracking record for asynchronous I/O operations within the read stream system. It maintains the minimal information needed to correlate pending read operations with their target buffers and the underlying I/O operations. This structure is used internally by the ReadStream infrastructure to manage multiple concurrent read operations that may complete out of order.

The structure is designed to be compact, using only 16-bit integers for buffer indexing to minimize memory overhead when managing large numbers of concurrent operations. Each instance represents one logical read operation that has been submitted to the storage layer but whose completion has not yet been processed.

## Parameters / Member Variables
- `buffer_index`: Index into the ReadStream's circular buffer array identifying which buffer this I/O operation will populate when complete
- `op`: The underlying ReadBuffersOperation structure that contains the actual I/O operation details and status
## Dependencies
- Functions called/Symbols referenced:
  - [ReadBuffersOperation](../R/ReadBuffersOperation.md)
- Called from (representative examples):
  - [ReadStream](../R/ReadStream.md) (as member variable)
  - read_stream_begin_relation

## Notes and Other Information
- This structure is part of PostgreSQL's asynchronous I/O infrastructure introduced for improved read-ahead performance
- The compact design using int16 types limits the maximum number of concurrent operations but provides better cache efficiency
- Located in src/backend/storage/aio/read_stream.c:100-104
- Used exclusively within the read stream implementation and not exposed to external consumers