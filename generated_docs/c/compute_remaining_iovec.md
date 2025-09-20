# compute_remaining_iovec

## Location
[src/common/file_utils.c:592-636](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/file_utils.c#L592-L636)

## Overview
A utility function that adjusts iovec arrays after partial vectored I/O operations to handle remaining data transfers.

## Definition

```c
int
compute_remaining_iovec(struct iovec *destination,
						const struct iovec *source,
						int iovcnt,
						size_t transferred)
```
## Detailed Description
 is designed to handle the common scenario in vectored I/O where read/write operations may transfer only part of the requested data. When a  or  system call completes with fewer bytes transferred than requested, this function calculates what remains to be transferred by adjusting the iovec array. It skips over wholly transferred iovecs, adjusts the first partially transferred iovec by advancing its base pointer and reducing its length, and copies the remaining iovecs to the destination array. The function supports in-place adjustment when source and destination arrays are the same, making it efficient for retry scenarios in I/O operations.

## Parameters / Member Variables
- : Output iovec array that will contain the adjusted vectors for remaining transfers
- : Input iovec array representing the original I/O request
- : Number of iovec structures in the source array
- : Number of bytes that were successfully transferred in the previous I/O operation

## Dependencies
- Functions called/Symbols referenced:
  - [iovec](../i/iovec.md) (system struct)
  - memmove
  - Assert (PostgreSQL assertion macro)
- Called from (representative examples):
  - [mdreadv](../m/mdreadv.md)
  - [mdwritev](../m/mdwritev.md)
  - pg_pwritev_with_retry

## Notes and Other Information
This function is crucial for implementing robust vectored I/O in PostgreSQL, particularly in the storage manager (smgr) subsystem where large multi-page reads and writes need to handle partial transfers gracefully. The function returns the number of remaining iovecs after adjustment, with a return value of 0 indicating that all data has been transferred. It includes assertions to detect kernel behavior that violates expected I/O semantics (such as transferring more data than requested). The function enables PostgreSQL to retry I/O operations reliably when dealing with signals, resource constraints, or other conditions that might cause partial transfers.