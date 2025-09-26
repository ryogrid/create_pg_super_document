# PrivateRefCountEntry

## Location
[src/backend/storage/buffer/bufmgr.c:88-92](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L88-L92)

## Overview
PrivateRefCountEntry is a structure used in PostgreSQL's buffer manager to track private reference counts for individual buffers on a per-backend basis.

## Definition

```c
typedef struct PrivateRefCountEntry
{
	Buffer		buffer;
	int32		refcount;
} PrivateRefCountEntry;
```
## Detailed Description
PrivateRefCountEntry is a lightweight structure that maintains local reference counting for buffers within a single backend process. This structure is part of PostgreSQL's buffer management system that allows each backend to track how many times it has pinned specific buffers without requiring shared memory synchronization for every reference count operation.

The private reference count system works alongside the shared buffer management to optimize performance by reducing contention on shared memory structures. Each backend maintains its own array of PrivateRefCountEntry structures to track buffers it has referenced.

## Parameters / Member Variables
- : The Buffer identifier that this entry tracks references for
- : The number of times this backend has pinned the specified buffer (private reference count)

## Dependencies
- Functions called/Symbols referenced:
  - Buffer (type)
  - int32 (type)

- Called from (representative examples):
  - ReservePrivateRefCountEntry
  - NewPrivateRefCountEntry
  - GetPrivateRefCountEntry
  - GetPrivateRefCount
  - ForgetPrivateRefCountEntry
  - PinBuffer
  - PinBuffer_Locked
  - UnpinBufferNoOwner
  - InitBufferPoolAccess
  - CheckForBufferLeaks
  - IncrBufferRefCount

## Notes and Other Information
- This structure is used in arrays to maintain private reference counts for multiple buffers per backend
- The private reference counting system helps reduce contention on shared buffer structures
- When a buffer's private reference count reaches zero, the corresponding entry can be removed or reused
- This is part of PostgreSQL's buffer manager optimization strategy to improve scalability in multi-backend environments
- The structure is defined in src/backend/storage/buffer/bufmgr.c:88-92