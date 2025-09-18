# BufMappingPartitionLock

## Location
src/include/storage/buf_internals.h: 186 - 192

## Overview
BufMappingPartitionLock is an inline function that returns a pointer to the appropriate LWLock for a given hash code, enabling partitioned locking of the shared buffer mapping table.

## Definition
static inline LWLock *
BufMappingPartitionLock(uint32 hashcode)

## Detailed Description
BufMappingPartitionLock provides the mechanism to obtain the correct lightweight lock (LWLock) for a specific partition of the shared buffer mapping table. This function is central to PostgreSQL's strategy for reducing lock contention in the buffer management system by partitioning the buffer mapping table and using separate locks for each partition.

The function combines the BufTableHashPartition() result with the BUFFER_MAPPING_LWLOCK_OFFSET to calculate the exact index into the MainLWLockArray where the appropriate lock is stored. This design allows multiple processes to concurrently access different partitions of the buffer mapping table without blocking each other, significantly improving scalability in multi-processor environments.

The returned LWLock pointer is used by buffer management operations to acquire exclusive or shared locks on the appropriate partition before performing buffer lookups, insertions, or deletions. This ensures data consistency while maximizing concurrency.

## Parameters / Member Variables
- : The hash code value (typically from BufTableHashCode()) used to determine the partition and corresponding lock

## Dependencies
- Functions called/Symbols referenced:
  - BufTableHashPartition
  - MainLWLockArray (global lock array)
  - BUFFER_MAPPING_LWLOCK_OFFSET (offset constant)
  - LWLock (lock structure type)
- Called from (representative examples):
  - PrefetchSharedBuffer
  - BufferAlloc
  - InvalidateBuffer
  - InvalidateVictimBuffer
  - ExtendBufferedRelShared
  - FindAndDropRelationBuffers

## Notes and Other Information
- This is an inline function optimized for frequent buffer locking operations
- Returns a pointer to an LWLock in the MainLWLockArray at the calculated offset
- Critical for maintaining concurrency control in the partitioned buffer mapping system
- Used extensively throughout buffer management for acquiring appropriate partition locks
- The partition lock must be held when modifying or searching the buffer mapping table
- Essential component of PostgreSQL's scalable buffer management architecture
- Enables fine-grained locking that reduces contention compared to a single global lock