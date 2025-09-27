# ShmemInitStruct

## Location
[src/backend/storage/ipc/shmem.c:387-492](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/shmem.c#L387-L492)

## Overview
ShmemInitStruct creates or attaches to a named data structure in shared memory, serving as the primary interface for initializing persistent shared memory objects that can be accessed across PostgreSQL processes.

## Definition

```c
structPtr;
```
## Detailed Description
This function is the central mechanism for creating and accessing named shared memory structures in PostgreSQL. It performs a dual role: if the named structure doesn't exist, it allocates space and creates it; if it already exists, it returns a pointer to the existing structure. This enables multiple PostgreSQL processes to share persistent data structures.

The function operates in several phases:
1. **Bootstrap handling**: Special case for initializing ShmemIndex itself, resolving the circular dependency
2. **Index lookup**: Searches the ShmemIndex hash table for the named structure
3. **Validation**: If found, verifies the size matches the expected size
4. **Allocation**: If not found, allocates new shared memory space and registers it
5. **Cleanup**: Handles error cases by removing incomplete entries

The function includes sophisticated error handling and maintains consistency through proper locking with ShmemIndexLock. It ensures that all allocated structures are properly aligned (CACHELINEALIGN) and validates that returned pointers fall within the shared memory segment.

## Parameters / Member Variables
- : String identifier for the shared memory structure (e.g., "LockMgrData", "BufferHeaders")
- : Number of bytes to allocate for the structure
- : Output parameter set to true if structure already existed, false if newly created

## Dependencies
- Functions called/Symbols referenced:
  - [LWLockAcquire](../L/LWLockAcquire.md)/LWLockRelease (for ShmemIndexLock synchronization)
  - [hash_search](../h/hash_search.md) (for ShmemIndex operations)
  - [ShmemAlloc](ShmemAlloc.md)/ShmemAllocRaw (for memory allocation)
  - [ShmemAddrIsValid](ShmemAddrIsValid.md) (for validation)
  - CACHELINEALIGN (for alignment verification)
  - strcmp (for string comparison)
- Called from (representative examples):
  - [InitBufferPool](../I/InitBufferPool.md) (buffer management)
  - [CreateSharedProcArray](../C/CreateSharedProcArray.md) (process array)
  - [StatsShmemInit](StatsShmemInit.md) (statistics collection)
  - Many other subsystem initialization functions

## Notes and Other Information
- Prior to PostgreSQL 9.0, this function could return NULL; now it always throws an error on failure
- The function handles the bootstrap problem where ShmemIndex must be created before it can be used to track other structures
- Size mismatches between expected and actual structure sizes result in errors to detect configuration problems
- All returned pointers are guaranteed to be cache-line aligned for performance
- The function is thread-safe through proper use of ShmemIndexLock
- Callers should not attempt to free memory returned by this function
- The function validates that returned addresses fall within the shared memory segment boundaries

## Simplified Source

```c
// Simplified version of ShmemInitStruct
void *ShmemInitStruct(const char *name, Size size, bool *foundPtr) {
    ShmemIndexEnt *result;
    void *structPtr;

    LWLockAcquire(ShmemIndexLock, LW_EXCLUSIVE);

    // Special case: initializing ShmemIndex itself
    if (!ShmemIndex) {
        PGShmemHeader *shmemseghdr = ShmemSegHdr;
        Assert(strcmp(name, "ShmemIndex") == 0);

        if (IsUnderPostmaster) {
            // Backend attaching to existing index
            Assert(shmemseghdr->index != NULL);
            structPtr = shmemseghdr->index;
            *foundPtr = true;
        } else {
            // Postmaster creating the index
            Assert(shmemseghdr->index == NULL);
            structPtr = ShmemAlloc(size);
            shmemseghdr->index = structPtr;
            *foundPtr = false;
        }
        LWLockRelease(ShmemIndexLock);
        return structPtr;
    }

    // Look up structure in shared memory index
    result = (ShmemIndexEnt *) hash_search(ShmemIndex, name, HASH_ENTER_NULL, foundPtr);

    if (!result) {
        LWLockRelease(ShmemIndexLock);
        ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
                       errmsg("could not create ShmemIndex entry for data structure \"%s\"", name)));
    }

    if (*foundPtr) {
        // Structure already exists - verify size matches
        if (result->size != size) {
            LWLockRelease(ShmemIndexLock);
            ereport(ERROR, (errmsg("ShmemIndex entry size is wrong for data structure"
                                  " \"%s\": expected %zu, actual %zu",
                                  name, size, result->size)));
        }
        structPtr = result->location;
    } else {
        // Allocate new structure
        Size allocated_size;
        structPtr = ShmemAllocRaw(size, &allocated_size);

        if (structPtr == NULL) {
            // Cleanup failed entry and report error
            hash_search(ShmemIndex, name, HASH_REMOVE, NULL);
            LWLockRelease(ShmemIndexLock);
            ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
                           errmsg("not enough shared memory for data structure"
                                 " \"%s\" (%zu bytes requested)", name, size)));
        }

        result->size = size;
        result->allocated_size = allocated_size;
        result->location = structPtr;
    }

    LWLockRelease(ShmemIndexLock);

    Assert(ShmemAddrIsValid(structPtr));
    Assert(structPtr == (void *) CACHELINEALIGN(structPtr));

    return structPtr;
}
```

Key simplifications made:
- Removed detailed comments for clarity
- Consolidated error handling paths
- Focused on the main logic flow: bootstrap handling, index lookup, validation, and allocation
- Maintained essential error reporting and validation logic
- Preserved the critical locking and assertion checks