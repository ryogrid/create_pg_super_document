# SharedFileSetInit

## Location
[src/backend/storage/file/sharedfileset.c:38-55](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/sharedfileset.c#L38-L55)

## Overview
Initializes a shared file set that can be accessed by multiple backend processes, typically used for temporary files that need to be shared across parallel operations.

## Definition

```c
void
SharedFileSetInit(SharedFileSet *fileset, dsm_segment *seg)
```
## Detailed Description
SharedFileSetInit creates and initializes a SharedFileSet structure that enables multiple PostgreSQL backend processes to share temporary files. The function sets up the necessary synchronization primitives and registers a cleanup callback to ensure proper resource management. The shared file set is associated with a dynamic shared memory (DSM) segment, and when the last backend detaches from this segment, all contained files are automatically deleted.

The function performs three main operations:
1. Initializes shared fileset-specific members including a spinlock for synchronization and sets the initial reference count to 1
2. Initializes the underlying FileSet structure that manages the actual file operations
3. Registers a cleanup callback (SharedFileSetOnDetach) with the DSM segment to handle cleanup when backends detach

## Parameters / Member Variables
- : Pointer to the SharedFileSet structure to be initialized
- : Pointer to the DSM segment that this shared file set will be associated with; can be NULL if no DSM cleanup is needed

## Dependencies
- Functions called/Symbols referenced:
  - SpinLockInit
  - [FileSetInit](../F/FileSetInit.md)
  - [on_dsm_detach](../o/on_dsm_detach.md)
  - [SharedFileSetOnDetach](SharedFileSetOnDetach.md)
  - [PointerGetDatum](../P/PointerGetDatum.md)
- Called from (representative examples):
  - [ExecHashJoinInitializeDSM](../E/ExecHashJoinInitializeDSM.md) (Hash join parallel execution setup)
  - [tuplesort_initialize_shared](../t/tuplesort_initialize_shared.md) (Shared tuplesort initialization)

## Notes and Other Information
- The function is designed for parallel query execution where multiple worker processes need to share temporary files
- Reference counting is used to track how many processes are using the shared file set
- If seg is NULL, no automatic cleanup callback is registered, requiring manual cleanup
- The underlying file system implementation creates directories that are automatically cleaned up when no longer needed
- This is part of PostgreSQL's parallel query infrastructure for sharing temporary storage across worker processes

## Simplified Source

```c
void SharedFileSetInit(SharedFileSet *fileset, dsm_segment *seg) {
    // Initialize shared fileset synchronization and reference counting
    SpinLockInit(&fileset->mutex);
    fileset->refcnt = 1;

    // Initialize the underlying fileset
    FileSetInit(&fileset->fs);

    // Register cleanup callback for automatic file deletion when DSM detaches
    if (seg)
        on_dsm_detach(seg, SharedFileSetOnDetach, PointerGetDatum(fileset));
}
```