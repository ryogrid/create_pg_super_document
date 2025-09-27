# dsm_impl_sysv

## Location
[src/backend/storage/ipc/dsm_impl.c:423-609](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/dsm_impl.c#L423-L609)

## Overview
System V shared memory implementation for PostgreSQL's dynamic shared memory system, using shmget(), shmat(), shmdt(), and shmctl() for cross-process shared memory segments.

## Definition

```c
struct shmid_ds shm;
```
## Detailed Description
The  function implements dynamic shared memory operations using System V IPC primitives. Unlike POSIX shared memory which uses names, System V shared memory uses integer keys derived from the dsm_handle. The implementation includes careful key management to handle type differences between dsm_handle and key_t, and uses impl_private to cache the shared memory identifier to avoid repeated shmget() lookups.

Key features:
- Converts dsm_handle to key_t, handling potential type size differences gracefully
- Caches shared memory identifiers in impl_private to optimize repeated operations
- Uses IPC_CREAT | IPC_EXCL for CREATE operations to prevent race conditions
- Handles the special IPC_PRIVATE key by treating it as already existing
- For ATTACH operations, uses IPC_STAT to determine segment size dynamically

## Parameters / Member Variables
- : The operation to perform (CREATE/ATTACH/DETACH/DESTROY)
- : DSM handle converted to System V key for segment identification  
- : Size for CREATE operations, ignored for others
- : Caches the shared memory identifier to avoid repeated shmget() calls
- : Pointer to current/new mapping address
- : Pointer to current/new mapping size
- : Error logging level for error messages

## Dependencies
- Functions called/Symbols referenced:
  - shmget (obtain shared memory identifier)
  - shmat (attach shared memory segment)
  - shmdt (detach shared memory segment)  
  - shmctl (control shared memory operations, used for IPC_STAT and IPC_RMID)
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md) (allocate cache for identifier)
  - [errcode_for_dynamic_shared_memory](../e/errcode_for_dynamic_shared_memory.md) (error code helper)
- Called from:
  - [dsm_impl_op](dsm_impl_op.md) (when dynamic_shared_memory_type is DSM_IMPL_SYSV)

## Notes and Other Information
- System V shared memory has typically lower default allocation limits than POSIX
- Handles potential type size mismatches between dsm_handle and key_t by consistent truncation
- Avoids negative keys for portability by negating them if handle casts to negative
- Uses IPCProtection flags for appropriate permissions on shared memory segments
- For shmget() with existing segments, must pass size as 0 to avoid EINVAL errors
- Comprehensive cleanup on failure paths including removal of newly created segments
- Identifier caching in impl_private improves performance for repeated operations on same segment

## Simplified Source

```c
// Simplified version of dsm_impl_sysv
static bool dsm_impl_sysv(dsm_op op, dsm_handle handle, Size request_size,
                         void **impl_private, void **mapped_address, Size *mapped_size,
                         int elevel) {
    key_t key;
    int ident;
    char *address;
    char name[64];
    int *ident_cache;

    // Generate name for error messages
    snprintf(name, 64, "%u", handle);

    // Convert handle to System V key
    key = (key_t) handle;
    if (key < 1) {
        key = -key;  // Ensure positive key
    }

    // Special handling for IPC_PRIVATE key
    if (key == IPC_PRIVATE) {
        if (op != DSM_OP_CREATE) {
            elog(DEBUG4, "System V shared memory key may not be IPC_PRIVATE");
        }
        errno = EEXIST;
        return false;
    }

    // Get or use cached shared memory identifier
    if (*impl_private != NULL) {
        ident_cache = *impl_private;
        ident = *ident_cache;
    } else {
        int flags = IPCProtection;
        size_t segsize = 0;

        // Allocate cache for identifier
        ident_cache = MemoryContextAlloc(TopMemoryContext, sizeof(int));

        if (op == DSM_OP_CREATE) {
            flags |= IPC_CREAT | IPC_EXCL;
            segsize = request_size;
        }

        if ((ident = shmget(key, segsize, flags)) == -1) {
            if (op == DSM_OP_ATTACH || errno != EEXIST) {
                pfree(ident_cache);
                ereport(elevel, (errcode_for_dynamic_shared_memory(),
                               errmsg("could not get shared memory segment: %m")));
            }
            return false;
        }

        *ident_cache = ident;
        *impl_private = ident_cache;
    }

    // Handle teardown cases (detach/destroy)
    if (op == DSM_OP_DETACH || op == DSM_OP_DESTROY) {
        pfree(ident_cache);
        *impl_private = NULL;

        if (*mapped_address != NULL && shmdt(*mapped_address) != 0) {
            ereport(elevel, (errcode_for_dynamic_shared_memory(),
                           errmsg("could not unmap shared memory segment \"%s\": %m", name)));
            return false;
        }
        *mapped_address = NULL;
        *mapped_size = 0;

        if (op == DSM_OP_DESTROY && shmctl(ident, IPC_RMID, NULL) < 0) {
            ereport(elevel, (errcode_for_dynamic_shared_memory(),
                           errmsg("could not remove shared memory segment \"%s\": %m", name)));
            return false;
        }
        return true;
    }

    // Determine size for attach operation
    if (op == DSM_OP_ATTACH) {
        struct shmid_ds shm;
        if (shmctl(ident, IPC_STAT, &shm) != 0) {
            ereport(elevel, (errcode_for_dynamic_shared_memory(),
                           errmsg("could not stat shared memory segment \"%s\": %m", name)));
            return false;
        }
        request_size = shm.shm_segsz;
    }

    // Attach the shared memory segment
    address = shmat(ident, NULL, PG_SHMAT_FLAGS);
    if (address == (void *) -1) {
        if (op == DSM_OP_CREATE) {
            shmctl(ident, IPC_RMID, NULL);
        }
        ereport(elevel, (errcode_for_dynamic_shared_memory(),
                       errmsg("could not map shared memory segment \"%s\": %m", name)));
        return false;
    }

    *mapped_address = address;
    *mapped_size = request_size;

    return true;
}
```

Key simplifications made:
- Removed detailed comments about key_t handling for clarity
- Consolidated error handling paths
- Removed some errno saving/restoration
- Focused on the main logic flow: key handling, identifier caching, and System V operations
- Maintained essential error reporting and resource cleanup