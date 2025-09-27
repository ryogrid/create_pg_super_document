# dsm_impl_posix

## Location
[src/backend/storage/ipc/dsm_impl.c:212-350](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/dsm_impl.c#L212-L350)

## Overview
POSIX shared memory implementation for PostgreSQL's dynamic shared memory system, using shm_open() and memory mapping for cross-process shared memory segments.

## Definition

```c
struct stat st;
```
## Detailed Description
The  function implements dynamic shared memory operations using POSIX shared memory primitives. It creates shared memory segments using  and maps them into the process address space using . The implementation uses a naming convention  for shared memory objects.

Key operations handled:
- **DSM_OP_CREATE**: Creates a new shared memory segment, sizes it, and maps it
- **DSM_OP_ATTACH**: Opens an existing segment, determines its size, and maps it  
- **DSM_OP_DETACH**: Unmaps the segment from process address space
- **DSM_OP_DESTROY**: Unmaps and removes the shared memory segment entirely

The function includes comprehensive error handling and cleanup for all failure scenarios, ensuring resources are properly released even when operations fail partway through.

## Parameters / Member Variables
- : The operation to perform (CREATE/ATTACH/DETACH/DESTROY)
- : Unique identifier used to generate the shared memory segment name
- : Size for CREATE operations, ignored for others
- : Implementation-specific private data (unused in POSIX implementation)
- : Pointer to current/new mapping address
- : Pointer to current/new mapping size  
- : Error logging level for error messages

## Dependencies
- Functions called/Symbols referenced:
  - shm_open (POSIX shared memory creation)
  - shm_unlink (POSIX shared memory removal)
  - mmap/munmap (memory mapping operations)
  - fstat (file statistics for size determination)
  - [dsm_impl_posix_resize](dsm_impl_posix_resize.md) (segment resizing)
  - [ReserveExternalFD](../R/ReserveExternalFD.md)/ReleaseExternalFD (file descriptor management)
  - [errcode_for_dynamic_shared_memory](../e/errcode_for_dynamic_shared_memory.md) (error code helper)
- Called from:
  - [dsm_impl_op](dsm_impl_op.md) (when dynamic_shared_memory_type is DSM_IMPL_POSIX)

## Notes and Other Information
- Uses file descriptor reservation to prevent EMFILE errors during segment operations
- Segment names follow pattern  in POSIX shared memory namespace
- On some platforms, POSIX shared memory may be implemented as files in filesystem
- Includes platform-specific mmap flags (MAP_HASSEMAPHORE, MAP_NOSYNC) for optimization
- For CREATE operations, uses O_CREAT | O_EXCL flags to prevent race conditions
- Comprehensive error handling with proper cleanup on all failure paths
- File descriptors are closed immediately after mapping to minimize resource usage

## Simplified Source

```c
// Simplified version of dsm_impl_posix
static bool dsm_impl_posix(dsm_op op, dsm_handle handle, Size request_size,
                          void **impl_private, void **mapped_address, Size *mapped_size,
                          int elevel) {
    char name[64];
    int flags;
    int fd;
    char *address;

    // Generate POSIX shared memory name
    snprintf(name, 64, "/PostgreSQL.%u", handle);

    // Handle teardown cases (detach/destroy)
    if (op == DSM_OP_DETACH || op == DSM_OP_DESTROY) {
        if (*mapped_address != NULL && munmap(*mapped_address, *mapped_size) != 0) {
            ereport(elevel, (errcode_for_dynamic_shared_memory(),
                           errmsg("could not unmap shared memory segment \"%s\": %m", name)));
            return false;
        }
        *mapped_address = NULL;
        *mapped_size = 0;

        if (op == DSM_OP_DESTROY && shm_unlink(name) != 0) {
            ereport(elevel, (errcode_for_dynamic_shared_memory(),
                           errmsg("could not remove shared memory segment \"%s\": %m", name)));
            return false;
        }
        return true;
    }

    // Reserve file descriptor to prevent EMFILE
    ReserveExternalFD();

    // Open shared memory segment
    flags = O_RDWR | (op == DSM_OP_CREATE ? O_CREAT | O_EXCL : 0);
    if ((fd = shm_open(name, flags, PG_FILE_MODE_OWNER)) == -1) {
        ReleaseExternalFD();
        if (op == DSM_OP_ATTACH || errno != EEXIST) {
            ereport(elevel, (errcode_for_dynamic_shared_memory(),
                           errmsg("could not open shared memory segment \"%s\": %m", name)));
        }
        return false;
    }

    // Determine size (attach) or set size (create)
    if (op == DSM_OP_ATTACH) {
        struct stat st;
        if (fstat(fd, &st) != 0) {
            close(fd);
            ReleaseExternalFD();
            ereport(elevel, (errcode_for_dynamic_shared_memory(),
                           errmsg("could not stat shared memory segment \"%s\": %m", name)));
            return false;
        }
        request_size = st.st_size;
    } else if (dsm_impl_posix_resize(fd, request_size) != 0) {
        close(fd);
        ReleaseExternalFD();
        shm_unlink(name);
        ereport(elevel, (errcode_for_dynamic_shared_memory(),
                       errmsg("could not resize shared memory segment \"%s\" to %zu bytes: %m",
                             name, request_size)));
        return false;
    }

    // Map the segment into memory
    address = mmap(NULL, request_size, PROT_READ | PROT_WRITE,
                   MAP_SHARED | MAP_HASSEMAPHORE | MAP_NOSYNC, fd, 0);
    if (address == MAP_FAILED) {
        close(fd);
        ReleaseExternalFD();
        if (op == DSM_OP_CREATE) {
            shm_unlink(name);
        }
        ereport(elevel, (errcode_for_dynamic_shared_memory(),
                       errmsg("could not map shared memory segment \"%s\": %m", name)));
        return false;
    }

    *mapped_address = address;
    *mapped_size = request_size;
    close(fd);
    ReleaseExternalFD();

    return true;
}
```

Key simplifications made:
- Removed detailed comments for clarity
- Consolidated error handling paths
- Removed some errno saving/restoration
- Focused on the main logic flow: teardown, POSIX shm operations, and memory mapping
- Maintained essential error reporting and resource cleanup