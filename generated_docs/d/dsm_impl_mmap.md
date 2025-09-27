# dsm_impl_mmap

## Location
[src/backend/storage/ipc/dsm_impl.c:792-962](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/dsm_impl.c#L792-L962)

## Overview
Implements mmap-based dynamic shared memory operations by creating and managing memory-mapped files in the pg_dynshmem directory.

## Definition

```c
struct stat st;
```
## Detailed Description
The  function provides mmap-based implementation for PostgreSQL's dynamic shared memory system. It creates temporary files in the  directory and maps them into the process address space using . This approach allows shared memory segments to persist on disk but may suffer from unwanted synchronization to storage. Users can relocate the pg_dynshmem directory to a ramdisk to mitigate this issue.

The function handles four primary operations:
- **CREATE**: Creates a new file, zero-fills it to the requested size, and maps it
- **ATTACH**: Opens an existing file, determines its size, and maps it  
- **DETACH**: Unmaps the memory segment but leaves the file intact
- **DESTROY**: Unmaps the memory segment and removes the underlying file

For CREATE operations, the function performs explicit zero-filling by writing zeros to ensure all file space is properly allocated, preventing segmentation faults during later access.

## Parameters / Member Variables
- : The dynamic shared memory operation type (CREATE, ATTACH, DETACH, DESTROY)
- : Unique identifier for the shared memory segment
- : Size in bytes for CREATE operations; ignored for ATTACH (determined from file size)
- : Implementation-private data pointer (unused in mmap implementation)
- : Pointer to store the mapped memory address
- : Pointer to store the actual mapped size
- : Error level for reporting failures

## Dependencies
- Functions called/Symbols referenced:
  - [OpenTransientFile](../O/OpenTransientFile.md), CloseTransientFile
  - mmap, munmap, fstat, unlink, write
  - [errcode_for_dynamic_shared_memory](../e/errcode_for_dynamic_shared_memory.md)
  - [pgstat_report_wait_start](../p/pgstat_report_wait_start.md), pgstat_report_wait_end
  - [palloc0](../p/palloc0.md), ereport
- Called from (representative examples):
  - [dsm_impl_op](dsm_impl_op.md)

## Notes and Other Information
- Files are created with pattern: 
- Uses  flags for mmap
- Zero-filling is done in  chunks to ensure proper allocation
- Proper cleanup is performed on all error paths to avoid resource leaks
- The implementation is somewhat misleadingly called 'shared memory' since it uses regular files

## Simplified Source

```c
// Simplified version of dsm_impl_mmap
static bool dsm_impl_mmap(dsm_op op, dsm_handle handle, Size request_size,
                         void **impl_private, void **mapped_address, Size *mapped_size,
                         int elevel) {
    char name[64];
    int flags;
    int fd;
    char *address;

    // Generate filename for the shared memory segment
    snprintf(name, 64, PG_DYNSHMEM_DIR "/" PG_DYNSHMEM_MMAP_FILE_PREFIX "%u", handle);

    // Handle teardown cases (detach/destroy)
    if (op == DSM_OP_DETACH || op == DSM_OP_DESTROY) {
        if (*mapped_address != NULL && munmap(*mapped_address, *mapped_size) != 0) {
            ereport(elevel, (errcode_for_dynamic_shared_memory(),
                           errmsg("could not unmap shared memory segment \"%s\": %m", name)));
            return false;
        }
        *mapped_address = NULL;
        *mapped_size = 0;

        if (op == DSM_OP_DESTROY && unlink(name) != 0) {
            ereport(elevel, (errcode_for_dynamic_shared_memory(),
                           errmsg("could not remove shared memory segment \"%s\": %m", name)));
            return false;
        }
        return true;
    }

    // Open file for create or attach
    flags = O_RDWR | (op == DSM_OP_CREATE ? O_CREAT | O_EXCL : 0);
    if ((fd = OpenTransientFile(name, flags)) == -1) {
        if (op == DSM_OP_ATTACH || errno != EEXIST) {
            ereport(elevel, (errcode_for_dynamic_shared_memory(),
                           errmsg("could not open shared memory segment \"%s\": %m", name)));
        }
        return false;
    }

    // Determine size (attach) or allocate space (create)
    if (op == DSM_OP_ATTACH) {
        struct stat st;
        if (fstat(fd, &st) != 0) {
            CloseTransientFile(fd);
            ereport(elevel, (errcode_for_dynamic_shared_memory(),
                           errmsg("could not stat shared memory segment \"%s\": %m", name)));
            return false;
        }
        request_size = st.st_size;
    } else {
        // Zero-fill the file for CREATE operation
        char *zbuffer = (char *) palloc0(ZBUFFER_SIZE);
        Size remaining = request_size;
        bool success = true;

        while (success && remaining > 0) {
            Size goal = (remaining > ZBUFFER_SIZE) ? ZBUFFER_SIZE : remaining;
            if (write(fd, zbuffer, goal) == goal) {
                remaining -= goal;
            } else {
                success = false;
            }
        }

        if (!success) {
            CloseTransientFile(fd);
            unlink(name);
            ereport(elevel, (errcode_for_dynamic_shared_memory(),
                           errmsg("could not resize shared memory segment \"%s\" to %zu bytes: %m",
                                 name, request_size)));
            return false;
        }
    }

    // Map the file into memory
    address = mmap(NULL, request_size, PROT_READ | PROT_WRITE,
                   MAP_SHARED | MAP_HASSEMAPHORE | MAP_NOSYNC, fd, 0);
    if (address == MAP_FAILED) {
        CloseTransientFile(fd);
        if (op == DSM_OP_CREATE) {
            unlink(name);
        }
        ereport(elevel, (errcode_for_dynamic_shared_memory(),
                       errmsg("could not map shared memory segment \"%s\": %m", name)));
        return false;
    }

    *mapped_address = address;
    *mapped_size = request_size;

    if (CloseTransientFile(fd) != 0) {
        ereport(elevel, (errcode_for_file_access(),
                       errmsg("could not close shared memory segment \"%s\": %m", name)));
        return false;
    }

    return true;
}
```

Key simplifications made:
- Removed detailed comments for clarity
- Consolidated error handling paths
- Removed some intermediate variables and errno saving
- Focused on the main logic flow: teardown, file operations, and memory mapping
- Maintained essential error reporting and cleanup logic