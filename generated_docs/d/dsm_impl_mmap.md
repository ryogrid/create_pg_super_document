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
  - OpenTransientFile, CloseTransientFile
  - mmap, munmap, fstat, unlink, write
  - errcode_for_dynamic_shared_memory
  - pgstat_report_wait_start, pgstat_report_wait_end
  - palloc0, ereport
- Called from (representative examples):
  - dsm_impl_op

## Notes and Other Information
- Files are created with pattern: 
- Uses  flags for mmap
- Zero-filling is done in  chunks to ensure proper allocation
- Proper cleanup is performed on all error paths to avoid resource leaks
- The implementation is somewhat misleadingly called 'shared memory' since it uses regular files