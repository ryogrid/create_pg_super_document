# dsm_impl_windows

## Location
[src/backend/storage/ipc/dsm_impl.c:610-791](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/dsm_impl.c#L610-L791)

## Overview
Windows-specific implementation for PostgreSQL's dynamic shared memory system using Windows file mapping objects backed by the system paging file.

## Definition

```c
static bool
dsm_impl_windows(dsm_op op, dsm_handle handle, Size request_size,
				 void **impl_private, void **mapped_address,
				 Size *mapped_size, int elevel)
```
## Detailed Description
The  function implements dynamic shared memory operations using Windows file mapping APIs. It uses the system paging file as backing store rather than physical files for performance and simplicity. File mapping objects are kernel objects that are automatically cleaned up when all references are closed or processes exit.

Key implementation details:
- Creates segments using  with  to use system paging file
- Uses  for attaching to existing segments
- Maps memory using  with read/write access
- Uses  to determine actual segment size for both create and attach operations
- Handles Windows-specific error codes and maps them to errno using 
- Stores the file mapping handle in impl_private for later cleanup

## Parameters / Member Variables
- : The operation to perform (CREATE/ATTACH/DETACH/DESTROY)
- : DSM handle used to generate unique segment names
- : Size for CREATE operations, ignored for others
- : Stores the Windows HANDLE for the file mapping object
- : Pointer to current/new mapping address
- : Pointer to current/new mapping size
- : Error logging level for error messages

## Dependencies
- Functions called/Symbols referenced:
  - CreateFileMapping (create new file mapping object)
  - OpenFileMapping (open existing file mapping object)
  - MapViewOfFile (map file mapping into address space)
  - UnmapViewOfFile (unmap file mapping from address space)
  - CloseHandle (close Windows handles)
  - VirtualQuery (query virtual memory information for size)
  - [_dosmaperr](_dosmaperr.md) (map Windows error codes to errno)
  - [errcode_for_dynamic_shared_memory](../e/errcode_for_dynamic_shared_memory.md) (error code helper)
- Called from:
  - [dsm_impl_op](dsm_impl_op.md) (when dynamic_shared_memory_type is DSM_IMPL_WINDOWS)

## Notes and Other Information
- Uses SEGMENT_NAME_PREFIX for consistent naming with other Windows shared memory
- Handles 32-bit and 64-bit size splitting for CreateFileMapping API requirements
- Windows automatically destroys file mapping objects when all references are closed
- Error handling distinguishes between ERROR_ALREADY_EXISTS and ERROR_ACCESS_DENIED
- VirtualQuery returns size in page units, providing consistent size reporting
- DETACH and DESTROY operations are treated identically due to Windows automatic cleanup
- Comprehensive error handling with proper cleanup of partially completed operations
- Uses system paging file backing for optimal performance characteristics

## Simplified Source

```c
// Simplified version of dsm_impl_windows
static bool
dsm_impl_windows(dsm_op op, dsm_handle handle, Size request_size,
                 void **impl_private, void **mapped_address,
                 Size *mapped_size, int elevel)
{
    HANDLE hmap;
    char *address;
    char name[64];
    MEMORY_BASIC_INFORMATION info;

    // Generate unique segment name using handle
    snprintf(name, 64, "%s.%u", SEGMENT_NAME_PREFIX, handle);

    // Handle cleanup operations (detach/destroy)
    if (op == DSM_OP_DETACH || op == DSM_OP_DESTROY) {
        // Unmap memory view if mapped
        if (*mapped_address != NULL && !UnmapViewOfFile(*mapped_address)) {
            ereport(elevel, (errmsg("could not unmap shared memory segment")));
            return false;
        }

        // Close file mapping handle if open
        if (*impl_private != NULL && !CloseHandle(*impl_private)) {
            ereport(elevel, (errmsg("could not remove shared memory segment")));
            return false;
        }

        // Clear output parameters
        *impl_private = NULL;
        *mapped_address = NULL;
        *mapped_size = 0;
        return true;
    }

    // Create new segment or attach to existing one
    if (op == DSM_OP_CREATE) {
        // Split size for 32/64-bit compatibility
        DWORD size_high = (DWORD)(request_size >> 32);
        DWORD size_low = (DWORD)request_size;

        // Create file mapping using system paging file
        hmap = CreateFileMapping(INVALID_HANDLE_VALUE, NULL, PAGE_READWRITE,
                                size_high, size_low, name);

        // Check for conflicts with existing segments
        DWORD errcode = GetLastError();
        if (errcode == ERROR_ALREADY_EXISTS || errcode == ERROR_ACCESS_DENIED) {
            if (hmap) CloseHandle(hmap);
            return false;
        }

        if (!hmap) {
            ereport(elevel, (errmsg("could not create shared memory segment")));
            return false;
        }
    } else {
        // Attach to existing segment
        hmap = OpenFileMapping(FILE_MAP_WRITE | FILE_MAP_READ, FALSE, name);
        if (!hmap) {
            ereport(elevel, (errmsg("could not open shared memory segment")));
            return false;
        }
    }

    // Map the file mapping into process address space
    address = MapViewOfFile(hmap, FILE_MAP_WRITE | FILE_MAP_READ, 0, 0, 0);
    if (!address) {
        CloseHandle(hmap);
        ereport(elevel, (errmsg("could not map shared memory segment")));
        return false;
    }

    // Query actual size of mapped region
    if (VirtualQuery(address, &info, sizeof(info)) == 0) {
        UnmapViewOfFile(address);
        CloseHandle(hmap);
        ereport(elevel, (errmsg("could not stat shared memory segment")));
        return false;
    }

    // Set output parameters
    *mapped_address = address;
    *mapped_size = info.RegionSize;
    *impl_private = hmap;

    return true;
}
```

Key simplifications made:
- Removed detailed error code mapping and complex error handling
- Simplified Windows API error checking to basic success/failure
- Consolidated similar error cleanup patterns
- Removed detailed comments about Windows internals
- Abstracted complex bit manipulation for size parameters
- Streamlined conditional logic flow
- Maintained essential algorithm structure and all critical operations