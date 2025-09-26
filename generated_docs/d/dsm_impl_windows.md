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
  - _dosmaperr (map Windows error codes to errno)
  - errcode_for_dynamic_shared_memory (error code helper)
- Called from:
  - dsm_impl_op (when dynamic_shared_memory_type is DSM_IMPL_WINDOWS)

## Notes and Other Information
- Uses SEGMENT_NAME_PREFIX for consistent naming with other Windows shared memory
- Handles 32-bit and 64-bit size splitting for CreateFileMapping API requirements
- Windows automatically destroys file mapping objects when all references are closed
- Error handling distinguishes between ERROR_ALREADY_EXISTS and ERROR_ACCESS_DENIED
- VirtualQuery returns size in page units, providing consistent size reporting
- DETACH and DESTROY operations are treated identically due to Windows automatic cleanup
- Comprehensive error handling with proper cleanup of partially completed operations
- Uses system paging file backing for optimal performance characteristics