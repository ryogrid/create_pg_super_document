# df_files

## Location
src/backend/utils/fmgr/dfmgr.c: 55 - 64

## Overview
The df_files struct is a node in a linked list that tracks dynamically loaded library files in PostgreSQL's dynamic function manager, storing file metadata and handles to prevent duplicate loading.

## Definition


## Detailed Description
The df_files struct (typedef'd as DynamicFileList) serves as the fundamental data structure for PostgreSQL's dynamic library management system. It maintains a linked list of all dynamically loaded shared libraries, ensuring that each library is loaded only once and providing efficient access to previously loaded libraries. The structure stores both file system metadata (device and inode) and runtime information (dlopen handle) to uniquely identify and manage loaded libraries across the PostgreSQL process lifetime.

This structure is critical for PostgreSQL's extension system, allowing the database to load C functions from shared libraries while maintaining proper resource management and avoiding duplicate loads of the same library file.

## Parameters / Member Variables
- : Pointer to the next DynamicFileList node in the linked list, forming a singly-linked list of all loaded libraries
- : Device identifier (st_dev) from stat() call, used in conjunction with inode for unique file identification
- : Inode number from stat() call for unique file identification (not used on Windows platforms)
- : Void pointer containing the dlopen() handle returned by the dynamic linker, used for subsequent dlsym() calls
- : Flexible array member containing the full pathname of the loaded library file

## Dependencies
- Functions called/Symbols referenced:
  - FLEXIBLE_ARRAY_MEMBER (macro for variable-length arrays)
- Used by (representative examples):
  - internal_load_library (src/backend/utils/fmgr/dfmgr.c:184)
  - EstimateLibraryStateSpace (src/backend/utils/fmgr/dfmgr.c:637)
  - SerializeLibraryState (src/backend/utils/fmgr/dfmgr.c:654)

## Notes and Other Information
- The structure is managed through global variables file_list and file_tail that maintain the head and tail pointers of the linked list
- On Windows platforms, the inode field is excluded since Windows stat() returns meaningless inode values
- The SAME_INODE macro uses both device and inode fields to detect when different paths refer to the same physical file (e.g., through symlinks)
- Memory for each DynamicFileList node is allocated using malloc() rather than PostgreSQL's memory contexts, as these structures must persist for the entire process lifetime
- The filename field uses FLEXIBLE_ARRAY_MEMBER to allow variable-length storage without separate memory allocation
- No mechanism currently exists to unload dynamically loaded libraries, making this a permanent registry of loaded extensions