# read_whole_file

## Location
src/backend/commands/extension.c: 3517 - 3554

## Overview
Reads the entire contents of a file into memory as a single null-terminated string buffer.

## Definition


## Detailed Description
This utility function provides a complete file reading operation that loads an entire file into a single memory buffer. It performs several safety checks including file size validation against PostgreSQL's maximum allocation limits, and handles file I/O errors comprehensively. The function allocates memory using PostgreSQL's memory management system (palloc) and adds a null terminator for string convenience, making it suitable for reading text-based configuration and script files.

The implementation follows these steps:
1. Uses stat() to determine file size and verify file existence
2. Validates that file size doesn't exceed PostgreSQL's MaxAllocSize limit
3. Opens the file using PostgreSQL's AllocateFile() wrapper
4. Allocates a buffer with space for content plus null terminator
5. Reads the entire file content in one operation
6. Handles I/O errors and closes the file properly
7. Null-terminates the buffer and returns it to the caller

## Parameters / Member Variables
- : Path to the file to be read, as a null-terminated string
- : Output parameter that receives the actual number of bytes read from the file (excluding the added null terminator)

## Dependencies
- Functions called/Symbols referenced:
  - stat (system call)
  - AllocateFile
  - palloc
  - fread
  - FreeFile
  - errcode_for_file_access
  - ereport
- Constants referenced:
  - MaxAllocSize
  - PG_BINARY_R
- Called from (representative examples):
  - read_extension_script_file

## Notes and Other Information
- This is a static utility function within extension.c, used for reading extension script files
- The returned buffer is allocated using palloc() and must be freed by the caller using pfree()
- The function adds an extra null byte beyond the file length for string processing convenience
- Enforces PostgreSQL's memory allocation limits to prevent excessive memory usage
- Uses PostgreSQL's file handling wrappers (AllocateFile/FreeFile) for proper resource management
- File is opened in binary mode (PG_BINARY_R) to preserve exact content regardless of platform
- Comprehensive error handling covers file access, memory allocation, and I/O operations