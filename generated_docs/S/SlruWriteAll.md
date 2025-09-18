# SlruWriteAll

## Location
src/backend/access/transam/slru.c: 133 - 142

## Overview
SlruWriteAll is a type alias that represents a pointer to the SlruWriteAllData structure, used for managing bulk write operations in PostgreSQL's Simple Log-based Recovery Unit (SLRU) system.

## Definition
```c
typedef struct SlruWriteAllData *SlruWriteAll;
```

## Detailed Description
SlruWriteAll serves as a convenient pointer type for passing SlruWriteAllData structures between functions during SLRU bulk write operations. This type alias improves code readability and provides a cleaner interface for functions that need to work with the file descriptor tracking structure. It is commonly used in SLRU-related functions that perform batch writing of dirty pages to optimize I/O performance by consolidating file operations.

## Parameters / Member Variables
This is a type alias (pointer type), so it doesn't have direct members, but it points to a SlruWriteAllData structure which contains:
- Indirect access to `num_files`: Number of currently open files
- Indirect access to `fd[]`: Array of file descriptors
- Indirect access to `segno[]`: Array of corresponding segment numbers

## Dependencies
- Functions called/Symbols referenced:
  - [SlruWriteAllData](SlruWriteAllData.md) (the underlying structure this type points to)
- Called from (representative examples):
  - SlruErrorCause (function that may use this type for error handling)
  - [SlruInternalWritePage](SlruInternalWritePage.md) (internal function for writing pages)
  - [SlruPhysicalWritePage](SlruPhysicalWritePage.md) (physical page write function)

## Notes and Other Information
- This type alias provides a more convenient way to pass SlruWriteAllData structures by pointer
- Used throughout the SLRU subsystem for bulk write operations
- Helps maintain clean function signatures when working with the file descriptor tracking structure
- Part of PostgreSQL's strategy to optimize I/O operations by batching writes and keeping files open during bulk operations