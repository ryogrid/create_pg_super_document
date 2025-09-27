# InitFileAccess

## Location
[src/backend/storage/file/fd.c:900-929](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L900-L929)

## Overview
Initializes PostgreSQL's Virtual File Descriptor (VFD) cache system during backend startup, setting up the data structures needed for managing file access throughout the backend's lifetime.

## Definition
void InitFileAccess(void)

## Detailed Description
InitFileAccess initializes the Virtual File Descriptor cache, which is PostgreSQL's abstraction layer for managing file access. This function sets up the foundation of PostgreSQL's file management system by allocating and initializing the VfdCache array that tracks all opened files.

The function performs the following initialization steps:
1. Verifies that this is the first call to prevent double initialization
2. Allocates memory for the initial VfdCache array with one entry
3. Initializes the header entry (VfdCache[0]) which serves as a list header but is not used as a real file descriptor
4. Sets the initial cache size to 1

This function is called during backend startup in both normal and standalone modes, but notably NOT in the postmaster process. The VFD cache provides PostgreSQL with better control over file descriptor usage, allowing it to manage a large number of files even when the OS has limited file descriptor resources.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - malloc
  - Vfd (structure type)
  - MemSet
  - VFD_CLOSED (constant)
- Called from (representative examples):
  - [BaseInit](../B/BaseInit.md)

## Notes and Other Information
- Must be called exactly once per backend process - enforced by Assert(SizeVfdCache == 0)
- This initialization is separate from temporary file access, which is handled by InitTemporaryFileAccess()
- The VFD cache enables PostgreSQL to handle more files than the OS file descriptor limit by intelligently opening and closing files as needed
- VfdCache[0] serves as a sentinel/header entry and is never used for actual file operations
- Critical for PostgreSQL's file management infrastructure - without this initialization, file operations would fail
- The VFD system provides resource management, automatic cleanup, and efficient file descriptor reuse

## Simplified Source

```c
// Simplified version of InitFileAccess
void InitFileAccess(void) {
    // Ensure this is called only once during backend startup
    Assert(SizeVfdCache == 0);

    // Allocate memory for the VFD cache header
    VfdCache = (Vfd *) malloc(sizeof(Vfd));
    if (VfdCache == NULL)
        ereport(FATAL, (errcode(ERRCODE_OUT_OF_MEMORY),
                       errmsg("out of memory")));

    // Initialize the header entry to empty state
    MemSet((char *) &(VfdCache[0]), 0, sizeof(Vfd));
    VfdCache->fd = VFD_CLOSED;

    // Set initial cache size
    SizeVfdCache = 1;
}
```

Key simplifications made:
- Preserved the essential initialization logic
- Kept critical error handling for memory allocation failure
- Maintained the Assert for preventing double initialization
- Focused on the core steps: allocate, initialize, and set size
- Added clear comments explaining each major step