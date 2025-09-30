# AllocateVfd

## Location
[src/backend/storage/file/fd.c:1411-1468](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L1411-L1468)

## Overview
Allocates a virtual file descriptor (VFD) from the free list, expanding the VFD cache array if necessary.

## Definition

```c
static File
AllocateVfd(void)
```
## Detailed Description
AllocateVfd manages the allocation of virtual file descriptor slots from PostgreSQL's VFD cache system. The function maintains a free list of available VFD slots and returns the next available slot. When the free list is empty, it dynamically expands the VfdCache array by doubling its size.

The allocation process works as follows:
1. Checks if there are free VFD slots available (VfdCache[0].nextFree != 0)
2. If no free slots exist, it expands the cache:
   - Doubles the current cache size (with a minimum of 32 entries)
   - Reallocates memory for the expanded VfdCache array
   - Initializes new entries with zero values and VFD_CLOSED status
   - Links all new entries into the free list
3. Takes the first available slot from the free list
4. Updates the free list head pointer
5. Returns the allocated File descriptor index

This function is crucial for PostgreSQL's virtual file descriptor system, which allows the database to manage more logical file descriptors than available kernel descriptors.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - Index (typedef for array indexing)
  - File (typedef for file descriptor indices)
  - DO_DB (debug macro for conditional logging)
  - elog (error/log reporting function)
  - SizeVfdCache (global variable tracking VFD cache size)
  - Assert (debugging assertion)
  - VfdCache (global virtual file descriptor cache array)
  - Size (typedef for size values)
  - Vfd (virtual file descriptor structure type)
  - realloc (standard C memory reallocation function)
  - ereport/ERROR/errcode/errmsg (PostgreSQL error reporting system)
  - MemSet (PostgreSQL memory initialization macro)
  - VFD_CLOSED (constant indicating closed file descriptor)

- Called from (representative examples):
  - AllocateDesc (when allocating new file descriptors)
  - [PathNameOpenFilePerm](../P/PathNameOpenFilePerm.md) (when opening files with specific permissions)

## Notes and Other Information
- Returns a File index that can be used to access the allocated VFD slot
- This is a static function internal to the file descriptor management module
- The function uses a doubling strategy for cache expansion to amortize reallocation costs
- Memory allocation failure results in an ERROR (which terminates the current transaction)
- New VFD entries are initialized to a clean state with VFD_CLOSED status
- The free list is maintained as a singly-linked list using the nextFree field
- Critical for PostgreSQL's ability to handle large numbers of file operations efficiently
- Debug logging is conditional on DO_DB macro compilation

## Simplified Source

```c
static File
AllocateVfd(void)
{
    // Check if we need to expand the VFD cache
    if (VfdCache[0].nextFree == 0) {
        // Calculate new cache size (double current, minimum 32)
        Size newCacheSize = SizeVfdCache * 2;
        if (newCacheSize < 32) {
            newCacheSize = 32;
        }

        // Reallocate VFD cache array
        Vfd *newVfdCache = (Vfd *) realloc(VfdCache, sizeof(Vfd) * newCacheSize);
        if (newVfdCache == NULL) {
            ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
        }
        VfdCache = newVfdCache;

        // Initialize new entries and build free list
        for (Index i = SizeVfdCache; i < newCacheSize; i++) {
            MemSet((char *) &(VfdCache[i]), 0, sizeof(Vfd));
            VfdCache[i].nextFree = i + 1;
            VfdCache[i].fd = VFD_CLOSED;
        }
        VfdCache[newCacheSize - 1].nextFree = 0;  // Terminate list
        VfdCache[0].nextFree = SizeVfdCache;      // Link to new entries

        SizeVfdCache = newCacheSize;
    }

    // Allocate next available VFD from free list
    File file = VfdCache[0].nextFree;
    VfdCache[0].nextFree = VfdCache[file].nextFree;

    return file;
}
```