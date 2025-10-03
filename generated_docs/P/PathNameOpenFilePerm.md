# PathNameOpenFilePerm

## Location
[src/backend/storage/file/fd.c:1585-1656](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L1585-L1656)

## Overview
PathNameOpenFilePerm opens a file in an arbitrary directory with explicit file permission control, serving as the core file opening function in PostgreSQL's virtual file descriptor (VFD) system.

## Definition

```c
File
PathNameOpenFilePerm(const char *fileName, int fileFlags, mode_t fileMode)
```
## Detailed Description
PathNameOpenFilePerm is the primary function for opening files within PostgreSQL's virtual file descriptor management system. It creates a VFD entry for the specified file, manages kernel file descriptor limits through the LRU cache system, and ensures proper cleanup on failure. The function automatically adds O_CLOEXEC to prevent file descriptor inheritance by child processes, and handles memory management for the filename copy. If the pathname is relative, it's interpreted relative to the process working directory (typically $PGDATA). The function integrates with PostgreSQL's resource management by tracking the VFD in the cache and managing kernel FD limits.

## Parameters / Member Variables
- `*fileName`: Path to the file to be opened (relative paths interpreted from $PGDATA)
- `fileFlags`: File access flags for opening (read, write, create, etc.)
- `fileMode`: File permissions to use when creating new files
## Dependencies
- Functions called/Symbols referenced:
  - [AllocateVfd](../A/AllocateVfd.md)
  - [ReleaseLruFiles](../R/ReleaseLruFiles.md)  
  - [BasicOpenFilePerm](../B/BasicOpenFilePerm.md)
  - [FreeVfd](../F/FreeVfd.md)
  - [Insert](../I/Insert.md)
  - strdup
  - ereport
- Called from (representative examples):
  - [PathNameOpenFile](PathNameOpenFile.md)

## Notes and Other Information
This function is central to PostgreSQL's file management architecture, located in src/backend/storage/file/fd.c. It implements a virtual file descriptor system that allows PostgreSQL to manage more files than the kernel limit permits by caching and reusing kernel FDs. The function ensures proper error handling and cleanup, including freeing allocated memory on failure. All descriptors are implicitly marked O_CLOEXEC for security. The saved flags are adjusted (removing O_CREAT, O_TRUNC, O_EXCL) to allow safe re-opening of the file later.

## Simplified Source

```c
File
PathNameOpenFilePerm(const char *fileName, int fileFlags, mode_t fileMode)
{
    char *fnamecopy;
    File file;
    Vfd *vfdP;

    // Step 1: Create a copy of the filename
    fnamecopy = strdup(fileName);
    if (fnamecopy == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg("out of memory")));

    // Step 2: Allocate a VFD entry
    file = AllocateVfd();
    vfdP = &VfdCache[file];

    // Step 3: Close excess kernel FDs to stay within limits
    ReleaseLruFiles();

    // Step 4: Add O_CLOEXEC for security (prevent inheritance by child processes)
    fileFlags |= O_CLOEXEC;

    // Step 5: Open the actual file
    vfdP->fd = BasicOpenFilePerm(fileName, fileFlags, fileMode);

    // Step 6: Handle open failure
    if (vfdP->fd < 0) {
        int save_errno = errno;
        FreeVfd(file);
        free(fnamecopy);
        errno = save_errno;
        return -1;
    }

    // Step 7: Initialize VFD entry
    ++nfile;
    vfdP->fileName = fnamecopy;
    vfdP->fileFlags = fileFlags & ~(O_CREAT | O_TRUNC | O_EXCL);  // Safe for reopening
    vfdP->fileMode = fileMode;
    vfdP->fileSize = 0;
    vfdP->fdstate = 0x0;
    vfdP->resowner = NULL;

    // Step 8: Insert into VFD cache
    Insert(file);

    return file;
}
```