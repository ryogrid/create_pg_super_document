# mdopenfork

## Location
[src/backend/storage/smgr/md.c:637-679](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/smgr/md.c#L637-L679)

## Overview
A static function that opens the first segment of a specified relation fork, handling various behaviors for file existence and providing the foundation for relation access.

## Definition

```c
static MdfdVec *
mdopenfork(SMgrRelation reln, ForkNumber forknum, int behavior)
```
## Detailed Description
The `mdopenfork` function is responsible for opening the first segment (segment 0) of a relation fork. It serves as the entry point for accessing relation files on disk by opening the primary segment file and initializing the corresponding MdfdVec structure. The function includes optimization logic to avoid redundant opens by checking if the fork is already open. When opening a file, it uses the appropriate flags via `_mdfd_open_flags()` and handles various error conditions based on the specified behavior parameter. The function only opens the first segment initially - additional segments are opened on-demand by other functions. It properly initializes the file descriptor vector and sets up the MdfdVec structure for subsequent operations.

## Parameters / Member Variables
- `reln`: SMgrRelation pointer to the storage manager relation descriptor
- `forknum`: Fork number (MAIN_FORKNUM, FSM_FORKNUM, VM_FORKNUM, or INIT_FORKNUM) to open
- `behavior`: Integer flags controlling behavior on file open failure (EXTENSION_RETURN_NULL, EXTENSION_FAIL, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - relpath (to construct the file path for the fork)
  - [PathNameOpenFile](../P/PathNameOpenFile.md) (to open the file with appropriate flags)
  - [_mdfd_open_flags](_mdfd_open_flags.md) (to get the correct file open flags)
  - [_fdvec_resize](../f/_fdvec_resize.md) (to resize the file descriptor vector)
  - [_mdnblocks](_mdnblocks.md) (for assertion checking)
  - FILE_POSSIBLY_DELETED (macro to check for file deletion errno values)
  - ereport (for error reporting)
  - [pfree](../p/pfree.md) (for memory cleanup)

- Called from (representative examples):
  - [mdexists](mdexists.md) (to check if a relation fork exists)
  - [mdnblocks](mdnblocks.md) (to get the number of blocks in a fork)
  - [_mdfd_getseg](_mdfd_getseg.md) (when opening segments for various operations)

## Notes and Other Information
- This is a static function only accessible within md.c
- Returns NULL or MdfdVec pointer depending on success and behavior flags
- Includes short-circuit logic to return existing open segment without redundant operations
- The EXTENSION_CREATE behavior is treated the same as EXTENSION_FAIL - it allows extending existing relations but not creating new ones
- Only opens the first segment (segment 0) initially; additional segments are opened on-demand
- Properly handles the case where files might be deleted concurrently (FILE_POSSIBLY_DELETED)
- Initializes the MdfdVec structure with file descriptor and segment number
- Includes assertion to verify the segment doesn't exceed RELSEG_SIZE
- Part of PostgreSQL's storage manager layer, providing the foundation for all file-based relation operations
- The function manages the md_seg_fds array and md_num_open_segs counter in the SMgrRelation structure

## Simplified Source

```c
static MdfdVec *
mdopenfork(SMgrRelation reln, ForkNumber forknum, int behavior)
{
    MdfdVec *mdfd;
    char *path;
    File fd;

    // Return existing open segment if already available
    if (reln->md_num_open_segs[forknum] > 0)
        return &reln->md_seg_fds[forknum][0];

    // Construct file path and open the first segment
    path = relpath(reln->smgr_rlocator, forknum);
    fd = PathNameOpenFile(path, _mdfd_open_flags());

    // Handle file open failure
    if (fd < 0) {
        if ((behavior & EXTENSION_RETURN_NULL) && FILE_POSSIBLY_DELETED(errno)) {
            pfree(path);
            return NULL;
        }
        ereport(ERROR, "could not open file \"%s\": %m", path);
    }

    pfree(path);

    // Initialize the file descriptor vector and MdfdVec structure
    _fdvec_resize(reln, forknum, 1);
    mdfd = &reln->md_seg_fds[forknum][0];
    mdfd->mdfd_vfd = fd;
    mdfd->mdfd_segno = 0;

    return mdfd;
}
```