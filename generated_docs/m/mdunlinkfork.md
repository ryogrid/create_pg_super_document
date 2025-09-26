# mdunlinkfork

## Location
[src/backend/storage/smgr/md.c:344-459](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/smgr/md.c#L344-L459)

## Overview
A static function that removes a relation fork by truncating and unlinking its segments, with special handling for different execution contexts like recovery, binary upgrade, and temporary relations.

## Definition

```c
static void
mdunlinkfork(RelFileLocatorBackend rlocator, ForkNumber forknum, bool isRedo)
```
## Detailed Description
The `mdunlinkfork` function is responsible for completely removing a relation fork from the filesystem. It operates in two phases: first handling the main segment (segment 0), then iterating through and removing all additional segments. The function implements different strategies based on the execution context - during recovery (`isRedo`), binary upgrade, or for non-main forks, it immediately unlinks files, while for main forks during normal operation, it may defer the unlink operation using the unlink scheduling mechanism. For each segment, it first truncates the file to prevent other backends from holding onto disk space, then either unlinks immediately or schedules the unlink for later execution.

## Parameters / Member Variables
- `rlocator`: RelFileLocatorBackend structure identifying the relation and backend
- `forknum`: The fork number (MAIN_FORKNUM, FSM_FORKNUM, VM_FORKNUM, or INIT_FORKNUM) to be removed
- `isRedo`: Boolean indicating whether this operation is being performed during WAL recovery

## Dependencies
- Functions called/Symbols referenced:
  - relpath (to construct file paths)
  - [do_truncate](../d/do_truncate.md) (to truncate file segments)
  - [register_forget_request](../r/register_forget_request.md) (to cancel pending sync requests)
  - [register_unlink_segment](../r/register_unlink_segment.md) (to schedule deferred unlink operations)
  - unlink (to remove files from filesystem)
  - RelFileLocatorBackendIsTemp (to check if relation is temporary)
  - ereport (for error logging)
  - [palloc](../p/palloc.md)/pfree (for memory management)

- Called from (representative examples):
  - [mdunlink](mdunlink.md) (the main entry point for relation unlinking)

## Notes and Other Information
- This is a static function only accessible within md.c
- Implements a two-phase removal strategy: truncate first, then unlink
- Special handling for temporary relations which bypass some operations
- Continues removing segments until ENOENT is encountered, ensuring all inactive segments are cleaned up
- Uses a compromise approach for error handling in the segment removal loop to avoid infinite loops while still attempting to clean up as much as possible
- The function preserves errno values using save_errno pattern throughout
- Deferred unlink scheduling is used for main forks during normal operation to avoid potential issues with other backends
- Part of PostgreSQL's storage manager layer responsible for physical file lifecycle management