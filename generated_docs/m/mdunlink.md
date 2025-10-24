# mdunlink

## Location
[src/backend/storage/smgr/md.c:307-322](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/smgr/md.c#L307-L322)

## Overview
mdunlink safely removes relation files from disk, coordinating the deletion of specific forks or all forks while handling complex scenarios involving crash recovery, relfilenumber reuse prevention, and different relation types.

## Definition
void mdunlink(RelFileLocatorBackend rlocator, ForkNumber forknum, bool isRedo)

## Detailed Description
This function provides a sophisticated file deletion mechanism that goes beyond simple file removal. For regular relations, it implements a careful two-phase deletion strategy: the main fork is first truncated to zero length and scheduled for deletion after the next checkpoint, while additional segments are unlinked immediately after truncation. This approach prevents relfilenumber reuse hazards that could occur during crash recovery when WAL logging is minimal. The function handles different scenarios differently - temporary relations are deleted immediately since they pose no reuse threats, and during binary upgrades immediate deletion is also safe. When InvalidForkNumber is specified, it iterates through all fork types, otherwise it processes only the specified fork by delegating to mdunlinkfork.

## Parameters / Member Variables
- : RelFileLocatorBackend specifying the relation location and backend information
- : ForkNumber indicating which fork to delete, or InvalidForkNumber for all forks
- : bool indicating whether this is during WAL redo (affects deletion strategy)

## Dependencies
- Functions called/Symbols referenced:
  - [mdunlinkfork](mdunlinkfork.md) (performs actual fork-specific unlinking)
  - InvalidForkNumber (constant for processing all forks)
  - MAX_FORKNUM (maximum fork number for iteration)

- Called from (representative examples):
  - Declared in src/include/storage/md.h for external usage
  - Used by higher-level storage management during relation drops

## Notes and Other Information
- Implements complex relfilenumber reuse prevention logic to avoid data corruption during recovery
- Uses truncate-then-unlink strategy for additional segments to reclaim disk space immediately
- Handles temporary relations and binary upgrade scenarios with immediate deletion
- Main fork deletion is deferred to post-checkpoint to ensure crash recovery safety
- Warnings are reported instead of errors since function is typically called outside transactions
- The extensive comments in source code detail the intricate reasoning behind the deletion strategy
- Critical component of PostgreSQL's crash-safe storage management system

## Simplified Source

```c
void mdunlink(RelFileLocatorBackend rlocator, ForkNumber forknum, bool isRedo)
{
    // Determine which forks to process
    if (forknum == InvalidForkNumber) {
        // Delete all forks
        for (forknum = 0; forknum <= MAX_FORKNUM; forknum++)
            mdunlinkfork(rlocator, forknum, isRedo);
    } else {
        // Delete specific fork
        mdunlinkfork(rlocator, forknum, isRedo);
    }
}
```

**Key Points:**
- Safely removes relation files with complex crash-recovery logic
- Can delete a specific fork or all forks (when forknum is InvalidForkNumber)
- Delegates actual deletion work to mdunlinkfork for each fork
- Main fork uses truncate-then-defer-unlink strategy to prevent relfilenumber reuse
- Additional segments are truncated and unlinked immediately to reclaim disk space
- Special handling for temp relations, binary upgrades, and WAL redo scenarios