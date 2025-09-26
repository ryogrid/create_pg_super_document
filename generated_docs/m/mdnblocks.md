# mdnblocks

## Location
[src/backend/storage/smgr/md.c:1089-1152](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/smgr/md.c#L1089-L1152)

## Overview
mdnblocks retrieves the total number of blocks stored in a PostgreSQL relation, with the important side effect of opening all active segments of the relation.

## Definition

```c
BlockNumber
mdnblocks(SMgrRelation reln, ForkNumber forknum)
```
## Detailed Description
mdnblocks calculates and returns the total number of blocks in a specified fork of a PostgreSQL relation. The function has a crucial side effect: it ensures that all active segments of the relation are opened and added to the md_seg_fds array. This is important because normally only segments up to the last one actually accessed are kept open.

The function works by starting from the last known open segment and iterating through segments, checking their sizes. It assumes that all intermediate segments are exactly RELSEG_SIZE blocks long (which is validated elsewhere), and only needs to determine the size of the final segment. The function handles segment boundaries carefully and stops when it encounters a segment smaller than RELSEG_SIZE or when no more segments exist.

## Parameters / Member Variables
- : SMgrRelation pointer representing the relation to examine
- : ForkNumber identifying which fork of the relation to measure

## Dependencies
- Functions called/Symbols referenced:
  - [mdopenfork](mdopenfork.md)
  - [_mdnblocks](_mdnblocks.md)
  - [_mdfd_openseg](_mdfd_openseg.md)
- Called from (representative examples):
  - [mdextend](mdextend.md)
  - [mdzeroextend](mdzeroextend.md)
  - [mdwritev](mdwritev.md)
  - [mdregistersync](mdregistersync.md)
  - [mdimmedsync](mdimmedsync.md)

## Notes and Other Information
- Opens all active segments as a side effect, which is essential for proper segment management
- Assumes intermediate segments are exactly RELSEG_SIZE blocks (validated by higher-level code)
- Handles race conditions where segments might be truncated by other backends through relcache flush mechanism
- Uses EXTENSION_FAIL flag when opening the fork to avoid creating missing segments
- Includes a FATAL error check to prevent segments from being larger than RELSEG_SIZE
- The checkpointer process may have entries for inactive segments, which is acceptable since it doesn't need to compute relation sizes
- Avoids using O_CREAT when opening additional segments to prevent masking missing segment errors