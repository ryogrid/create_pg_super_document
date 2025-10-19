# XLogRecordMatchesRelationBlock

## Location
[src/bin/pg_waldump/pg_waldump.c:438-468](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_waldump/pg_waldump.c#L438-L468)

## Overview
XLogRecordMatchesRelationBlock determines whether a given WAL record matches specific relation and block filtering criteria used in pg_waldump.

## Definition

```c
static bool
XLogRecordMatchesRelationBlock(XLogReaderState *record,
							   RelFileLocator matchRlocator,
							   BlockNumber matchBlock,
							   ForkNumber matchFork)
```
## Detailed Description
This function iterates through all block references in a WAL record to determine if any of them match the specified filtering criteria. It supports flexible matching where any combination of relation, fork, and block number can be specified or left as wildcards. The function is used by pg_waldump to implement filtering functionality, allowing users to focus on WAL records that affect specific database objects or blocks. It handles empty/invalid values as wildcards, enabling partial matching scenarios.

## Parameters / Member Variables
- `*record`: XLogReaderState containing the decoded WAL record to examine
- `matchRlocator`: RelFileLocator to match against, or emptyRelFileLocator for wildcard matching
- `matchBlock`: Specific block number to match, or InvalidBlockNumber for any block
- `matchFork`: Fork number to match, or InvalidForkNumber for any fork
## Dependencies
- Functions called/Symbols referenced:
  - XLogRecMaxBlockId
  - [XLogRecGetBlockTagExtended](XLogRecGetBlockTagExtended.md)
  - RelFileLocatorEquals
  - InvalidForkNumber (constant)
  - emptyRelFileLocator (constant)
  - InvalidBlockNumber (constant)
- Called from (representative examples):
  - [main](../m/main.md) (used for filtering WAL records in pg_waldump)

## Notes and Other Information
- Returns true if any block reference in the record matches the filtering criteria
- Supports wildcard matching using invalid/empty values for flexible filtering
- Iterates through all block IDs present in the WAL record (0 to XLogRecMaxBlockId)
- Used primarily for implementing the --relation and --block options in pg_waldump
- Enables focused analysis of WAL activity affecting specific database objects

## Simplified Source

```c
static bool
XLogRecordMatchesRelationBlock(XLogReaderState *record,
                               RelFileLocator matchRlocator,
                               BlockNumber matchBlock,
                               ForkNumber matchFork)
{
    // Iterate through all block references in the WAL record
    for (int block_id = 0; block_id <= XLogRecMaxBlockId(record); block_id++) {
        RelFileLocator rlocator;
        ForkNumber forknum;
        BlockNumber blk;

        // Get block tag information for this block reference
        if (!XLogRecGetBlockTagExtended(record, block_id, &rlocator, &forknum, &blk, NULL))
            continue;

        // Check if this block matches our filter criteria
        // Note: Invalid values act as wildcards (match any)
        bool fork_matches = (matchFork == InvalidForkNumber || matchFork == forknum);
        bool relation_matches = (RelFileLocatorEquals(matchRlocator, emptyRelFileLocator) ||
                                RelFileLocatorEquals(matchRlocator, rlocator));
        bool block_matches = (matchBlock == InvalidBlockNumber || matchBlock == blk);

        if (fork_matches && relation_matches && block_matches)
            return true;
    }

    return false;
}
```