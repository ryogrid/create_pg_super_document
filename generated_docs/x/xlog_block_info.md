# xlog_block_info

## Location
[src/backend/access/transam/xlogrecovery.c:2336-2376](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogrecovery.c#L2336-L2376)

## Overview
A utility function that appends detailed information about all block references contained within an XLog record to a string buffer, including relation identifiers, fork numbers, block numbers, and full page write indicators.

## Definition

```c
static void
xlog_block_info(StringInfo buf, XLogReaderState *record)
```
## Detailed Description
This static function iterates through all block references in a WAL (Write-Ahead Log) record and generates detailed information about each referenced block. For each block reference found in the record, it extracts and formats:

1. **Block Reference ID**: The sequential identifier for this block reference within the record
2. **RelFileLocator**: Contains the relation's location identifiers (tablespace OID, database OID, relation number)
3. **Fork Number**: Identifies which fork of the relation (main, FSM, VM, etc.)
4. **Block Number**: The specific block within the fork
5. **Full Page Write (FPW) Indicator**: Shows if the record contains a complete block image

The function handles different output formats:
- For main fork blocks: "blkref #N: rel spc/db/rel, blk N"
- For other forks: "blkref #N: rel spc/db/rel, fork N, blk N"
- Appends "FPW" if the record includes a full page write for that block

This information is crucial for understanding which database pages are affected by a WAL record and whether full page images are included.

## Parameters / Member Variables
- : A StringInfo buffer where the formatted block information will be appended
- : An XLogReaderState pointer containing the WAL record to analyze

## Dependencies
- Functions called/Symbols referenced:
  -  (gets the maximum block ID in the record)
  -  (extracts block reference information)
  -  (appends formatted text to buffer)
  -  (appends string to buffer)
  -  (checks if block has full page image)
  -  (constant for main fork identifier)
- Called from (representative examples):
  -  (src/backend/access/transam/xlogrecovery.c:2282)
  -  (src/backend/access/transam/xlogrecovery.c:2327)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the xlogrecovery.c file
- The function handles the case where not all block IDs from 0 to max may be present (uses continue on missing blocks)
- Full Page Write (FPW) indicators are important for understanding recovery behavior and performance implications
- The RelFileLocator structure provides hierarchical identification: tablespace/database/relation
- Fork numbers distinguish between different files associated with a relation (main data, free space map, visibility map, etc.)
- Block references are fundamental to PostgreSQL's WAL system, tracking exactly which pages are modified by each transaction
- The output format provides sufficient detail to uniquely identify any database block affected by the WAL record

## Simplified Source

```c
static void xlog_block_info(StringInfo buf, XLogReaderState *record)
{
    // Iterate through all block references in the WAL record
    for (int block_id = 0; block_id <= XLogRecMaxBlockId(record); block_id++) {
        RelFileLocator rlocator;
        ForkNumber forknum;
        BlockNumber blk;

        // Try to get block reference information
        if (!XLogRecGetBlockTagExtended(record, block_id, &rlocator, &forknum, &blk, NULL))
            continue;  // Skip missing block references

        // Format block reference info differently based on fork type
        if (forknum != MAIN_FORKNUM) {
            // Include fork number for non-main forks
            appendStringInfo(buf, "; blkref #%d: rel %u/%u/%u, fork %u, blk %u",
                           block_id,
                           rlocator.spcOid, rlocator.dbOid, rlocator.relNumber,
                           forknum, blk);
        } else {
            // Simpler format for main fork
            appendStringInfo(buf, "; blkref #%d: rel %u/%u/%u, blk %u",
                           block_id,
                           rlocator.spcOid, rlocator.dbOid, rlocator.relNumber,
                           blk);
        }

        // Add "FPW" indicator if this block has a full page write
        if (XLogRecHasBlockImage(record, block_id))
            appendStringInfoString(buf, " FPW");
    }
}
```