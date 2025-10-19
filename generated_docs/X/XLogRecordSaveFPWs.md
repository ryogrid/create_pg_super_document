# XLogRecordSaveFPWs

## Location
[src/bin/pg_waldump/pg_waldump.c:490-545](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_waldump/pg_waldump.c#L490-L545)

## Overview
XLogRecordSaveFPWs extracts and saves all full page writes (FPWs) from a WAL record to disk as individual files.

## Definition

```c
static void
XLogRecordSaveFPWs(XLogReaderState *record, const char *savepath)
```
## Detailed Description
This function iterates through all block references in a WAL record, identifies those containing full page writes, and saves them as individual files to a specified directory. Each saved page is automatically decompressed if necessary using RestoreBlockImage. The function generates descriptive filenames that include timeline, LSR, relation identifiers, block numbers, and fork information, making it easy to identify and analyze specific page images. This functionality is particularly useful for debugging, forensic analysis, and understanding the content of full page writes in PostgreSQL's WAL stream.

## Parameters / Member Variables
- `*record`: XLogReaderState containing the WAL record to process for full page writes
- `*savepath`: Directory path where the extracted page files should be saved
## Dependencies
- Functions called/Symbols referenced:
  - XLogRecMaxBlockId
  - XLogRecHasBlockRef
  - XLogRecHasBlockImage
  - [RestoreBlockImage](../R/RestoreBlockImage.md)
  - [XLogRecGetBlockTagExtended](XLogRecGetBlockTagExtended.md)
  - fopen
  - fwrite
  - fclose
  - PGAlignedBlock (type)
  - FORKNAMECHARS (constant)
  - MAX_FORKNUM (constant)
  - PG_BINARY_W (constant)
- Called from (representative examples):
  - [main](../m/main.md) (used when --save-fpw option is specified in pg_waldump)

## Notes and Other Information
- Creates files with naming pattern: timeline-lsn-space-db-relation-block-fork.extension
- Automatically handles decompression of compressed full page writes
- Validates fork numbers and uses standard fork names in filenames
- Provides comprehensive error handling for file operations
- Useful for forensic analysis and understanding WAL content at the page level
- Each saved file represents a complete 8KB database page as stored in the WAL

## Simplified Source

```c
static void
XLogRecordSaveFPWs(XLogReaderState *record, const char *savepath)
{
    // Iterate through all block references in the WAL record
    for (int block_id = 0; block_id <= XLogRecMaxBlockId(record); block_id++) {
        PGAlignedBlock buf;
        Page page;
        char filename[MAXPGPATH];
        char forkname[FORKNAMECHARS + 2];
        FILE *file;
        BlockNumber blk;
        RelFileLocator rnode;
        ForkNumber fork;

        // Skip blocks without references or full page images
        if (!XLogRecHasBlockRef(record, block_id) || !XLogRecHasBlockImage(record, block_id))
            continue;

        page = (Page) buf.data;

        // Restore the full page image (decompresses if needed)
        if (!RestoreBlockImage(record, block_id, page))
            pg_fatal("%s", record->errormsg_buf);

        // Get block tag information (relation, fork, block number)
        XLogRecGetBlockTagExtended(record, block_id, &rnode, &fork, &blk, NULL);

        // Generate fork name for filename
        if (fork >= 0 && fork <= MAX_FORKNUM)
            sprintf(forkname, "_%s", forkNames[fork]);
        else
            pg_fatal("invalid fork number: %u", fork);

        // Create descriptive filename with timeline, LSN, and relation info
        snprintf(filename, MAXPGPATH, "%s/%08X-%08X-%08X.%u.%u.%u.%u%s", savepath,
                 record->seg.ws_tli,
                 LSN_FORMAT_ARGS(record->ReadRecPtr),
                 rnode.spcOid, rnode.dbOid, rnode.relNumber, blk, forkname);

        // Write the page to file
        file = fopen(filename, PG_BINARY_W);
        if (!file)
            pg_fatal("could not open file \"%s\": %m", filename);

        if (fwrite(page, BLCKSZ, 1, file) != 1)
            pg_fatal("could not write file \"%s\": %m", filename);

        if (fclose(file) != 0)
            pg_fatal("could not close file \"%s\": %m", filename);
    }
}
```