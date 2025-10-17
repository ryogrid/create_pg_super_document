# FindStreamingStart

## Location
[src/bin/pg_basebackup/pg_receivewal.c:268-369](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/pg_receivewal.c#L268-L369)

## Overview
Determines the starting location for WAL streaming by analyzing existing WAL segment files in the destination directory to find the appropriate resume point.

## Definition

```c
struct dirent *dirent;
```
## Detailed Description
This function scans the destination directory for existing WAL (Write-Ahead Logging) files to determine where streaming should resume. It examines all WAL segment files, validates their completeness by checking file sizes, and identifies the highest timeline ID with the latest complete segment. The function supports multiple compression algorithms (none, gzip, LZ4) and handles the complexity of verifying compressed segment integrity.

The function implements sophisticated logic to handle different compression formats:
- For uncompressed segments: directly checks file size against expected WAL segment size
- For gzip-compressed segments: reads the last 4 bytes which contain uncompressed size information
- For LZ4-compressed segments: decompresses content to verify actual uncompressed size

If no valid WAL files are found in the directory, it returns InvalidXLogRecPtr to indicate streaming should start from the beginning.

## Parameters / Member Variables
- : Output parameter that receives the timeline ID of the highest timeline found with complete segments

## Dependencies
- Functions called/Symbols referenced:
  - [get_destination_dir](../g/get_destination_dir.md)
  - [readdir](../r/readdir.md)
  - [is_xlogfilename](../i/is_xlogfilename.md)
  - [XLogFromFileName](../X/XLogFromFileName.md)
  - [stat](../s/stat.md), open, lseek, read, close (system calls)
  - pg_log_warning
  - [pg_fatal](../p/pg_fatal.md)
- Called from (representative examples):
  - [StreamLog](../S/StreamLog.md) (in pg_receivewal.c:537)

## Notes and Other Information
- This is a static function, only accessible within pg_receivewal.c
- Handles three compression algorithms: none, gzip, and LZ4 (if compiled with LZ4 support)
- Returns InvalidXLogRecPtr when no suitable WAL files are found
- Performs comprehensive validation of WAL segment file integrity
- Uses timeline ID to handle PostgreSQL's timeline switching mechanism
- Critical for ensuring WAL streaming continuity and avoiding gaps or overlaps
- The function's logic accounts for partial files (*.partial) which are skipped during size validation

## Simplified Source

```c
static XLogRecPtr FindStreamingStart(uint32 *tli) {
    DIR *dir;
    struct dirent *dirent;
    XLogSegNo high_segno = 0;
    uint32 high_tli = 0;
    bool high_ispartial = false;

    // Open destination directory
    dir = get_destination_dir(basedir);

    // Scan directory for WAL files
    while ((dirent = readdir(dir)) != NULL) {
        uint32 timeline;
        XLogSegNo segno;
        bool ispartial;
        pg_compress_algorithm compression;

        // Check if this is a valid WAL filename
        if (!is_xlogfilename(dirent->d_name, &ispartial, &compression)) {
            continue;
        }

        // Parse timeline and segment number from filename
        XLogFromFileName(dirent->d_name, &timeline, &segno, WalSegSz);

        // Validate segment completeness based on compression type
        if (!ispartial) {
            if (!validate_segment_size(dirent->d_name, compression)) {
                continue; // Skip invalid segments
            }
        }

        // Track highest complete segment on highest timeline
        if (timeline > high_tli ||
            (timeline == high_tli && segno >= high_segno && !ispartial)) {
            high_tli = timeline;
            high_segno = segno;
            high_ispartial = ispartial;
        }
    }

    close_destination_dir(dir, basedir);

    // Return starting position or invalid if no complete segments found
    if (high_tli == 0) {
        return InvalidXLogRecPtr;
    }

    *tli = high_tli;
    return XLogSegNoOffsetToRecPtr(high_segno + 1, 0, WalSegSz);
}
```