# SlruScanDirectory

## Location
[src/backend/access/transam/slru.c:1788-1827](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/slru.c#L1788-L1827)

## Overview
A core function that scans an SLRU directory and applies a callback function to each valid SLRU segment file found, providing the foundation for various SLRU maintenance operations.

## Definition
```c
bool SlruScanDirectory(SlruCtl ctl, SlruScanCallback callback, void *data)
```

## Detailed Description
SlruScanDirectory is a fundamental utility function in the PostgreSQL SLRU subsystem that provides a generic mechanism for processing all SLRU segment files in a directory. The function opens the SLRU directory, iterates through all files, validates each filename for proper SLRU segment format (using both length and hexadecimal character validation), and calls a user-provided callback function for each valid segment. The callback receives the SLRU control structure, filename, starting page number of the segment, and optional user data. The function supports early termination if the callback returns true, making it suitable for both complete directory processing and search operations. It handles the conversion from segment filenames (hexadecimal) to segment numbers and page numbers automatically.

## Parameters / Member Variables
- `ctl`: SlruCtl structure containing SLRU control information and directory path
- `callback`: SlruScanCallback function pointer to be called for each valid segment file
- `data`: Opaque pointer passed through to the callback function for user-specific data

## Dependencies
- Functions called/Symbols referenced:
  - [AllocateDir](../A/AllocateDir.md)
  - [ReadDir](../R/ReadDir.md)  
  - [SlruCorrectSegmentFilenameLength](SlruCorrectSegmentFilenameLength.md)
  - strtoi64
  - SLRU_PAGES_PER_SEGMENT
  - elog (DEBUG2)
  - [FreeDir](../F/FreeDir.md)
- Called from (representative examples):
  - [TruncateCLOG](../T/TruncateCLOG.md)
  - [DeactivateCommitTs](../D/DeactivateCommitTs.md)
  - [TruncateCommitTs](../T/TruncateCommitTs.md)
  - [TruncateMultiXact](../T/TruncateMultiXact.md)
  - [SimpleLruTruncate](SimpleLruTruncate.md)
  - [AsyncShmemInit](../A/AsyncShmemInit.md)
  - [test_slru_delete_all](../t/test_slru_delete_all.md)

## Notes and Other Information
- No locking is applied during directory scanning - callers must ensure appropriate locking
- Directory scanning order is not guaranteed and depends on filesystem implementation  
- Validates filenames using both length (via SlruCorrectSegmentFilenameLength) and character content (hexadecimal only)
- Converts hexadecimal filenames to int64 segment numbers using base-16 parsing
- Provides DEBUG2-level logging for each callback invocation
- Returns the last return value from the callback, enabling result propagation
- Core infrastructure function used by all major SLRU maintenance operations including truncation and cleanup

## Simplified Source

```c
// Simplified version of SlruScanDirectory
bool SlruScanDirectory(SlruCtl ctl, SlruScanCallback callback, void *data) {
    bool retval = false;
    DIR *cldir;
    struct dirent *clde;

    // Core logic step 1: Open SLRU directory
    cldir = AllocateDir(ctl->Dir);

    // Core logic step 2: Scan all files in directory
    while ((clde = ReadDir(cldir, ctl->Dir)) != NULL) {
        size_t len = strlen(clde->d_name);

        // Core logic step 3: Validate filename format (length and hex characters)
        if (SlruCorrectSegmentFilenameLength(ctl, len) &&
            strspn(clde->d_name, "0123456789ABCDEF") == len) {

            // Core logic step 4: Convert filename to segment info and invoke callback
            int64 segno = strtoi64(clde->d_name, NULL, 16);
            int64 segpage = segno * SLRU_PAGES_PER_SEGMENT;

            elog(DEBUG2, "SlruScanDirectory invoking callback on %s/%s",
                 ctl->Dir, clde->d_name);

            retval = callback(ctl, clde->d_name, segpage, data);
            if (retval) {
                break; // Stop scanning if callback returns true
            }
        }
    }

    // Core logic step 5: Clean up directory handle
    FreeDir(cldir);

    return retval;
}
```

Key simplifications made:
- Focused on the five core steps: open directory, scan files, validate format, invoke callback, cleanup
- Removed detailed explanatory comments about callback parameters
- Maintained essential validation and error handling logic
- Simplified variable declarations
- Preserved early termination behavior when callback returns true