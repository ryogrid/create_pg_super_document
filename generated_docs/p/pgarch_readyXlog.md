# pgarch_readyXlog

## Location
[src/backend/postmaster/pgarch.c:643-778](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/pgarch.c#L643-L778)

## Overview
Discovers and returns the highest priority WAL file ready for archival by scanning the archive_status directory and maintaining a priority-ordered cache of candidates.

## Definition
static bool pgarch_readyXlog(char *xlog)

## Detailed Description
pgarch_readyXlog implements an efficient file discovery mechanism for WAL files that need to be archived. The function operates in multiple phases:

1. **Cache Management**: Maintains a cached list of WAL files from previous directory scans to avoid repeated filesystem operations
2. **Directory Scanning**: When the cache is empty or a forced scan is requested, reads the archive_status directory for .ready files
3. **Priority Sorting**: Uses a binary heap to maintain files in priority order, ensuring .history files and older timeline segments are processed first
4. **Validation**: Verifies that status files still exist before returning cached entries
5. **Efficient Batching**: Processes up to NUM_FILES_PER_DIRECTORY_SCAN files at once to balance memory usage with scan efficiency

The function implements sophisticated priority logic where .history files are considered older than regular WAL segments, and segments from earlier timelines have higher priority. This ensures proper archival order for PostgreSQL recovery requirements.

## Parameters / Member Variables
- : Output parameter - character array to store the selected WAL filename
- Returns: Boolean indicating whether a ready file was found

## Dependencies
- Functions called/Symbols referenced:
  - [pg_atomic_exchange_u32](pg_atomic_exchange_u32.md) (check/clear forced directory scan flag)
  - [StatusFilePath](../S/StatusFilePath.md) (construct status file paths)
  - [binaryheap_reset](../b/binaryheap_reset.md) (reset the priority heap)
  - [AllocateDir](../A/AllocateDir.md)/ReadDir/FreeDir (directory traversal)
  - [binaryheap_add_unordered](../b/binaryheap_add_unordered.md)/binaryheap_build (heap construction)
  - [binaryheap_first](../b/binaryheap_first.md)/binaryheap_remove_first (heap operations)
  - [ready_file_comparator](../r/ready_file_comparator.md) (compare file priorities)
  - [CStringGetDatum](../C/CStringGetDatum.md)/DatumGetCString (datum conversion)
- Constants used:
  - XLOGDIR, MAX_XFN_CHARS, MIN_XFN_CHARS, VALID_XFN_CHARS, NUM_FILES_PER_DIRECTORY_SCAN
- Called from (representative examples):
  - [pgarch_ArchiverCopyLoop](pgarch_ArchiverCopyLoop.md) (main archival loop)

## Notes and Other Information
- Returns true if a file is found, false if no ready files exist
- This is a static function internal to the pgarch.c module
- Implements caching strategy to reduce filesystem I/O overhead during continuous archival
- Uses binary heap data structure for efficient priority-based file selection
- Priority ordering ensures WAL files are archived in sequence, maintaining recovery chain integrity
- Handles both timeline history files (.history) and regular WAL segments with appropriate priorities
- The function can be forced to rescan the directory via atomic flag manipulation
- Designed to handle large numbers of pending archive files efficiently through batched processing

## Simplified Source

```c
static bool pgarch_readyXlog(char *xlog) {
    char XLogArchiveStatusDir[MAXPGPATH];
    DIR *rldir;
    struct dirent *rlde;

    // Check if directory scan was requested
    if (pg_atomic_exchange_u32(&PgArch->force_dir_scan, 0) == 1)
        arch_files->arch_files_size = 0;

    // Return cached files if available
    while (arch_files->arch_files_size > 0) {
        struct stat st;
        char status_file[MAXPGPATH];
        char *arch_file;

        arch_files->arch_files_size--;
        arch_file = arch_files->arch_files[arch_files->arch_files_size];
        StatusFilePath(status_file, arch_file, ".ready");

        // Verify status file still exists
        if (stat(status_file, &st) == 0) {
            strcpy(xlog, arch_file);
            return true;
        }
    }

    // Reset heap for new scan
    binaryheap_reset(arch_files->arch_heap);

    // Scan archive_status directory for .ready files
    snprintf(XLogArchiveStatusDir, MAXPGPATH, XLOGDIR "/archive_status");
    rldir = AllocateDir(XLogArchiveStatusDir);

    while ((rlde = ReadDir(rldir, XLogArchiveStatusDir)) != NULL) {
        int basenamelen = (int) strlen(rlde->d_name) - 6;
        char basename[MAX_XFN_CHARS + 1];
        char *arch_file;

        // Validate filename format
        if (basenamelen < MIN_XFN_CHARS || basenamelen > MAX_XFN_CHARS)
            continue;
        if (strspn(rlde->d_name, VALID_XFN_CHARS) < basenamelen)
            continue;
        if (strcmp(rlde->d_name + basenamelen, ".ready") != 0)
            continue;

        // Extract basename (remove .ready suffix)
        memcpy(basename, rlde->d_name, basenamelen);
        basename[basenamelen] = '\0';

        // Add to priority heap
        if (arch_files->arch_heap->bh_size < NUM_FILES_PER_DIRECTORY_SCAN) {
            // Heap not full - add directly
            arch_file = arch_files->arch_filenames[arch_files->arch_heap->bh_size];
            strcpy(arch_file, basename);
            binaryheap_add_unordered(arch_files->arch_heap, CStringGetDatum(arch_file));

            // Build valid heap when full
            if (arch_files->arch_heap->bh_size == NUM_FILES_PER_DIRECTORY_SCAN)
                binaryheap_build(arch_files->arch_heap);
        } else if (ready_file_comparator(binaryheap_first(arch_files->arch_heap),
                                       CStringGetDatum(basename), NULL) > 0) {
            // Replace lowest priority file with current one
            arch_file = DatumGetCString(binaryheap_remove_first(arch_files->arch_heap));
            strcpy(arch_file, basename);
            binaryheap_add(arch_files->arch_heap, CStringGetDatum(arch_file));
        }
    }
    FreeDir(rldir);

    // No files found
    if (arch_files->arch_heap->bh_size == 0)
        return false;

    // Build heap if not already done
    if (arch_files->arch_heap->bh_size < NUM_FILES_PER_DIRECTORY_SCAN)
        binaryheap_build(arch_files->arch_heap);

    // Extract files in priority order
    arch_files->arch_files_size = arch_files->arch_heap->bh_size;
    for (int i = 0; i < arch_files->arch_files_size; i++)
        arch_files->arch_files[i] = DatumGetCString(binaryheap_remove_first(arch_files->arch_heap));

    // Return highest priority file
    arch_files->arch_files_size--;
    strcpy(xlog, arch_files->arch_files[arch_files->arch_files_size]);

    return true;
}
```