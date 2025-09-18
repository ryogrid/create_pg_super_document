# pgarch_readyXlog

## Location
src/backend/postmaster/pgarch.c: 643 - 778

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
  - StatusFilePath (construct status file paths)
  - [binaryheap_reset](../b/binaryheap_reset.md) (reset the priority heap)
  - AllocateDir/ReadDir/FreeDir (directory traversal)
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