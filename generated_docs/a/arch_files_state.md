# arch_files_state

## Location
src/backend/postmaster/pgarch.c: 123 - 156

## Overview
arch_files_state is a structure used to efficiently track multiple WAL files ready for archiving, minimizing directory scans by using a max-heap and batch processing approach.

## Definition
struct arch_files_state
{
    binaryheap *arch_heap;
    int         arch_files_size;    /* number of live entries in arch_files[] */
    char       *arch_files[NUM_FILES_PER_DIRECTORY_SCAN];
    /* buffers underlying heap, and later arch_files[], entries: */
    char        arch_filenames[NUM_FILES_PER_DIRECTORY_SCAN][MAX_XFN_CHARS + 1];
};

## Detailed Description
arch_files_state is a local structure within the PostgreSQL archiver process designed to optimize the archival of multiple WAL files by reducing the frequency of directory scans. The structure implements a two-phase approach: first, it uses a max-heap (arch_heap) during directory scanning to track the highest-priority files for archiving. After the scan completes, file names are stored in ascending priority order in arch_files array. This design significantly improves archival performance when there are many files waiting to be archived, as it processes files in batches of up to NUM_FILES_PER_DIRECTORY_SCAN (64) files per scan.

## Parameters / Member Variables
- : Binary max-heap used during directory scan to prioritize files for archiving based on their importance
- : Number of active entries currently stored in the arch_files array
- : Array of pointers to filenames, storing up to NUM_FILES_PER_DIRECTORY_SCAN (64) files in priority order
- : Static buffer array providing storage for the actual filename strings, with each filename limited to MAX_XFN_CHARS (40) + 1 characters

## Dependencies
- Functions called/Symbols referenced:
  - binaryheap
  - NUM_FILES_PER_DIRECTORY_SCAN (64)
  - MAX_XFN_CHARS (40)
- Called from (representative examples):
  - PgArchiverMain

## Notes and Other Information
- This structure is allocated using palloc() within the archiver process rather than being a static array, making it process-local
- The design optimizes for scenarios with many files to archive by batching directory scans
- Files are processed in priority order to ensure the most critical WAL files are archived first
- The structure coordinates with various archiver functions including pgarch_readyXlog(), pgarch_archiveXlog(), and pgarch_archiveDone()
- Located at src/backend/postmaster/pgarch.c:123-156