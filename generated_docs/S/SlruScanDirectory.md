# SlruScanDirectory

## Location
src/backend/access/transam/slru.c: 1788 - 1827

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
  - AllocateDir
  - ReadDir  
  - SlruCorrectSegmentFilenameLength
  - strtoi64
  - SLRU_PAGES_PER_SEGMENT
  - elog (DEBUG2)
  - FreeDir
- Called from (representative examples):
  - TruncateCLOG
  - DeactivateCommitTs
  - TruncateCommitTs
  - TruncateMultiXact
  - SimpleLruTruncate
  - AsyncShmemInit
  - test_slru_delete_all

## Notes and Other Information
- No locking is applied during directory scanning - callers must ensure appropriate locking
- Directory scanning order is not guaranteed and depends on filesystem implementation  
- Validates filenames using both length (via SlruCorrectSegmentFilenameLength) and character content (hexadecimal only)
- Converts hexadecimal filenames to int64 segment numbers using base-16 parsing
- Provides DEBUG2-level logging for each callback invocation
- Returns the last return value from the callback, enabling result propagation
- Core infrastructure function used by all major SLRU maintenance operations including truncation and cleanup