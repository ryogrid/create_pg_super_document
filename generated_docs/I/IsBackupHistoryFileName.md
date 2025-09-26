# IsBackupHistoryFileName

## Location
src/include/access/xlog_internal.h: 253 - 260

## Overview
IsBackupHistoryFileName is a static inline function that determines whether a given filename corresponds to a PostgreSQL backup history file by checking its format and extension.

## Definition


## Detailed Description
This function validates whether a filename follows the expected backup history file naming convention in PostgreSQL's WAL system. It performs three checks: verifies the filename is longer than the standard WAL filename length, ensures the initial portion contains only hexadecimal characters (0-9, A-F), and confirms the file ends with the '.backup' extension. This validation is crucial for identifying legitimate backup history files during cleanup and archival operations.

## Parameters / Member Variables
- `fname`: Constant character pointer to the filename string to be validated

## Dependencies
- Functions called/Symbols referenced:
  - XLOG_FNAME_LEN (constant used twice for length validation)
  - Standard C library functions: strlen, strspn, strcmp
- Called from (representative examples):
  - CleanupBackupHistory (in src/backend/access/transam/xlog.c)
  - CleanupPriorWALFiles (in src/bin/pg_archivecleanup/pg_archivecleanup.c)
  - SetWALFileNameForCleanup (in src/bin/pg_archivecleanup/pg_archivecleanup.c)

## Notes and Other Information
The function implements a three-step validation: first ensuring the filename is longer than XLOG_FNAME_LEN, then verifying the prefix contains only hexadecimal digits for the expected length, and finally confirming it ends with '.backup'. This approach efficiently filters out non-backup-history files while ensuring proper format compliance. The function is commonly used in WAL cleanup and archival utilities.