# SetWALFileNameForCleanup

## Location
[src/bin/pg_archivecleanup/pg_archivecleanup.c:183-256](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_archivecleanup/pg_archivecleanup.c#L183-L256)

## Overview
Determines and sets the earliest WAL filename that should be kept in the archive by parsing and normalizing the restart WAL filename provided by the user.

## Definition
```c
static void SetWALFileNameForCleanup(void)
```

## Detailed Description
The SetWALFileNameForCleanup function processes the user-provided restart WAL filename to determine the cleanup boundary for the archive. It handles three types of WAL-related filenames: regular WAL files, partial WAL files (.partial), and backup history files (.backup). For partial and backup history files, the function extracts the base WAL filename components (timeline, log, and segment numbers) and constructs the corresponding standard WAL filename to use as the cleanup boundary.

The function ensures that files with extensions like .partial or .backup are properly normalized to their base WAL filename equivalent, preventing incorrect cleanup boundaries that could result in removing the wrong files from the archive.

## Parameters / Member Variables
This function takes no parameters and operates on several global variables:
- `restartWALFileName`: Input filename provided by the user, which may be a WAL file, partial WAL file, or backup history file
- `additional_ext`: Optional additional extension to trim from the filename before processing
- `exclusiveCleanupFileName`: Output variable that will contain the normalized WAL filename to use as the cleanup boundary
- `progname`: Program name used in error messages

## Dependencies
- Functions called/Symbols referenced:
  - [TrimExtension](../T/TrimExtension.md) (custom function to remove file extensions)
  - [IsXLogFileName](../I/IsXLogFileName.md) (PostgreSQL function to validate WAL filenames)  
  - [IsPartialXLogFileName](../I/IsPartialXLogFileName.md) (PostgreSQL function to validate partial WAL filenames)
  - [IsBackupHistoryFileName](../I/IsBackupHistoryFileName.md) (PostgreSQL function to validate backup history filenames)
  - [XLogFileNameById](../X/XLogFileNameById.md) (PostgreSQL function to construct WAL filename from components)
  - sscanf (standard library function for parsing formatted strings)
  - strcpy (standard library function for string copying)
  - pg_log_error (PostgreSQL logging function)
  - pg_log_error_hint (PostgreSQL logging function for hints)
  - exit (standard library function)
- Called from (representative examples):
  - [main](../m/main.md) (in src/bin/pg_archivecleanup/pg_archivecleanup.c:385)

## Notes and Other Information
- The function is marked as `static`, making it internal to the pg_archivecleanup.c file
- For regular WAL filenames, the filename is used directly as the cleanup boundary
- For partial WAL files (.partial), the timeline, log, and segment numbers are extracted and used to construct the base WAL filename
- For backup history files (.backup), the timeline, log, segment, and offset numbers are extracted, but only the first three are used to construct the base WAL filename  
- The function terminates the program with exit code 2 if the provided filename does not match any of the expected formats
- This normalization prevents issues where partial/backup files would create incorrect cleanup boundaries due to their extended naming convention
- Located at src/bin/pg_archivecleanup/pg_archivecleanup.c:183-256

## Simplified Source

```c
static void
SetWALFileNameForCleanup(void)
{
    bool fnameOK = false;

    // Remove any additional extension first
    TrimExtension(restartWALFileName, additional_ext);

    // Handle regular WAL files directly
    if (IsXLogFileName(restartWALFileName)) {
        strcpy(exclusiveCleanupFileName, restartWALFileName);
        fnameOK = true;
    }
    // Handle partial WAL files (.partial)
    else if (IsPartialXLogFileName(restartWALFileName)) {
        uint32 tli = 1, log = 0, seg = 0;

        // Parse timeline, log, and segment from partial filename
        int args = sscanf(restartWALFileName, "%08X%08X%08X.partial", &tli, &log, &seg);
        if (args == 3) {
            fnameOK = true;
            // Create base WAL filename without .partial extension
            XLogFileNameById(exclusiveCleanupFileName, tli, log, seg);
        }
    }
    // Handle backup history files (.backup)
    else if (IsBackupHistoryFileName(restartWALFileName)) {
        uint32 tli = 1, log = 0, seg = 0, offset = 0;

        // Parse all components from backup filename
        int args = sscanf(restartWALFileName, "%08X%08X%08X.%08X.backup",
                         &tli, &log, &seg, &offset);
        if (args == 4) {
            fnameOK = true;
            // Create base WAL filename without .backup extension
            XLogFileNameById(exclusiveCleanupFileName, tli, log, seg);
        }
    }

    // Exit with error if filename format not recognized
    if (!fnameOK) {
        pg_log_error("invalid file name argument");
        pg_log_error_hint("Try \"%s --help\" for more information.", progname);
        exit(2);
    }
}
```