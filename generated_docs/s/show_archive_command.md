# show_archive_command

## Location
[src/backend/access/transam/xlog.c:4765-4776](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L4765-L4776)

## Overview
A GUC show hook function that displays the current archive_command value, showing "(disabled)" when archiving is inactive.

## Definition
```c
const char *show_archive_command(void)
```

## Detailed Description
This function serves as the show hook for the archive_command GUC parameter, providing a user-friendly display of the current archiving configuration. Instead of always showing the raw archive_command value, it intelligently displays "(disabled)" when WAL archiving is not currently active, giving users clear feedback about the actual state of the archiving system.

The function checks the current archiving status using XLogArchivingActive() and returns either the actual command string or the disabled indicator. This provides better user experience when viewing configuration settings, as it clearly indicates when the archive command is configured but not operational.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - XLogArchivingActive
- Called from (representative examples):
  - GUC system when displaying parameter values

## Notes and Other Information
- Returns the global XLogArchiveCommand when archiving is active
- Returns the literal string "(disabled)" when archiving is inactive
- Provides user-friendly display of archiving status in configuration views
- Part of the GUC hook system for parameter display customization

## Simplified Source

```c
const char *show_archive_command(void) {
    // Check if WAL archiving is currently active
    if (XLogArchivingActive())
        return XLogArchiveCommand;  // Return actual command
    else
        return "(disabled)";        // Show disabled status
}
```