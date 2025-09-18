# load_hba

## Location
[src/backend/libpq/hba.c:2583-2688](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/hba.c#L2583-L2688)

## Overview
Reads and parses the pg_hba.conf configuration file, creating a validated list of HBA (Host-Based Authentication) rules that control client access to the PostgreSQL database.

## Definition


## Detailed Description
This function orchestrates the complete loading and validation process for the pg_hba.conf configuration file. It implements a safe, atomic replacement strategy that ensures the system continues operating with valid configuration even if errors occur during reload:

1. **File Opening**: Opens the HBA configuration file using the configured HbaFileName
2. **Tokenization**: Breaks the file content into structured tokens for parsing
3. **Memory Management**: Creates a dedicated memory context for HBA data to enable clean cleanup
4. **Line-by-Line Parsing**: Processes each tokenized line through parse_hba_line(), accumulating valid rules while tracking any parse errors
5. **Validation**: Ensures the configuration contains at least one valid entry (required for database connectivity)
6. **Atomic Replacement**: Only replaces the active configuration if the entire file parses successfully

The function employs comprehensive error handling, continuing to parse the entire file even after encountering errors to provide complete error reporting. If any errors occur, the original configuration remains active and the function returns false.

## Parameters / Member Variables
- Returns:  - true if file loaded successfully, false if parse errors occurred

## Dependencies
- Functions called/Symbols referenced:
  - [open_auth_file](../o/open_auth_file.md) (opens authentication configuration file)
  - [tokenize_auth_file](../t/tokenize_auth_file.md) (breaks file into tokens)
  - AllocSetContextCreate (creates memory context for HBA data)
  - [parse_hba_line](../p/parse_hba_line.md) (parses individual HBA configuration lines)
  - lappend (appends items to list)
  - ereport/errcode/errmsg (error reporting)
  - [free_auth_file](../f/free_auth_file.md) (cleanup tokenized file data)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)/MemoryContextDelete (memory context management)
  - PostmasterContext, ALLOCSET_SMALL_SIZES (memory allocation constants)
  - [TokenizedAuthLine](../T/TokenizedAuthLine.md), HbaLine (data structures)
- Called from:
  - [PostmasterMain](../P/PostmasterMain.md) (src/backend/postmaster/postmaster.c:1306)
  - [process_pm_reload_request](../p/process_pm_reload_request.md) (src/backend/postmaster/postmaster.c:2131)
  - [PerformAuthentication](../P/PerformAuthentication.md) (src/backend/utils/init/postinit.c:213)

## Notes and Other Information
- Uses global variables HbaFileName, parsed_hba_context, and parsed_hba_lines
- Implements "all-or-nothing" parsing - configuration is only replaced if entirely valid
- Requires at least one valid HBA entry to prevent complete database lockout
- Memory context usage ensures complete cleanup of allocated structures on parse failure
- Critical system function called during startup and SIGHUP configuration reload
- Parse errors are logged but don't prevent continued parsing of remaining lines
- On failure during reload, the system continues with the previous valid configuration