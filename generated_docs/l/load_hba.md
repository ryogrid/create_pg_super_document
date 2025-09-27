# load_hba

## Location
[src/backend/libpq/hba.c:2583-2688](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/hba.c#L2583-L2688)

## Overview
Reads and parses the pg_hba.conf configuration file, creating a validated list of HBA (Host-Based Authentication) rules that control client access to the PostgreSQL database.

## Definition

```c
bool
load_hba(void)
```
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
  - [lappend](lappend.md) (appends items to list)
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

## Simplified Source

```c
// Simplified version of load_hba
bool load_hba(void) {
    FILE *file;
    List *hba_lines = NIL;
    List *new_parsed_lines = NIL;
    bool ok = true;
    MemoryContext hbacxt;
    MemoryContext oldcxt;

    // Step 1: Open and tokenize the HBA configuration file
    file = open_auth_file(HbaFileName, LOG, 0, NULL);
    if (file == NULL) {
        return false;  // File opening failed
    }

    tokenize_auth_file(HbaFileName, file, &hba_lines, LOG, 0);

    // Step 2: Create dedicated memory context for HBA data
    hbacxt = AllocSetContextCreate(PostmasterContext,
                                   "hba parser context",
                                   ALLOCSET_SMALL_SIZES);
    oldcxt = MemoryContextSwitchTo(hbacxt);

    // Step 3: Parse each tokenized line into HBA rules
    foreach(line, hba_lines) {
        TokenizedAuthLine *tok_line = (TokenizedAuthLine *) lfirst(line);
        HbaLine *newline;

        // Skip lines that already have tokenization errors
        if (tok_line->err_msg != NULL) {
            ok = false;
            continue;
        }

        // Parse the line into an HBA rule
        newline = parse_hba_line(tok_line, LOG);
        if (newline == NULL) {
            ok = false;  // Parse error occurred
            continue;   // Keep parsing to find all errors
        }

        new_parsed_lines = lappend(new_parsed_lines, newline);
    }

    // Step 4: Validate that we have at least one valid entry
    if (ok && new_parsed_lines == NIL) {
        ereport(LOG, (errcode(ERRCODE_CONFIG_FILE_ERROR),
                     errmsg("configuration file \"%s\" contains no entries",
                            HbaFileName)));
        ok = false;
    }

    // Step 5: Cleanup tokenizer resources
    free_auth_file(file, 0);
    MemoryContextSwitchTo(oldcxt);

    // Step 6: Handle parse results - atomic replacement or cleanup
    if (!ok) {
        // Parsing failed - cleanup and keep old configuration
        MemoryContextDelete(hbacxt);
        return false;
    }

    // Step 7: Success - replace the active HBA configuration
    if (parsed_hba_context != NULL) {
        MemoryContextDelete(parsed_hba_context);
    }
    parsed_hba_context = hbacxt;
    parsed_hba_lines = new_parsed_lines;

    return true;
}
```

Key simplifications made:
- Removed detailed comments and consolidated error handling logic
- Added step-by-step comments to clarify the main algorithm flow
- Simplified variable declarations by grouping related ones
- Focused on the core atomic replacement pattern
- Abstracted complex memory context switching details
- Emphasized the main execution path while preserving error handling structure