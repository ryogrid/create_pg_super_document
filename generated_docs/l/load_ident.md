# load_ident

## Location
[src/backend/libpq/hba.c:2959-3047](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/hba.c#L2959-L3047)

## Overview
Reads and parses the PostgreSQL ident configuration file to create a list of IdentLine records that define user name mapping rules for authentication.

## Definition

```c
bool
load_ident(void)
```
## Detailed Description
The `load_ident` function is responsible for loading and parsing PostgreSQL's ident mapping configuration file (typically pg_ident.conf). This function works similarly to `load_hba()` but specifically handles the user identity mapping configuration.

The function performs the following operations:
1. Opens the ident configuration file using `open_auth_file()`
2. Tokenizes the file contents using `tokenize_auth_file()`  
3. Creates a dedicated memory context for parsing
4. Parses each tokenized line using `parse_ident_line()`
5. Builds a list of `IdentLine` structures containing the mapping rules
6. Replaces the global `parsed_ident_lines` with the newly parsed configuration

If any parsing errors occur, the function continues processing the remaining lines to report multiple errors, but ultimately returns false and cleans up all allocated memory. The function is designed to be non-fatal - if the ident file cannot be loaded, PostgreSQL will simply not perform any special identity mappings.

## Parameters / Member Variables
This function takes no parameters and returns a boolean indicating success or failure.

## Dependencies
- Functions called/Symbols referenced:
  - [open_auth_file](../o/open_auth_file.md) (opens the ident configuration file)
  - [tokenize_auth_file](../t/tokenize_auth_file.md) (breaks file into tokens)
  - AllocSetContextCreate (creates memory context for parsing)
  - [parse_ident_line](../p/parse_ident_line.md) (parses individual ident mapping lines)
  - [free_auth_file](../f/free_auth_file.md) (cleans up file resources)
  - [MemoryContextDelete](../M/MemoryContextDelete.md) (manages memory cleanup)
  - [lappend](lappend.md) (adds parsed lines to result list)
  - IdentFileName (global variable containing ident file path)
  - PostmasterContext (parent memory context)
  - parsed_ident_context, parsed_ident_lines (global variables storing results)
- Called from (representative examples):
  - [PostmasterMain](../P/PostmasterMain.md) (during server startup)
  - [process_pm_reload_request](../p/process_pm_reload_request.md) (when configuration is reloaded)
  - [PerformAuthentication](../P/PerformAuthentication.md) (during authentication process)

## Notes and Other Information
- This function is located at src/backend/libpq/hba.c:2959-3047
- The function uses a separate memory context (`ident_context`) to ensure proper cleanup if parsing fails
- Unlike some configuration loading functions, this one is designed to be non-fatal - server startup continues even if the ident file cannot be loaded
- The parsed results are stored in global variables `parsed_ident_lines` and `parsed_ident_context` for use by `check_usermap()`
- Error reporting is done at LOG level rather than FATAL level to allow the server to continue operating
- The function supports regular expressions in ident mapping rules through the `parse_ident_line()` helper
- Memory management is carefully handled - if parsing fails, the entire memory context is deleted to prevent leaks

## Simplified Source

```c
// Simplified version of load_ident
bool load_ident(void) {
    FILE *file;
    List *ident_lines = NIL;
    List *new_parsed_lines = NIL;
    bool success = true;
    MemoryContext ident_context;
    MemoryContext oldcxt;

    // Step 1: Open the ident configuration file
    file = open_auth_file(IdentFileName, LOG, 0, NULL);
    if (file == NULL) {
        return false;  // File couldn't be opened
    }

    // Step 2: Break file into tokens
    tokenize_auth_file(IdentFileName, file, &ident_lines, LOG, 0);

    // Step 3: Create memory context for parsing
    ident_context = AllocSetContextCreate(PostmasterContext,
                                          "ident parser context",
                                          ALLOCSET_SMALL_SIZES);
    oldcxt = MemoryContextSwitchTo(ident_context);

    // Step 4: Parse each line and build mapping rules
    foreach(line_cell, ident_lines) {
        TokenizedAuthLine *tok_line = (TokenizedAuthLine *) lfirst(line_cell);

        // Skip lines that already have errors
        if (tok_line->err_msg != NULL) {
            success = false;
            continue;
        }

        // Parse this line into an IdentLine structure
        IdentLine *newline = parse_ident_line(tok_line, LOG);
        if (newline == NULL) {
            success = false;  // Parse error occurred
            continue;         // Keep processing other lines
        }

        new_parsed_lines = lappend(new_parsed_lines, newline);
    }

    // Step 5: Clean up file resources
    free_auth_file(file, 0);
    MemoryContextSwitchTo(oldcxt);

    // Step 6: Handle parsing results
    if (!success) {
        // Parsing failed - clean up and return false
        MemoryContextDelete(ident_context);
        return false;
    }

    // Step 7: Replace global configuration with new parsed data
    if (parsed_ident_context != NULL) {
        MemoryContextDelete(parsed_ident_context);
    }
    parsed_ident_context = ident_context;
    parsed_ident_lines = new_parsed_lines;

    return true;
}
```

Key simplifications made:
- Removed detailed error handling comments for clarity
- Simplified variable declarations and initialization
- Added step-by-step comments explaining the main logic flow
- Abstracted the foreach loop details while preserving the essential parsing logic
- Focused on the main execution path (successful file loading and parsing)
- Consolidated the memory management operations
- Renamed `ok` variable to `success` for clarity