# load_ident

## Location
src/backend/libpq/hba.c: 2959 - 3047

## Overview
Reads and parses the PostgreSQL ident configuration file to create a list of IdentLine records that define user name mapping rules for authentication.

## Definition


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
  - open_auth_file (opens the ident configuration file)
  - tokenize_auth_file (breaks file into tokens)
  - AllocSetContextCreate (creates memory context for parsing)
  - parse_ident_line (parses individual ident mapping lines)
  - free_auth_file (cleans up file resources)
  - MemoryContextDelete (manages memory cleanup)
  - lappend (adds parsed lines to result list)
  - IdentFileName (global variable containing ident file path)
  - PostmasterContext (parent memory context)
  - parsed_ident_context, parsed_ident_lines (global variables storing results)
- Called from (representative examples):
  - PostmasterMain (during server startup)
  - process_pm_reload_request (when configuration is reloaded)
  - PerformAuthentication (during authentication process)

## Notes and Other Information
- This function is located at src/backend/libpq/hba.c:2959-3047
- The function uses a separate memory context (`ident_context`) to ensure proper cleanup if parsing fails
- Unlike some configuration loading functions, this one is designed to be non-fatal - server startup continues even if the ident file cannot be loaded
- The parsed results are stored in global variables `parsed_ident_lines` and `parsed_ident_context` for use by `check_usermap()`
- Error reporting is done at LOG level rather than FATAL level to allow the server to continue operating
- The function supports regular expressions in ident mapping rules through the `parse_ident_line()` helper
- Memory management is carefully handled - if parsing fails, the entire memory context is deleted to prevent leaks