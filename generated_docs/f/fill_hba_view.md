# fill_hba_view

## Location
src/backend/utils/adt/hbafuncs.c: 374 - 429

## Overview
Reads the pg_hba.conf file and processes all authentication rules to fill a tuplestore with view records for the pg_hba_file_rules system view.

## Definition


## Detailed Description
The  function implements the core logic for populating the pg_hba_file_rules system view. It opens and reads the entire pg_hba.conf configuration file, tokenizes all lines, and then parses each valid authentication rule. The function creates a dedicated memory context for parsing operations to ensure proper cleanup, and processes both valid and invalid configuration lines. Each line is converted into a view record through the fill_hba_line function, with valid rules receiving sequential rule numbers while invalid lines are reported with their error messages. The function handles file I/O errors by throwing exceptions rather than trying to represent them as view entries.

## Parameters / Member Variables
- : Tuplestore where all processed HBA rules will be stored as view records
- : Tuple descriptor defining the structure of the pg_hba_file_rules view

## Dependencies
- Functions called/Symbols referenced:
  - open_auth_file
  - tokenize_auth_file
  - AllocSetContextCreate
  - MemoryContextSwitchTo
  - MemoryContextDelete
  - parse_hba_line
  - fill_hba_line
  - free_auth_file
- Types referenced:
  - Tuplestorestate, TupleDesc
  - TokenizedAuthLine, HbaLine
  - HbaFileName (global variable)
  - ALLOCSET_SMALL_SIZES, DEBUG3 (constants)
- Called from:
  - pg_hba_file_rules

## Notes and Other Information
- Function is static and only used within hbafuncs.c for system view implementation
- Creates a dedicated memory context for parsing operations to prevent memory leaks
- Handles both successful parsing and error conditions gracefully
- Assigns sequential rule numbers only to valid HBA rules
- Uses DEBUG3 log level for parsing errors to avoid flooding logs
- File I/O errors result in exceptions rather than view entries
- Memory management includes proper cleanup of both file resources and memory contexts
- Part of PostgreSQL's system view infrastructure for exposing configuration