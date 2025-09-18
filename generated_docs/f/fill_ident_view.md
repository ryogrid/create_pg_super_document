# fill_ident_view

## Location
src/backend/utils/adt/hbafuncs.c: 521 - 573

## Overview
Reads the pg_ident.conf file and fills a tuplestore with view records for the pg_ident_file_mappings system view.

## Definition


## Detailed Description
This internal function is responsible for parsing PostgreSQL's pg_ident.conf authentication configuration file and populating a tuplestore with the parsed identity mapping entries. The function performs the following operations:

1. Opens the pg_ident.conf file using open_auth_file()
2. Tokenizes the entire file content into structured lines
3. Creates a temporary memory context for parsing operations
4. Iterates through each tokenized line and parses valid identity mapping entries
5. For each line (valid or invalid), creates a tuplestore entry via fill_ident_line()
6. Cleans up memory contexts and file handles

The function handles both valid configuration entries and lines with errors, ensuring that diagnostic information is preserved in the resulting view. Each successfully parsed mapping is assigned an incrementing map_number for identification.

## Parameters / Member Variables
- : Tuplestorestate pointer where the parsed identity mapping records will be stored
- : TupleDesc describing the structure of the target view's tuples

## Dependencies
- Functions called/Symbols referenced:
  - open_auth_file
  - tokenize_auth_file  
  - AllocSetContextCreate
  - parse_ident_line
  - fill_ident_line
  - free_auth_file
  - MemoryContextSwitchTo
  - MemoryContextDelete
- Called from (representative examples):
  - pg_ident_file_mappings

## Notes and Other Information
- This is a static (internal) function used exclusively by the pg_ident_file_mappings SQL function
- Uses a dedicated memory context ('ident parser context') to manage memory for parsing operations
- Handles file access errors by throwing exceptions rather than returning error entries in the view
- Maintains sequential map numbering only for successfully parsed entries (lines with errors don't increment the counter)
- Part of PostgreSQL's Host-Based Authentication (HBA) system for identity mapping between system and database users
- Located in src/backend/utils/adt/hbafuncs.c:521-573