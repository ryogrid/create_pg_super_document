# check_restrict_nonsystem_relation_kind

## Location
[src/backend/tcop/postgres.c:3702-3751](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/postgres.c#L3702-L3751)

## Overview
A GUC check hook function that validates the format and content of the  configuration parameter, parsing a comma-separated list of relation kinds to be restricted.

## Definition


## Detailed Description
This function serves as a validation hook for the  GUC parameter. It parses a comma-separated string containing relation kind names (such as "view" and "foreign-table") and converts them into internal flag representations. The function performs syntax validation, recognizes valid keywords, and prepares the parsed flags for use by the corresponding assign hook. If validation fails, it provides specific error messages indicating the nature of the problem.

## Parameters / Member Variables
- : Pointer to the new string value to be validated. Contains a comma-separated list of relation kind names.
- : Pointer to store additional context data (parsed flags) for use by the assign function.
- : The source of the configuration change (e.g., configuration file, command line, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - [pstrdup](../p/pstrdup.md): Creates a modifiable copy of the input string
  - SplitIdentifierString: Parses comma-separated identifiers into a list
  - GUC_check_errdetail: Provides detailed error messages for GUC validation failures
  - [pg_strcasecmp](../p/pg_strcasecmp.md): Case-insensitive string comparison
  - [list_free](../l/list_free.md): Frees memory allocated for the list
  - [guc_malloc](../g/guc_malloc.md): Allocates memory in GUC context
  - RESTRICT_RELKIND_VIEW: Flag constant for view restrictions
  - RESTRICT_RELKIND_FOREIGN_TABLE: Flag constant for foreign table restrictions
- Called from (representative examples):
  - GUC system (via function pointer in guc_hooks.h)

## Notes and Other Information
- Currently supports two relation kinds: "view" and "foreign-table"
- The parsed flags are stored in the extra parameter for later use by the assign hook
- Memory management includes proper cleanup of temporary strings and lists on both success and failure paths
- Case-insensitive keyword matching allows flexible user input
- Returns false on any validation error, preventing invalid configurations from being applied