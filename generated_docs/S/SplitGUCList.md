# SplitGUCList

## Location
[src/bin/pg_dump/dumputils.c:761-860](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/dumputils.c#L761-L860)

## Overview
SplitGUCList is a utility function that parses a string containing identifiers or file names, specifically designed for splitting the value of a GUC_LIST_QUOTE GUC (Grand Unified Configuration) variable.

## Definition

```c
bool
SplitGUCList(char *rawstring, char separator,
			 char ***namelist)
```
## Detailed Description
This function splits a delimited string into a list of individual components without presuming whether the elements will be treated as identifiers or file names. The function is designed to work with strings that have already been processed through flatten_set_variable_args(), so it never performs downcasing or truncation operations.

The function handles both quoted and unquoted elements:
- Quoted elements can contain any characters and handle quote-quote pairs (doubled quotes are collapsed into single quotes)
- Unquoted elements extend until a separator or whitespace is encountered
- Embedded whitespace is disallowed for simplicity, as it should have led to double-quoting during input processing

The function modifies the input string in-place to contain the separated identifiers and returns a list of pointers into the modified string.

## Parameters / Member Variables
- : The input string that must be overwritable. On return, it's been modified to contain the separated identifiers with null terminators
- : The separator punctuation expected between identifiers (typically '.' or ','). Whitespace may also appear around identifiers
- : Output parameter filled with a palloc'd list of pointers to identifiers within the modified rawstring. Caller should list_free() this even on error return

## Dependencies
- Functions called/Symbols referenced:
  - [scanner_isspace](../s/scanner_isspace.md): Used to skip whitespace characters
  - strchr: Used to find closing quotes in quoted strings
  - memmove: Used to collapse adjacent quotes
  - strlen: Used for string length calculation
  - [lappend](../l/lappend.md): Used to add elements to the output list

- Called from (representative examples):
  - [parse_hba_auth_opt](../p/parse_hba_auth_opt.md): Used in HBA (Host-Based Authentication) configuration parsing
  - [PostmasterMain](../P/PostmasterMain.md): Used in postmaster initialization
  - [check_debug_io_direct](../c/check_debug_io_direct.md): Used in file descriptor management
  - [pg_get_functiondef](../p/pg_get_functiondef.md): Used in rule utilities for function definitions
  - [makeAlterConfigCommand](../m/makeAlterConfigCommand.md): Used in pg_dump utilities
  - [dumpFunc](../d/dumpFunc.md): Used in pg_dump for function dumping

## Notes and Other Information
- The function returns true if parsing is successful, false if there is a syntax error
- Empty strings are allowed and return true with an empty namelist
- There is a duplicate version of this function in src/bin/pg_dump/dumputils.c that should be kept in sync
- The API is intentionally identical to SplitIdentifierString for consistency
- The function is part of the varlena.c module which handles variable-length data types
- Located at src/backend/utils/adt/varlena.c:3705-3793