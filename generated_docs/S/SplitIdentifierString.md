# SplitIdentifierString

## Location
src/backend/utils/adt/varlena.c: 3457 - 3583

## Overview
Parses a string containing identifiers separated by a specified character, handling SQL identifier quoting rules and case conversion for use throughout PostgreSQL's configuration and name parsing.

## Definition

```c
bool
SplitIdentifierString(char *rawstring, char separator,
					  List **namelist)
```
## Detailed Description
The  function is a fundamental parsing utility that splits strings containing multiple identifiers separated by a delimiter (typically '.' or ','). This function serves as the core parsing engine for qualified object names, GUC variable lists, and other configuration strings throughout PostgreSQL.

Key parsing features:
- **Quoted Identifiers**: Handles double-quoted identifiers that preserve case and allow special characters, including quote-quote escape sequences
- **Unquoted Identifiers**: Processes unquoted identifiers with automatic downcasing using PostgreSQL's standard lexer rules
- **Whitespace Handling**: Skips leading and trailing whitespace around identifiers and separators
- **Memory Efficiency**: Modifies the input string in-place to minimize memory allocation, making it suitable for GUC processing
- **Error Detection**: Returns false for syntax errors like mismatched quotes or empty unquoted names

The function is designed to minimize memory allocation, making it suitable for GUC variable processing where memory leaks must be avoided. It modifies the input string in-place and returns pointers into the modified string.

## Parameters / Member Variables
- : Input string to be parsed (must be modifiable; will be overwritten with separated identifiers)
- : Character used to separate identifiers (typically '.' for qualified names or ',' for lists)
- : Output parameter filled with a list of pointers to parsed identifiers within the modified rawstring

## Dependencies
- Functions called/Symbols referenced:
  - : Checks for whitespace characters using PostgreSQL's scanner rules
  - : Converts unquoted identifiers to lowercase with length limits
  - : Ensures identifiers don't exceed maximum length
  - : Finds quote characters in quoted identifiers
  - : Collapses quote-quote sequences
  - : Copies downcased identifier back to original location
  - : String length calculation
  - : Adds parsed identifier to result list

- Called from (representative examples):
  - : Primary caller for qualified object name parsing
  - : Alternative qualified name parsing interface
  - : WAL consistency check configuration
  - : Schema search path processing
  - : Search path validation
  - : Extension configuration parsing
  - : Temporary tablespace configuration
  - : Date style configuration parsing

## Notes and Other Information
- The function is explicitly designed for memory efficiency, crucial for GUC variable processing
- Empty strings are allowed (return true), but empty unquoted identifiers are not
- The input string is destructively modified - callers must pass a copy if original is needed
- Quoted identifiers support the SQL standard quote-quote escape mechanism
- Unquoted identifiers are subject to PostgreSQL's standard identifier length limits
- The function handles both qualified object names (dot-separated) and configuration lists (comma-separated)
- Critical for PostgreSQL's configuration system and SQL object name resolution
- Location: src/backend/utils/adt/varlena.c:3457-3583