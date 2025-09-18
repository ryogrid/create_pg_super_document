# CopyGetAttnums

## Location
[src/backend/commands/copy.c:896-970](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/copy.c#L896-L970)

## Overview
CopyGetAttnums builds an integer list of attribute numbers (column numbers) to be copied during PostgreSQL COPY operations, either from a user-specified column list or generating a default list of all non-dropped, non-generated columns.

## Definition


## Detailed Description
CopyGetAttnums is a utility function in PostgreSQL's COPY command implementation that determines which columns should be included in COPY operations. The function handles two scenarios:

1. **Default column selection**: When no column list is specified (attnamelist is NIL), it automatically generates a list containing all non-dropped and non-generated columns from the table's tuple descriptor.

2. **Explicit column validation**: When a column list is provided, it validates each column name against the table schema, ensuring:
   - The column exists in the table
   - The column is not dropped
   - The column is not generated (generated columns are explicitly forbidden in COPY operations)
   - No duplicate columns are specified

The function enforces PostgreSQL's policy that generated columns cannot be used in COPY operations, ensuring that anything copied out can be copied back in. This restriction applies to both COPY FROM and COPY TO operations.

## Parameters / Member Variables
- : Tuple descriptor containing metadata about the table's columns, including attribute names, types, and flags
- : Relation (table) object used primarily for error reporting; can be NULL for anonymous operations
- : List of column names specified by the user, or NIL to select all eligible columns automatically

## Dependencies
- Functions called/Symbols referenced:
  - : Appends integer values to the result list
  - : Compares PostgreSQL Name objects with C strings
  - : Checks if an integer is already in the list (for duplicate detection)
  - : Constant representing an invalid attribute number
  - : Macro to access tuple descriptor attributes
  - : PostgreSQL's error reporting mechanism

- Called from (representative examples):
  - : Main COPY command handler in src/backend/commands/copy.c:155
  - : Initializes COPY FROM operations in src/backend/commands/copyfrom.c (multiple calls)
  - : Initializes COPY TO operations in src/backend/commands/copyto.c (multiple calls)

## Notes and Other Information
- The function returns attribute numbers (1-based indexing) rather than attribute indexes (0-based)
- Generated columns are systematically excluded to maintain data consistency between COPY FROM and COPY TO operations
- Error handling provides detailed messages including column and relation names when available
- The function is central to PostgreSQL's COPY infrastructure and is used by both input (FROM) and output (TO) operations
- Column validation includes checking for dropped columns (attisdropped) and generated columns (attgenerated)
- The rel parameter being optional (can be NULL) allows the function to work with anonymous tuple descriptors for more generic use cases