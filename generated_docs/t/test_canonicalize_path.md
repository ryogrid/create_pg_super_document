# test_canonicalize_path

## Location
src/test/regress/regress.c: 542 - 551

## Overview
A PostgreSQL test function that exposes the internal  function for testing purposes, allowing regression tests to verify path canonicalization behavior.

## Definition


## Detailed Description
The  function is a wrapper function designed for PostgreSQL's regression test suite that provides access to the internal  utility function. It takes a text input representing a file path, converts it to a C-string, applies PostgreSQL's path canonicalization logic to normalize the path (removing redundant components like "./" and "../", resolving symbolic links where appropriate), and returns the canonicalized path as a PostgreSQL text value. This function enables comprehensive testing of path handling logic within the PostgreSQL test framework.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that provides access to function call context and arguments
- : The input file path as a C-string to be canonicalized

## Dependencies
- Functions called/Symbols referenced:
  - : Converts PostgreSQL text type to C-string
  - : Macro to extract text argument from function call
  - : Core PostgreSQL function that performs path canonicalization
  - : Converts C-string to PostgreSQL text type
  - : Macro to return a text value from PostgreSQL function
- Called from (representative examples):
  - : Referenced in the same test regression file

## Notes and Other Information
- This is a test utility function located in the PostgreSQL regression test suite
- The function modifies the path in-place via , which is why the same buffer is used for input and output
- Path canonicalization typically involves resolving relative path components, removing redundant separators, and standardizing path format
- The function follows PostgreSQL's V1 calling convention for user-defined functions
- Provides a SQL-accessible interface to test internal path manipulation functionality