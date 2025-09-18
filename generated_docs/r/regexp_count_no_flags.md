# regexp_count_no_flags

## Location
src/backend/utils/adt/regexp.c: 1142 - 1151

## Overview
A wrapper function for regexp_count that provides compatibility for function calls without the optional flags parameter, kept separate to avoid opr_sanity regression test complaints.

## Definition


## Detailed Description
This function serves as a simple wrapper around the main regexp_count function. It exists primarily for SQL function overload resolution and to maintain compatibility with PostgreSQL's function dispatch system. The function directly forwards all arguments to regexp_count without any modification or additional processing.

The comment in the source indicates this separation is specifically to keep the opr_sanity regression test from complaining, suggesting this is part of PostgreSQL's internal function organization strategy for handling different function signatures.

## Parameters / Member Variables
- : Standard PostgreSQL function call information structure containing all arguments passed from SQL

## Dependencies
- Functions called/Symbols referenced:
  - [regexp_count](regexp_count.md): The main function that performs the actual regular expression counting logic

- Called from (representative examples):
  - SQL function dispatcher (no direct C code references found)

## Notes and Other Information
- This is a thin wrapper function with no independent logic
- The separation exists purely for PostgreSQL's internal function organization and testing framework compatibility
- All actual functionality is implemented in the regexp_count function
- Part of PostgreSQL's regular expression support in the backend utilities