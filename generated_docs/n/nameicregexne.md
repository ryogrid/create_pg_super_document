# nameicregexne

## Location
[src/backend/utils/adt/regexp.c:536-549](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regexp.c#L536-L549)

## Overview
A PostgreSQL function that performs case-insensitive regular expression matching for name values, returning true if the pattern does NOT match the name.

## Definition


## Detailed Description
The  function implements the SQL operator  for PostgreSQL's name data type. It takes a name value and a regular expression pattern, performs case-insensitive pattern matching, and returns the negation of the match result. The function uses PostgreSQL's advanced regular expression engine with case-insensitive flags to evaluate whether the given pattern does NOT match the name string.

## Parameters / Member Variables
- : The name value to be tested against the regular expression pattern
- : The regular expression pattern as a text value

## Dependencies
- Functions called/Symbols referenced:
  - : Extracts name argument from function call
  - : Extracts text argument from function call  
  - : Core regex compilation and execution function
  - : Converts Name type to C string
  - : Gets collation information for the operation
  - : Flag for advanced regular expression features
  - : Flag for case-insensitive matching
- Called from (representative examples):
  - No direct references found in the codebase (likely called through SQL operator dispatch)

## Notes and Other Information
- This function implements the negated case-insensitive regex match operator (!~*)
- Uses PostgreSQL's advanced regex engine with case-insensitive matching
- Returns the boolean negation of the regex match result
- Part of PostgreSQL's comprehensive set of regular expression operators for different data types
- The function is typically invoked through SQL expressions rather than direct function calls