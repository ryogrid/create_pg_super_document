# get_alternative_expectfile

## Location
[src/test/regress/pg_regress.c:1336-1369](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/regress/pg_regress.c#L1336-L1369)

## Overview
Generates alternative expected output filenames by inserting a numeric suffix before the file extension for PostgreSQL regression test variant expectations.

## Definition


## Detailed Description
This function transforms a given expectfile path by inserting a numeric suffix before the file extension. It's used in PostgreSQL's regression testing framework to handle multiple expected output variants for the same test. For example, if a test might produce different valid outputs on different platforms or configurations, alternative expected files can be provided with numbered suffixes (e.g., , ).

The function allocates memory for the new filename, finds the last dot in the original filename, splits the name at that point, and reconstructs it with the numeric suffix inserted before the extension.

## Parameters / Member Variables
- : The original expected output filename (e.g., "test.out")
- : The numeric suffix to insert (should be between 1-9 based on the comment)

## Dependencies
- Functions called/Symbols referenced:
  - malloc (for memory allocation)
  - strlen, strcpy, strrchr, snprintf (standard string functions)
- Called from (representative examples):
  - [results_differ](../r/results_differ.md) (in src/test/regress/pg_regress.c:1456)

## Notes and Other Information
- Returns a dynamically allocated string that must be freed by the caller
- Returns NULL on memory allocation failure or if no dot is found in the filename
- The numeric suffix is designed to be single-digit (1-9) based on the size calculation
- Used specifically in the regression testing framework to support multiple valid expected outputs for the same test
- Memory management is carefully handled with proper cleanup on allocation failures