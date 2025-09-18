# get_expectfile

## Location
src/test/regress/pg_regress.c: 689 - 717

## Overview
Checks the resultmap to determine if a different expected output file should be used for a specific test and file type.

## Definition
```c
static const char *get_expectfile(const char *testname, const char *file)
```

## Detailed Description
The `get_expectfile` function searches through the loaded resultmap (populated by `load_resultmap`) to find platform-specific expected output files. It extracts the file type from the provided filename by finding the extension after the last dot, then searches the resultmap linked list for an entry that matches both the test name and file type. If a match is found, it returns the alternative expected file path; otherwise, it returns NULL to indicate the default expected file should be used.

This function enables PostgreSQL's regression testing framework to use different expected output files based on the platform, allowing tests to pass on systems where output may legitimately differ due to platform-specific behaviors.

## Parameters / Member Variables
- `testname`: The name of the test to look up in the resultmap
- `file`: The filename from which to extract the file type (extension)

## Dependencies
- Functions called/Symbols referenced:
  - _resultmap (struct type)
  - [test](../t/test.md) (struct member access)
- Called from (representative examples):
  - [results_differ](../r/results_differ.md)

## Notes and Other Information
- Returns NULL if no matching resultmap entry is found or if the file parameter is invalid
- File type is determined by the extension after the last dot in the filename
- Searches the resultmap in the order entries were added (with last-match-wins from load_resultmap)
- Part of the PostgreSQL regression testing framework (pg_regress)
- Used to handle platform-specific differences in test output