# _resultmap

## Location
[src/test/regress/pg_regress.c:42-48](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/regress/pg_regress.c#L42-L48)

## Overview
A linked list structure used by the PostgreSQL regression test framework to map test names and file types to platform-specific expected result files.

## Definition
```c
typedef struct _resultmap
{
    char       *test;
    char       *type;
    char       *resultfile;
    struct _resultmap *next;
} _resultmap;
```

## Detailed Description
The `_resultmap` structure is part of PostgreSQL's regression testing infrastructure (pg_regress). It implements a linked list that stores mappings from test names and file extensions to platform-specific expected result files. This allows the regression test suite to use different expected output files based on the target platform, accommodating platform-specific variations in test output.

The structure is populated by parsing a "resultmap" file that contains entries in the format:
`testname:filetype:platformpattern=substitutefile`

When a test is run, the system can look up the appropriate expected result file based on the test name and file type, enabling platform-specific test result validation.

## Parameters / Member Variables
- `test`: Name of the test case for which this mapping applies
- `type`: File extension/type (e.g., "out", "sql") that this mapping covers  
- `resultfile`: Path to the platform-specific expected result file to use instead of the default
- `next`: Pointer to the next entry in the linked list, allowing multiple mappings to be chained together

## Dependencies
- Functions called/Symbols referenced:
  - (This structure definition does not directly call functions)
  - Referenced as part of self-referential pointer in `next` member

- Used by:
  - `load_resultmap`: Function that populates the linked list by parsing the resultmap file
  - `get_expectfile`: Function that searches through the list to find appropriate expected result files
  - Global variable `resultmap`: Head pointer to the linked list of `_resultmap` entries

## Notes and Other Information
- The linked list is built in reverse order (new entries added at the head) to ensure that later entries in the resultmap file take precedence over earlier ones
- Memory for each entry is allocated using `pg_malloc()` and strings are duplicated using `pg_strdup()`
- This structure is specific to the PostgreSQL regression testing framework and is not used in the main database server code
- The resultmap mechanism allows tests to have different expected outputs on different platforms while maintaining a single test suite