# results_differ

## Location
[src/test/regress/pg_regress.c:1402-1547](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/regress/pg_regress.c#L1402-L1547)

## Overview
Compares actual test results against expected output files, trying multiple alternative expected files and returning whether differences exist, while generating diff output for failed comparisons.

## Definition

```c
static bool
results_differ(const char *testname, const char *resultsfile, const char *default_expectfile)
```
## Detailed Description
This function is the core comparison engine for PostgreSQL's regression testing framework. It systematically compares actual test output against expected results, implementing a sophisticated fallback strategy to handle platform-specific variations and multiple valid expected outputs. The function first tries platform-specific expected files, then attempts up to 10 alternative expected files (numbered 0-9), and finally falls back to the default expected file. For each comparison, it tracks which produces the smallest diff (indicating the best match) and uses that for the final report. When differences are found, it appends a formatted diff to the global diffs file for review.

## Parameters / Member Variables
- : The name of the test being compared (used for platform-specific expectfile lookup)
- : Path to the actual test output file
- : Path to the primary expected output file

## Dependencies
- Functions called/Symbols referenced:
  - [get_expectfile](../g/get_expectfile.md) (platform-specific expected file lookup)
  - [get_alternative_expectfile](../g/get_alternative_expectfile.md) (numbered alternative file generation)
  - [run_diff](run_diff.md) (diff command execution)
  - [file_line_count](../f/file_line_count.md), file_exists (file utility functions)
  - strlcpy, unlink, fopen (system functions)
  - bail (error handling)
- Called from (representative examples):
  - [run_single_test](run_single_test.md) (in src/test/regress/pg_regress.c:1879)
  - Referenced in MAX_PARALLEL_TESTS context (src/test/regress/pg_regress.c:1801)

## Notes and Other Information
- Returns false for matching results (test passed), true for differences (test failed)
- Implements intelligent diff comparison by selecting the expected file that produces the smallest diff
- Supports up to 10 alternative expected files per test (suffixed _0 through _9)
- Handles platform-specific expected files through get_expectfile()
- Automatically cleans up temporary diff files when tests pass
- Appends formatted diffs to the global difffilename for failed tests
- Uses both basic_diff_opts for comparison and pretty_diff_opts for final output
- Critical component of PostgreSQL's regression testing infrastructure that enables cross-platform testing with multiple valid expected outputs