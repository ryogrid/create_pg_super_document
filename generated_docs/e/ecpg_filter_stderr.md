# ecpg_filter_stderr

## Location
[src/interfaces/ecpg/test/pg_regress_ecpg.c:93-147](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/test/pg_regress_ecpg.c#L93-L147)

## Overview
Removes specific connection failure error message details from test result files to ensure consistent output across different environments by filtering out variable host/port information.

## Definition
```c
static void ecpg_filter_stderr(const char *resultfile, const char *tmpfile)
```

## Detailed Description
This function processes ECPG test result files to normalize connection failure error messages. It specifically targets lines containing "connection to server" followed by "failed: " and removes the variable portion (host/port details) between these markers. This normalization ensures that test results remain consistent regardless of the specific connection parameters used during testing.

The function operates by reading the result file line by line, identifying connection error messages, and rewriting them to remove environment-specific details. After processing, it replaces the original result file with the filtered version. This is essential for regression testing where connection details may vary between test environments.

## Parameters / Member Variables
- `resultfile`: Path to the test result file to be filtered (will be overwritten)
- `tmpfile`: Temporary file path used during the filtering process

## Dependencies
- Functions called/Symbols referenced:
  - fopen (for file I/O operations)
  - [pg_get_line_buf](../p/pg_get_line_buf.md) (PostgreSQL utility for line reading)
  - [initStringInfo](../i/initStringInfo.md), pfree (PostgreSQL string utilities)
  - rename (system call for file replacement)
  - Standard C string functions (strstr, memmove, strlen)
- Called from:
  - [ecpg_postprocess_result](ecpg_postprocess_result.md) (test result post-processing function)

## Notes and Other Information
- This is a static function used internally within the ECPG test framework
- Modifies the original result file in-place by using a temporary file and rename operation
- Essential for maintaining consistent test results across different database connection configurations
- Exits with error code 2 if file operations fail
- The comment suggests potential future unification with ecpg_filter_source for a more general pattern matching system
- Part of the PostgreSQL ECPG testing infrastructure located at src/interfaces/ecpg/test/pg_regress_ecpg.c:93-147