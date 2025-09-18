# ecpg_filter_source

## Location
src/interfaces/ecpg/test/pg_regress_ecpg.c: 34 - 92

## Overview
Filters source files by normalizing #line directives, removing path components to make output consistent across different build environments and platforms.

## Definition


## Detailed Description
This function creates a filtered copy of a source file, specifically designed to normalize #line preprocessor directives. It removes path components from file references in #line directives to ensure consistent output regardless of compiler, platform, or build configuration differences. For example, it transforms  into  by stripping the relative path portion.

The function processes the input file line by line, detecting lines that start with "#line " and then removing any leading path components (sequences of '.' and '/') from the quoted filename portion of the directive. This normalization is crucial for regression testing where build paths may vary between environments.

## Parameters / Member Variables
- : Input source file path to be filtered
- : Output file path where the filtered content will be written

## Dependencies
- Functions called/Symbols referenced:
  - fopen (for file I/O operations)
  - pg_get_line_buf (PostgreSQL utility for line reading)
  - initStringInfo, pfree (PostgreSQL string utilities)
  - Standard C string functions (strstr, strchr, memmove, strlen)
- Called from:
  - [ecpg_start_test](ecpg_start_test.md) (main test execution function)

## Notes and Other Information
- This is a static function used internally within the ECPG test framework
- Essential for consistent regression test output across different build environments
- Handles memory management properly using PostgreSQL's StringInfo utilities
- Exits with error code 2 if file operations fail
- Part of the PostgreSQL ECPG (Embedded SQL in C) testing infrastructure located at src/interfaces/ecpg/test/pg_regress_ecpg.c:34-92