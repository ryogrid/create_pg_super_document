# test_regex

## Location
src/test/modules/test_regex/test_regex.c: 80 - 160

## Overview
test_regex is a PostgreSQL set-returning function (SRF) that provides comprehensive regex testing functionality, offering detailed information about pattern matching similar to Tcl's "regexp -about" output.

## Definition
Datum test_regex(PG_FUNCTION_ARGS)

## Detailed Description
This function implements a PostgreSQL set-returning function that takes a regex pattern, input text, and flags as arguments. It returns multiple rows of results: the first row contains information about the compiled regex pattern (equivalent to Tcl's "regexp -about" output), and subsequent rows describe each match found in the input text.

The function operates in two phases:
1. **First call (SRF_IS_FIRSTCALL)**: Compiles the regex pattern, sets up matching context, and returns pattern information
2. **Subsequent calls**: Returns details about each individual match found in the input text

The function uses PostgreSQL's SRF infrastructure to manage state between calls and efficiently return multiple result rows.

## Parameters / Member Variables
-  (pattern): Text containing the regular expression pattern to compile and use
-  (input text): Text to search for matches against the pattern  
-  (flags): Text containing regex compilation and execution flags

## Dependencies
- Functions called/Symbols referenced:
  - SRF_IS_FIRSTCALL (PostgreSQL SRF macro)
  - SRF_FIRSTCALL_INIT (PostgreSQL SRF initialization)
  - SRF_PERCALL_SETUP (PostgreSQL SRF per-call setup)
  - parse_test_flags (parses flag arguments)
  - test_re_compile (compiles regex pattern)
  - setup_test_matches (sets up match execution context)
  - build_test_info_result (builds pattern info result)
  - build_test_match_result (builds individual match result)
  - pg_regfree (frees compiled regex)
  - PG_GET_COLLATION (gets collation for pattern compilation)
- Called from (representative examples):
  - No direct references found (likely called via SQL function interface)

## Notes and Other Information
- This function is part of PostgreSQL's test_regex module for testing regular expression functionality
- Uses PostgreSQL's memory context management for proper cleanup
- Implements the SRF protocol correctly with proper state management
- The first result row provides metadata about the compiled pattern
- Subsequent rows provide detailed information about each match found
- Located in src/test/modules/test_regex/test_regex.c:80-160