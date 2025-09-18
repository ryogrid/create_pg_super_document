# setup_test_matches

## Location
src/test/modules/test_regex/test_regex.c: 435 - 617

## Overview
setup_test_matches is a static function that performs comprehensive regex matching on input text, executing the pattern potentially multiple times and storing all match results in a structured context for later retrieval.

## Definition
static test_regex_ctx *setup_test_matches(text *orig_str, regex_t *cpattern, test_re_flags *re_flags, Oid collation, bool use_subpatterns)

## Detailed Description
This function performs the core regex matching operation by executing a compiled pattern against input text and storing all match results. It handles both single and global matching modes, manages memory efficiently through dynamic allocation, and supports both full pattern matches and subpattern captures.

Key operations include:
1. Converting input text from database encoding to wide characters for regex engine
2. Setting up output storage with dynamic growth for match results  
3. Executing regex pattern repeatedly (if glob flag set) until no more matches
4. Handling zero-length matches by advancing search position
5. Managing subpattern capture when requested
6. Special handling for partial matches when no full matches found
7. Optimizing memory usage for single-byte vs multibyte character sets

The function returns a test_regex_ctx structure containing all match information for subsequent result building.

## Parameters / Member Variables
- : TEXT object containing the input string to search
- : Compiled regex_t pattern to execute  
- : test_re_flags structure controlling matching behavior
- : OID of collation for character classification (currently unused)
- : Boolean indicating whether to capture subpattern matches

## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md)/palloc (PostgreSQL memory allocation)
  - [pg_database_encoding_max_length](../p/pg_database_encoding_max_length.md) (gets max character length)
  - [pg_mb2wchar_with_len](../p/pg_mb2wchar_with_len.md) (converts multibyte to wide characters)
  - [test_re_execute](../t/test_re_execute.md) (executes regex pattern)
  - [repalloc](../r/repalloc.md) (reallocates memory with larger size)
  - [pfree](../p/pfree.md) (PostgreSQL memory deallocation)
  - MaxAllocSize (PostgreSQL memory limit constant)
  - ereport/ERROR (PostgreSQL error reporting)
- Called from (representative examples):
  - [test_regex](../t/test_regex.md) (main regex testing function)

## Notes and Other Information
- This is a static (internal) function within the test_regex module
- Uses dynamic memory allocation with exponential growth (2^n-1 pattern)
- Handles global matching by repeatedly executing until no more matches found
- Correctly advances past zero-length matches to avoid infinite loops
- Optimizes memory usage by keeping wide string only for multibyte encodings
- Supports partial matching details when no full matches are found
- Stores match locations as integer pairs (start, end) for each subpattern
- Located in src/test/modules/test_regex/test_regex.c:435-617