# icu_test

## Location
[src/common/unicode/category_test.c:56-222](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/unicode/category_test.c#L56-L222)

## Overview
Performs comprehensive validation of PostgreSQL's Unicode tables by comparing them against the International Components for Unicode (ICU) library across all Unicode codepoints.

## Definition


## Detailed Description
This static function conducts an exhaustive test of PostgreSQL's Unicode implementation by comparing it with ICU (International Components for Unicode) for every valid Unicode codepoint (0x000000 to 0x10FFFF). The test validates:

1. **General Categories**: Compares PostgreSQL's  function against ICU's 
2. **Unicode Properties**: Tests eight binary properties including Alphabetic, Lowercase, Uppercase, Cased, Case_Ignorable, White_Space, Hex_Digit, and Join_Control
3. **Character Classes**: Validates twelve character classification functions (isalpha, islower, isupper, ispunct, isdigit, isxdigit, isalnum, isspace, isblank, iscntrl, isgraph, isprint)

The function handles Unicode version mismatches gracefully by skipping codepoints that are unassigned in one implementation but assigned in another when there's a version difference. This ensures the test remains meaningful even when PostgreSQL and ICU are using different Unicode versions.

If any discrepancy is found, the function prints detailed diagnostic information and exits with status 1. On successful completion, it reports the number of successfully tested codepoints.

## Parameters / Member Variables
This function takes no parameters but uses several local variables:
- : Counter for successfully tested assigned codepoints
- : Counter for codepoints skipped due to PostgreSQL Unicode version being older
- : Counter for codepoints skipped due to ICU Unicode version being older

## Dependencies
- Functions called/Symbols referenced:
  -  (PostgreSQL Unicode category function)
  -  (ICU character type function)
  -  functions (PostgreSQL Unicode property functions)
  -  (ICU binary property function)
  -  functions (PostgreSQL character class functions)
  -  functions (ICU character class functions)
  -  and  (PostgreSQL category display functions)
  -  and  (standard C library functions)

- Called from (representative examples):
  -  (in category_test.c at line 232)

## Notes and Other Information
- The function is marked as , making it internal to the category_test.c file
- Tests all 1,114,112 possible Unicode codepoints (0x000000 to 0x10FFFF)
- Handles Unicode version mismatches by skipping codepoints unassigned in the older version
- Provides detailed diagnostic output when discrepancies are found, including hexadecimal codepoint values and comparison results
- Part of PostgreSQL's Unicode testing infrastructure, ensuring compatibility with the ICU library
- The test is exhaustive and may take considerable time to complete due to the large number of codepoints tested
- Uses global variables  and  for version comparison logic
- Exits immediately upon finding the first discrepancy to provide fast feedback during development