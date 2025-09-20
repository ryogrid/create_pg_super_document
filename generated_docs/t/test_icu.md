# test_icu

## Location
[src/common/unicode/case_test.c:55-88](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/unicode/case_test.c#L55-L88)

## Overview
An exhaustive test function that compares PostgreSQL's Unicode case mappings with ICU library results across the entire Unicode codepoint range.

## Definition

```c
static void
test_icu(void)
```
## Detailed Description
This function performs a comprehensive validation of PostgreSQL's Unicode case conversion implementation by comparing it against the ICU library across all valid Unicode codepoints (0x0 to 0x10FFFF). The function iterates through the entire Unicode space, filtering out unassigned codepoints in PostgreSQL's implementation, and then cross-checks with ICU to ensure both libraries agree on character categorization. For each valid assigned codepoint, it calls  to perform detailed case conversion testing.

The function tracks statistics including successful tests and skipped mismatches (where PostgreSQL considers a codepoint assigned but ICU considers it unassigned, typically due to Unicode version differences), providing summary information upon completion.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - [unicode_category](../u/unicode_category.md)
  - [icu_test_simple](../i/icu_test_simple.md)
  - u_charType (ICU function)
  - printf
- Constants/Types referenced:
  - [pg_unicode_category](../p/pg_unicode_category.md)
  - PG_U_UNASSIGNED
- Called from (representative examples):
  - [main](../m/main.md)

## Notes and Other Information
- This is a static function, only accessible within the case_test.c compilation unit
- Processes the entire Unicode codepoint space (0x0 to 0x10FFFF), which is over 1 million codepoints
- Handles Unicode version mismatches gracefully by tracking and reporting skipped codepoints
- The function will terminate the program if any case conversion discrepancies are found via 
- Performance note: This is an exhaustive test that may take significant time to complete
- Used as part of PostgreSQL's Unicode compliance testing to ensure comprehensive alignment with ICU standards