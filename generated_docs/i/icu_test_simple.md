# icu_test_simple

## Location
[src/common/unicode/case_test.c:30-54](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/unicode/case_test.c#L30-L54)

## Overview
A static test function that validates PostgreSQL's Unicode case conversion implementation against ICU library results for a specific Unicode codepoint.

## Definition

```c
static void
icu_test_simple(pg_wchar code)
```
## Detailed Description
This function performs a comparative test between PostgreSQL's internal Unicode case conversion functions and the ICU (International Components for Unicode) library's equivalent functions. It takes a single Unicode codepoint and converts it to lowercase, titlecase, and uppercase using both PostgreSQL's implementation and ICU's implementation, then compares the results. If any discrepancy is found, it prints detailed error information and terminates the program with exit code 1.

The function is part of PostgreSQL's Unicode case conversion testing infrastructure, ensuring that PostgreSQL's Unicode handling remains consistent with the ICU standard.

## Parameters / Member Variables
- `code`: A Unicode codepoint (pg_wchar) to be tested for case conversion accuracy
## Dependencies
- Functions called/Symbols referenced:
  - [unicode_lowercase_simple](../u/unicode_lowercase_simple.md)
  - [unicode_titlecase_simple](../u/unicode_titlecase_simple.md)  
  - [unicode_uppercase_simple](../u/unicode_uppercase_simple.md)
  - u_tolower (ICU function)
  - u_totitle (ICU function)
  - u_toupper (ICU function)
  - printf
  - exit
- Called from (representative examples):
  - [test_icu](../t/test_icu.md)

## Notes and Other Information
- This is a static function, only accessible within the case_test.c compilation unit
- The function performs strict comparison and will terminate the program immediately upon finding any mismatch
- Used as part of PostgreSQL's Unicode compliance testing to ensure alignment with ICU standards
- The function outputs detailed diagnostic information when failures occur, showing both PostgreSQL and ICU results for debugging purposes

## Simplified Source

```c
static void icu_test_simple(pg_wchar code) {
    // Test PostgreSQL's case conversions
    pg_wchar lower = unicode_lowercase_simple(code);
    pg_wchar title = unicode_titlecase_simple(code);
    pg_wchar upper = unicode_uppercase_simple(code);

    // Test ICU's case conversions
    pg_wchar iculower = u_tolower(code);
    pg_wchar icutitle = u_totitle(code);
    pg_wchar icuupper = u_toupper(code);

    // Compare results and fail if any mismatch
    if (lower != iculower || title != icutitle || upper != icuupper) {
        printf("case_test: FAILURE for codepoint 0x%06x\n", code);
        printf("case_test: Postgres lower/title/upper:\t0x%06x/0x%06x/0x%06x\n",
               lower, title, upper);
        printf("case_test: ICU lower/title/upper:\t\t0x%06x/0x%06x/0x%06x\n",
               iculower, icutitle, icuupper);
        printf("\n");
        exit(1);
    }
}
```