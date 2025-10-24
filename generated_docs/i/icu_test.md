# icu_test

## Location
[src/common/unicode/category_test.c:56-222](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/unicode/category_test.c#L56-L222)

## Overview
Performs comprehensive validation of PostgreSQL's Unicode tables by comparing them against the International Components for Unicode (ICU) library across all Unicode codepoints.

## Definition

```c
static void
icu_test()
```
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

## Simplified Source

```c
static void icu_test() {
    int successful = 0;
    int pg_skipped_codepoints = 0;
    int icu_skipped_codepoints = 0;

    // Test all Unicode codepoints from 0 to 0x10FFFF
    for (pg_wchar code = 0; code <= 0x10ffff; code++) {
        uint8_t pg_category = unicode_category(code);
        uint8_t icu_category = u_charType(code);

        // Get property flags from both PostgreSQL and ICU
        bool prop_alphabetic = pg_u_prop_alphabetic(code);
        bool prop_lowercase = pg_u_prop_lowercase(code);
        bool prop_uppercase = pg_u_prop_uppercase(code);
        // ... other properties

        bool icu_prop_alphabetic = u_hasBinaryProperty(code, UCHAR_ALPHABETIC);
        bool icu_prop_lowercase = u_hasBinaryProperty(code, UCHAR_LOWERCASE);
        bool icu_prop_uppercase = u_hasBinaryProperty(code, UCHAR_UPPERCASE);
        // ... other ICU properties

        // Get character class results from both implementations
        bool isalpha = pg_u_isalpha(code);
        bool icu_isalpha = u_isUAlphabetic(code);
        // ... other character classes

        // Handle version mismatches by skipping unassigned codepoints
        if (pg_category == PG_U_UNASSIGNED && icu_category != PG_U_UNASSIGNED &&
            pg_unicode_version < icu_unicode_version) {
            pg_skipped_codepoints++;
            continue;
        }

        if (icu_category == PG_U_UNASSIGNED && pg_category != PG_U_UNASSIGNED &&
            icu_unicode_version < pg_unicode_version) {
            icu_skipped_codepoints++;
            continue;
        }

        // Verify category matches
        if (pg_category != icu_category) {
            printf("category_test: FAILURE for codepoint 0x%06x\n", code);
            exit(1);
        }

        // Verify property matches
        if (prop_alphabetic != icu_prop_alphabetic ||
            prop_lowercase != icu_prop_lowercase ||
            prop_uppercase != icu_prop_uppercase /* ... other property checks */) {
            printf("category_test: FAILURE for codepoint 0x%06x\n", code);
            exit(1);
        }

        // Verify character class matches
        if (isalpha != icu_isalpha /* ... other class checks */) {
            printf("category_test: FAILURE for codepoint 0x%06x\n", code);
            exit(1);
        }

        if (pg_category != PG_U_UNASSIGNED)
            successful++;
    }

    // Report results
    if (pg_skipped_codepoints > 0)
        printf("category_test: skipped %d codepoints unassigned in Postgres\n", pg_skipped_codepoints);
    if (icu_skipped_codepoints > 0)
        printf("category_test: skipped %d codepoints unassigned in ICU\n", icu_skipped_codepoints);

    printf("category_test: ICU test: %d codepoints successful\n", successful);
}
```