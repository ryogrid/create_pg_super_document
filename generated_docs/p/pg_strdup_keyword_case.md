# pg_strdup_keyword_case

## Location
[src/bin/psql/tab-complete.c:5898-5929](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/tab-complete.c#L5898-L5929)

## Overview
Creates a case-converted duplicate of a string based on psql's keyword completion case settings and reference text context.

## Definition
static char *pg_strdup_keyword_case(const char *s, const char *ref)

## Detailed Description
This helper function creates a pg_strdup copy of the input string and converts its case according to the COMP_KEYWORD_CASE setting in psql. The case conversion logic considers the reference text that was already entered by the user to determine appropriate casing. It supports various case preservation modes including lower case, upper case, and preserve modes that maintain consistency with the user's input style.

The function implements PostgreSQL's keyword case completion behavior, ensuring that completed keywords match the user's preferred casing style or the established context of their input.

## Parameters / Member Variables
- s: The source string to duplicate and case-convert
- ref: Reference text already entered by user, used to determine appropriate case conversion

## Dependencies
- Functions called/Symbols referenced:
  - [pg_strdup](pg_strdup.md)
  - [pg_tolower](pg_tolower.md)
  - [pg_toupper](pg_toupper.md)
  - PSQL_COMP_CASE_LOWER
  - PSQL_COMP_CASE_PRESERVE_LOWER
  - PSQL_COMP_CASE_PRESERVE_UPPER
- Called from (representative examples):
  - [create_or_drop_command_generator](../c/create_or_drop_command_generator.md)
  - [_complete_from_query](../c/_complete_from_query.md)
  - [complete_from_list](../c/complete_from_list.md)
  - [complete_from_const](../c/complete_from_const.md)

## Notes and Other Information
The case conversion logic examines the first character of the reference text to determine whether to apply lower or upper case transformation. Different completion case modes (LOWER, PRESERVE_LOWER, PRESERVE_UPPER) affect the conversion behavior. The caller is responsible for freeing the returned string.

## Simplified Source

```c
static char *
pg_strdup_keyword_case(const char *s, const char *ref)
{
    char *ret, *p;
    unsigned char first = ref[0];

    // Create a duplicate of the source string
    ret = pg_strdup(s);

    // Determine if we should convert to lowercase
    if (pset.comp_case == PSQL_COMP_CASE_LOWER ||
        ((pset.comp_case == PSQL_COMP_CASE_PRESERVE_LOWER ||
          pset.comp_case == PSQL_COMP_CASE_PRESERVE_UPPER) && islower(first)) ||
        (pset.comp_case == PSQL_COMP_CASE_PRESERVE_LOWER && !isalpha(first))) {

        // Convert to lowercase
        for (p = ret; *p; p++)
            *p = pg_tolower((unsigned char) *p);
    } else {
        // Convert to uppercase
        for (p = ret; *p; p++)
            *p = pg_toupper((unsigned char) *p);
    }

    return ret;
}
```