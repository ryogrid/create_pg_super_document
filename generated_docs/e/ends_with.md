# ends_with

## Location
[src/bin/psql/tab-complete.c:1635-1650](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/tab-complete.c#L1635-L1650)

## Overview
A simple utility function that checks if a string ends with a specific character, used throughout the tab completion system for string pattern matching.

## Definition
```c
static bool ends_with(const char *s, char c)
```

## Detailed Description
This function provides a straightforward way to test whether a string ends with a particular character. It calculates the string length and checks if the last character matches the provided character. The function includes a safety check to ensure the string is not empty before attempting to access its last character.

This utility is commonly used in psql's tab completion logic to determine command context based on trailing characters like semicolons, commas, or other syntactic markers that affect what completions should be offered.

## Parameters / Member Variables
- `s`: Pointer to the null-terminated string to examine
- `c`: The character to check for at the end of the string

## Dependencies
- Functions called/Symbols referenced:
  - strlen (C standard library function to calculate string length)

- Called from (representative examples):
  - Used extensively in HeadMatchesCS macro calls throughout tab-complete.c at lines 1859, 1867, 1875, 1883, 1912, 2723, 2788, 3171, 3195, 3738, 3754, 3871, 4248, 4506, 4762

## Notes and Other Information
- Returns false for empty strings (length == 0) as a safety measure
- This is a static function, only accessible within the tab-complete.c file
- Simple but essential utility for parsing SQL command syntax during tab completion
- Used primarily in conditional logic to determine appropriate completion contexts
- Performs bounds checking by verifying string length before array access