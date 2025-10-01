# is_separator_char

## Location
[src/backend/utils/adt/formatting.c:1139-1152](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/formatting.c#L1139-L1152)

## Overview
Determines if a character is a separator character, defined as an ASCII printable character that is not a letter or digit.

## Definition
```c
static bool
is_separator_char(const char *str)
```

## Detailed Description
This function checks whether the first character of the input string is a separator character. A separator character is defined as any ASCII printable character (0x21 to 0x7E) that is not an alphabetic letter (A-Z, a-z) or a numeric digit (0-9). This includes punctuation marks, symbols, and special characters that are commonly used as separators in formatting strings.

The function performs multiple range checks:
1. Ensures the character is within printable ASCII range (> 0x20 and < 0x7F)
2. Excludes uppercase letters (A-Z)
3. Excludes lowercase letters (a-z) 
4. Excludes numeric digits (0-9)

Characters like spaces (0x20) and DEL (0x7F) are excluded as they are not considered separator characters in this context.

## Parameters / Member Variables
- `str`: Pointer to the string whose first character is to be checked

## Dependencies
- Functions called/Symbols referenced:
  - None (uses only basic character comparisons)
- Called from (representative examples):
  - DCH_ZONED
  - [parse_format](../p/parse_format.md)
  - [DCH_from_char](../D/DCH_from_char.md)

## Notes and Other Information
- This is a static function, only accessible within formatting.c
- Used in date/time formatting to identify separator characters in format strings
- Returns true for characters like: !, @, #, $, %, ^, &, *, (, ), -, _, +, =, [, ], {, }, |, \, :, ;, ", ', <, >, ,, ., ?, /, ~, `
- Returns false for spaces, control characters, letters, and digits
- The function only examines the first character of the input string

## Simplified Source

```c
static bool
is_separator_char(const char *str)
{
    char c = *str;

    // Must be printable ASCII (excluding space and DEL)
    if (c <= 0x20 || c >= 0x7F)
        return false;

    // Exclude letters and digits
    if ((c >= 'A' && c <= 'Z') ||    // Uppercase letters
        (c >= 'a' && c <= 'z') ||    // Lowercase letters
        (c >= '0' && c <= '9'))      // Digits
        return false;

    return true;  // Valid separator character
}
```