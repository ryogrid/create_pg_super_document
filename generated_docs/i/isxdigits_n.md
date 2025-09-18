# isxdigits_n

## Location
src/backend/utils/adt/varlena.c: 6462 - 6471

## Overview
A utility function that checks whether the first n characters of a string are all valid hexadecimal digits.

## Definition
```c
static bool isxdigits_n(const char *instr, size_t n)
```

## Detailed Description
The `isxdigits_n` function is a static utility function that validates whether the first n characters of a given string are all valid hexadecimal digits (0-9, a-f, A-F). It iterates through the specified number of characters and uses the standard C library `isxdigit()` function to check each character. The function returns false immediately upon encountering the first non-hexadecimal character, or true if all n characters are valid hexadecimal digits. This function is typically used for parsing and validation of hexadecimal sequences in larger string processing operations.

## Parameters / Member Variables
- `instr`: A `const char*` pointer to the input string to be checked
- `n`: A `size_t` value specifying the number of characters to check from the beginning of the string

## Dependencies
- Functions called/Symbols referenced:
  - isxdigit (standard C library function)
- Called from:
  - unistr (multiple references at lines 6530, 6531, 6567, 6602)

## Notes and Other Information
- This is a static function, meaning it has internal linkage and is only accessible within the same translation unit
- The function casts characters to unsigned char before passing to isxdigit() to avoid undefined behavior with negative char values
- Used primarily by the unistr function for parsing Unicode escape sequences in hexadecimal format
- The function provides early termination on the first invalid character for efficiency
- No bounds checking is performed on the input string; caller must ensure string has at least n characters