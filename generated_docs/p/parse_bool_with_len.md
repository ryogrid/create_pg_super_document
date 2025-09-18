# parse_bool_with_len

## Location
src/backend/utils/adt/bool.c: 36 - 125

## Overview
Core boolean parsing function that interprets a string of specified length as a boolean value, supporting various boolean representations and their unique prefixes.

## Definition


## Detailed Description
The `parse_bool_with_len` function is the core implementation for boolean string parsing in PostgreSQL. It efficiently parses boolean values by first checking the most commonly used representations through a switch statement on the first character. The function supports case-insensitive matching and accepts unique prefixes of boolean keywords. It uses `pg_strncasecmp` for safe string comparison with the specified length, making it suitable for parsing substrings and non-null-terminated strings. The implementation is optimized for performance by checking frequent cases first.

## Parameters / Member Variables
- `value`: Pointer to the string to be parsed (not required to be null-terminated)
- `len`: Length of the string to parse
- `result`: Pointer to a bool variable where the parsed result will be stored (can be NULL if only validation is needed)

## Dependencies
- Functions called/Symbols referenced:
  - pg_strncasecmp (PostgreSQL's case-insensitive string comparison function)
- Called from (representative examples):
  - parse_bool (bool.c:32)
  - boolin (bool.c:144)

## Notes and Other Information
- Returns true if the string parses successfully as a boolean, false otherwise
- Supported boolean representations: "true", "false", "yes", "no", "on", "off", "1", "0" (case-insensitive)
- Accepts unique prefixes of the above keywords (e.g., "t" for "true", "f" for "false")
- Special handling for 'o' prefix requires at least 2 characters to distinguish between "on" and "off"
- Numeric values "1" and "0" must be exactly one character long
- The function is designed to work with both null-terminated and non-null-terminated strings
- Performance optimized by checking most common cases ('t', 'f', 'y', 'n') first
- Used as the foundation for all boolean parsing operations in PostgreSQL