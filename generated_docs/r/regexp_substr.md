# regexp_substr

## Location
[src/backend/utils/adt/regexp.c:1858-1945](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regexp.c#L1858-L1945)

## Overview
Returns the substring that matches a regular expression pattern, providing full parameter control for pattern matching position, occurrence number, flags, and subexpression selection.

## Definition


## Detailed Description
The  function is the main implementation for PostgreSQL's REGEXP_SUBSTR SQL function. It extracts and returns a substring from an input text that matches a specified regular expression pattern. The function supports up to 6 parameters:

1. Input text string
2. Regular expression pattern
3. Start position (optional, defaults to 1)
4. Occurrence number (optional, defaults to 1 - first match)
5. Flags for regex behavior (optional)
6. Subexpression number (optional, defaults to 0 - full match)

The function performs comprehensive parameter validation, sets up the regex matching context using , and extracts the appropriate substring based on the specified occurrence and subexpression. It returns NULL if no match is found, if the requested occurrence exceeds available matches, or if the requested subexpression doesn't exist.

## Parameters / Member Variables
-  (text): Input text string to search within
-  (text): Regular expression pattern to match
-  (int32): Start position in string (1-based, optional, defaults to 1)
-  (int32): Which occurrence to return (1-based, optional, defaults to 1)
-  (text): Regex flags (optional, e.g., 'i' for case-insensitive)
-  (int32): Subexpression number (0-based, optional, defaults to 0 for full match)

## Dependencies
- Functions called/Symbols referenced:
  - 
  - 
  - 
  - 
  - 
  - 
  - 
- Called from (representative examples):
  - 
  -  
  - 
  - 

## Notes and Other Information
- The function prohibits the 'g' (global) flag from being specified by users, but internally enables it to find all matches
- Extensive parameter validation ensures start > 0, n > 0, and subexpr >= 0
- Returns NULL for invalid match positions, missing occurrences, or non-existent subexpressions
- The function is located in src/backend/utils/adt/regexp.c:1858-1945
- Other regexp_substr variants delegate to this main function with default parameter values