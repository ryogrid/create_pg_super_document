# xstrcat

## Location
src/bin/psql/copy.c: 78 - 88

## Overview
Concatenates a string to an existing dynamically allocated string, automatically managing memory reallocation and freeing the original string.

## Definition
```c
static void
xstrcat(char **var, const char *more)
```

## Detailed Description
This utility function extends a dynamically allocated string by appending additional content. It creates a new string containing the concatenation of the original string and the new content, frees the original string memory, and updates the pointer to reference the new concatenated string. The function uses psprintf for formatted string creation and handles the memory management automatically.

## Parameters / Member Variables
- `var`: Pointer to a char pointer that holds the original string to be extended
- `more`: Constant string to append to the original string

## Dependencies
- Functions called/Symbols referenced:
  - psprintf (PostgreSQL's sprintf variant)
  - free (standard library)
- Called from (representative examples):
  - parse_slash_copy (multiple locations: lines 114, 128, 129, 141, 142, 155, 160, 172, 173, 181, 182)

## Notes and Other Information
- This is a static function, only accessible within src/bin/psql/copy.c
- Essential utility for building SQL command strings during \copy command parsing
- Automatically handles memory management, preventing memory leaks by freeing the original string
- The function modifies the input pointer to point to the newly allocated concatenated string
- Heavily used in parse_slash_copy to incrementally build the SQL COPY command