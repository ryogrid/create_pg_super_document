# strip_lineno_from_objdesc

## Location
[src/bin/psql/command.c:5826-5883](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L5826-L5883)

## Overview
Parses and removes a line number suffix from object descriptions used in psql's \ef and \ev commands, returning the extracted line number as an integer.

## Definition
```c
static int strip_lineno_from_objdesc(char *obj)
```

## Detailed Description
This function implements a "kluge" solution for parsing line numbers from the end of object descriptions in psql commands. It's specifically designed to handle cases where \ef (edit function) or \ev (edit view) commands include a line number specification at the end of the object name. The function parses backwards through the string to locate and extract the line number, then removes it from the original string.

The implementation includes careful handling of multibyte character encodings by using isascii() checks before applying ctype.h macros. This backward parsing approach is inherently dangerous in multibyte environments, but the specific bit patterns being searched for are unlikely to occur as non-first bytes of multibyte characters.

## Parameters / Member Variables
- `obj`: A null-terminated string containing the object description that may end with a line number. The string is modified in-place by removing the line number portion.

## Dependencies
- Functions called/Symbols referenced:
  - strlen (standard C library)
  - isascii (standard C library)
  - isspace (standard C library) 
  - isdigit (standard C library)
  - atoi (standard C library)
  - pg_log_error (PostgreSQL logging function)
- Called from (representative examples):
  - [exec_command_ef_ev](../e/exec_command_ef_ev.md) (in src/bin/psql/command.c:1199)

## Notes and Other Information
- Returns -1 if no line number is present in the input
- Returns 0 on parsing error (invalid line number)
- Returns positive integer (the line number) on successful parsing
- The function modifies the input string by null-terminating it before the line number
- Line numbers must be separated from the object name by whitespace or a closing parenthesis
- Line numbers must be positive integers (>= 1)
- The backward parsing approach requires caution in multibyte environments
- This is explicitly described as a "kluge" because psql uses OT_WHOLE_LINE mode for parsing these commands rather than careful argument parsing