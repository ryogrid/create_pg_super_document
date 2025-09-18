# free_copy_options

## Location
src/bin/psql/copy.c: 65 - 77

## Overview
Frees memory allocated for a copy_options structure and its dynamically allocated members.

## Definition


## Detailed Description
This function performs cleanup by freeing all dynamically allocated memory within a copy_options structure. It safely handles NULL pointers and frees the individual string members (before_tofrom, after_tofrom, file) before freeing the structure itself. This function is essential for preventing memory leaks in psql's \copy command implementation.

## Parameters / Member Variables
- : Pointer to the copy_options structure to be freed. Can be NULL (function returns early in this case).

## Dependencies
- Functions called/Symbols referenced:
  - free (standard library)
  - copy_options (struct definition)
- Called from (representative examples):
  - parse_slash_copy (src/bin/psql/copy.c:256)
  - do_copy (src/bin/psql/copy.c:331, 352, 406)

## Notes and Other Information
- This is a static function, only accessible within src/bin/psql/copy.c
- Follows defensive programming practices by checking for NULL pointer before proceeding
- Part of the memory management infrastructure for psql's \copy command parsing and execution
- The function frees three string members: before_tofrom, after_tofrom, and file, which contain the parsed components of the \copy command