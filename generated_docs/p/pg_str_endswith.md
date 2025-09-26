# pg_str_endswith

## Location
src/common/string.c: 32 - 50

## Overview
A utility function that checks whether a given string ends with a specified suffix, commonly used for file extension or pattern matching in PostgreSQL.

## Definition


## Detailed Description
The function determines if the string  has the postfix  by comparing the end portion of the main string with the suffix. It first calculates the lengths of both strings, then performs a direct string comparison on the relevant portion. The implementation is optimized to avoid unnecessary comparisons by checking length constraints first.

The function uses pointer arithmetic to position at the correct starting point in the main string and performs a standard string comparison using .

## Parameters / Member Variables
- : The main string to check for the suffix
- : The suffix string to look for at the end of 

## Dependencies
- Functions called/Symbols referenced:
  - strlen (standard C library function)
  - strcmp (standard C library function)
- Called from (representative examples):
  - StartupReplicationSlots (src/backend/replication/slot.c:1918)
  - decide_file_action (src/bin/pg_rewind/filemap.c:786)

## Notes and Other Information
- Returns  immediately if the suffix is longer than the main string, providing an efficient early exit
- The function is located in src/common/string.c, making it available across different PostgreSQL components
- Commonly used for file extension checking and string pattern matching throughout the PostgreSQL codebase
- The implementation is straightforward and efficient, avoiding memory allocation or complex string manipulation