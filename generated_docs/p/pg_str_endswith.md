# pg_str_endswith

## Location
[src/common/string.c:32-50](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/string.c#L32-L50)

## Overview
A utility function that checks whether a given string ends with a specified suffix, commonly used for file extension or pattern matching in PostgreSQL.

## Definition

```c
bool
pg_str_endswith(const char *str, const char *end)
```
## Detailed Description
The function determines if the string  has the postfix  by comparing the end portion of the main string with the suffix. It first calculates the lengths of both strings, then performs a direct string comparison on the relevant portion. The implementation is optimized to avoid unnecessary comparisons by checking length constraints first.

The function uses pointer arithmetic to position at the correct starting point in the main string and performs a standard string comparison using .

## Parameters / Member Variables
- `*str`: The main string to check for the suffix
- `*end`: The suffix string to look for at the end of
## Dependencies
- Functions called/Symbols referenced:
  - strlen (standard C library function)
  - strcmp (standard C library function)
- Called from (representative examples):
  - [StartupReplicationSlots](../S/StartupReplicationSlots.md) (src/backend/replication/slot.c:1918)
  - [decide_file_action](../d/decide_file_action.md) (src/bin/pg_rewind/filemap.c:786)

## Notes and Other Information
- Returns  immediately if the suffix is longer than the main string, providing an efficient early exit
- The function is located in src/common/string.c, making it available across different PostgreSQL components
- Commonly used for file extension checking and string pattern matching throughout the PostgreSQL codebase
- The implementation is straightforward and efficient, avoiding memory allocation or complex string manipulation

## Simplified Source

```c
// Simplified version of pg_str_endswith
bool pg_str_endswith(const char *str, const char *end) {
    // Get lengths of both strings
    size_t str_len = strlen(str);
    size_t end_len = strlen(end);

    // Quick check: suffix can't be longer than main string
    if (end_len > str_len)
        return false;

    // Move to the position where suffix should start in main string
    str += str_len - end_len;

    // Compare the end portion with the suffix
    return strcmp(str, end) == 0;
}
```

Key simplifications made:
- Used more descriptive variable names (`str_len`, `end_len` instead of `slen`, `elen`)
- Added clear comments explaining each logical step
- Maintained the exact same algorithm and logic flow
- Preserved all essential functionality including the early exit optimization