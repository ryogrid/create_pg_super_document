# eqin

## Location
[src/tools/pg_bsd_indent/args.c:233-245](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/tools/pg_bsd_indent/args.c#L233-L245)

## Overview
The eqin function performs prefix matching by checking if one string is a prefix of another and returns a pointer to the remainder of the second string if it matches.

## Definition
```c
static const char *eqin(const char *s1, const char *s2)
```

## Detailed Description
This static utility function implements a prefix matching algorithm commonly used in command-line argument parsing. It compares the characters of s1 against the beginning of s2:

1. Iterates through each character of s1
2. Compares corresponding characters in s2
3. If all characters of s1 match the beginning of s2, returns a pointer to the position in s2 immediately after the matched prefix
4. If any character doesn't match, returns NULL

This function is particularly useful for parsing command-line options where you want to check if an argument starts with a specific prefix (like "--verbose" starting with "--verb") and then process the remainder of the argument.

## Parameters / Member Variables
- `s1`: The prefix string to match against
- `s2`: The target string to check for the prefix

## Dependencies
- Functions called/Symbols referenced:
  - None (uses only basic pointer arithmetic and character comparison)
- Called from (representative examples):
  - [set_option](../s/set_option.md) (src/tools/pg_bsd_indent/args.c:268)

## Notes and Other Information
- Returns NULL if s1 is not a prefix of s2
- Returns a pointer to the remainder of s2 (after the matched prefix) if s1 is a prefix of s2
- The function name "eqin" likely stands for "equal in" or "equal initial"
- This is a static function, meaning it's only accessible within the args.c compilation unit
- The function handles the case where s1 is longer than s2 correctly by returning NULL when s2 is exhausted first
- Commonly used pattern in option parsing where you want to match option prefixes and extract values