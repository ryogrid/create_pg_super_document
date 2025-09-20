# strbncmp

## Location
[src/backend/tsearch/spell.c:280-310](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/spell.c#L280-L310)

## Overview
A static function that performs reverse string comparison for a specified number of characters, comparing strings from their end towards the beginning.

## Definition

```c
static int
strbncmp(const unsigned char *s1, const unsigned char *s2, size_t count)
```
## Detailed Description
The  function implements a specialized string comparison algorithm that compares two strings in reverse order (from the end towards the beginning) for up to  characters. This function is particularly useful in spell-checking contexts where suffix comparison is needed, such as when sorting affixes by their endings.

The function starts from the last character of both strings and moves backwards, comparing character by character. If the strings differ in length after comparing  characters, the shorter string is considered "less than" the longer one.

## Parameters / Member Variables
- : First string to compare (unsigned char pointer)
- : Second string to compare (unsigned char pointer) 
- : Maximum number of characters to compare from the end

## Dependencies
- Functions called/Symbols referenced:
  - strlen (standard C library function)
- Called from (representative examples):
  - [NISortAffixes](../N/NISortAffixes.md)

## Notes and Other Information
- Returns -1 if s1 < s2, 1 if s1 > s2, and 0 if they are equal
- The comparison is done in reverse order (from end to beginning)
- This function is specifically designed for affix sorting in the spell-checking module
- Located in src/backend/tsearch/spell.c:280-310