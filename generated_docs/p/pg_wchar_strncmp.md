# pg_wchar_strncmp

## Location
[src/backend/utils/mb/wstrncmp.c:40-54](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/wstrncmp.c#L40-L54)

## Overview
Compares at most n wide characters of two wide character strings, returning an integer indicating their lexicographic relationship.

## Definition


## Detailed Description
This function performs a lexicographic comparison of two wide character strings (pg_wchar), comparing at most n characters. It follows the standard strncmp semantics but operates on PostgreSQL's wide character type (pg_wchar) rather than regular char arrays. The function stops comparison when it encounters differing characters, reaches a null terminator, or has compared n characters.

The function returns:
- A negative value if s1 is lexicographically less than s2
- Zero if the strings are equal (up to n characters)
- A positive value if s1 is lexicographically greater than s2

## Parameters / Member Variables
- : Pointer to the first wide character string to compare
- : Pointer to the second wide character string to compare  
- : Maximum number of wide characters to compare

## Dependencies
- Functions called/Symbols referenced: None (implements basic comparison logic)
- Called from: No direct references found in the indexed codebase

## Notes and Other Information
- This is a utility function for wide character string operations in PostgreSQL's multibyte character support
- The function is implemented in src/backend/utils/mb/wstrncmp.c as part of PostgreSQL's multibyte/wide character handling infrastructure
- Follows standard C library strncmp semantics but adapted for pg_wchar type
- The implementation is optimized for efficiency with a do-while loop that avoids unnecessary iterations