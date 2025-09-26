# appendStringInfoChar

## Location
[src/common/stringinfo.c:194-211](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/stringinfo.c#L194-L211)

## Overview
A utility function that appends a single character to a StringInfo buffer, providing an optimized alternative to using appendStringInfo with a "%c" format specifier.

## Definition
void appendStringInfoChar(StringInfo str, char ch)

## Detailed Description
appendStringInfoChar is a specialized append function in PostgreSQL's StringInfo system that efficiently appends a single character to an existing StringInfo buffer. It serves as an optimized version of calling appendStringInfo(str, "%c", ch), avoiding the overhead of format string parsing when you simply need to append a single character.

The function performs manual buffer management by checking if there's sufficient space (len + 1 >= maxlen) and calling enlargeStringInfo if expansion is needed. After ensuring adequate capacity, it directly writes the character to the buffer at the current length position, increments the length, and maintains null-termination by adding a NUL byte at the new end position.

## Parameters / Member Variables
- str: Target StringInfo buffer to append to
- ch: Single character to append to the buffer

## Dependencies
- Functions called/Symbols referenced:
  - [enlargeStringInfo](../e/enlargeStringInfo.md)
- Called from (representative examples):
  - Not directly referenced by other symbols in the indexed codebase

## Notes and Other Information
- This function is highly optimized for single character operations
- Part of PostgreSQL's StringInfo utility system located in src/common/stringinfo.c:194-211
- Performs inline buffer expansion check rather than delegating to other append functions
- Maintains null-termination by explicitly setting str->data[str->len] = '\0' after appending
- Much faster than the general-purpose appendStringInfo for single character operations
- Directly manipulates StringInfo internal fields (len, maxlen, data) for maximum efficiency