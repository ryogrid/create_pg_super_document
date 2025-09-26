# appendStringInfoString

## Location
src/common/stringinfo.c: 182 - 193

## Overview
A utility function that appends a null-terminated string to a StringInfo buffer, providing an optimized alternative to using appendStringInfo with a "%s" format specifier.

## Definition
void appendStringInfoString(StringInfo str, const char *s)

## Detailed Description
appendStringInfoString is a specialized append function in PostgreSQL's StringInfo system that efficiently appends a null-terminated string to an existing StringInfo buffer. It serves as an optimized version of calling appendStringInfo(str, "%s", s), avoiding the overhead of format string parsing when you simply need to concatenate a string.

The function internally delegates to appendBinaryStringInfo, first calculating the string length using strlen() and then performing the binary append operation. This approach maintains the StringInfo buffer's automatic memory management while providing better performance than the general-purpose formatted append functions.

## Parameters / Member Variables
- str: Target StringInfo buffer to append to
- s: Null-terminated C string to append to the buffer

## Dependencies
- Functions called/Symbols referenced:
  - appendBinaryStringInfo
  - strlen (standard C library function)
- Called from (representative examples):
  - Not directly referenced by other symbols in the indexed codebase

## Notes and Other Information
- This function is optimized for performance when appending plain strings without formatting
- Part of PostgreSQL's StringInfo utility system located in src/common/stringinfo.c:182-193
- Automatically handles buffer expansion if the current buffer is too small
- The input string must be null-terminated for strlen() to work correctly
- More efficient than appendStringInfo when no format specifiers are needed