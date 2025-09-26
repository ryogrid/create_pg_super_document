# appendBinaryStringInfoNT

## Location
src/common/stringinfo.c: 259 - 288

## Overview
A specialized utility function that appends arbitrary binary data to a StringInfo buffer without ensuring null termination, designed for pure binary data operations.

## Definition
void appendBinaryStringInfoNT(StringInfo str, const void *data, int datalen)

## Detailed Description
appendBinaryStringInfoNT is a specialized variant of appendBinaryStringInfo that appends arbitrary binary data to a StringInfo buffer but deliberately does not add a trailing null byte. The "NT" suffix stands for "No Termination", indicating this function is optimized for pure binary data operations where null termination is unnecessary or undesired.

The function follows the same basic pattern as appendBinaryStringInfo - it validates the StringInfo parameter with an assertion, ensures adequate buffer capacity via enlargeStringInfo, and uses memcpy for efficient data transfer. However, unlike its sibling function, it omits the final null termination step, making it slightly more efficient for scenarios where the caller knows the data is purely binary and null termination would be wasteful.

## Parameters / Member Variables
- str: Target StringInfo buffer to append to
- data: Pointer to the binary data to append (can be any data type)
- datalen: Length of the data to append in bytes

## Dependencies
- Functions called/Symbols referenced:
  - enlargeStringInfo
  - memcpy (standard C library function)
  - Assert (debug assertion macro)
- Called from (representative examples):
  - pq_sendcountedtext
  - pq_sendstring
  - appendStringInfoStringQuoted
  - appendStringInfoCharMacro

## Notes and Other Information
- This is the "no termination" variant of appendBinaryStringInfo
- Part of PostgreSQL's StringInfo utility system located in src/common/stringinfo.c:259-288
- Used primarily in network protocol handling where null termination is not needed
- Slightly more efficient than appendBinaryStringInfo for pure binary operations
- The lack of null termination makes the buffer unsuitable for C string functions after this operation
- Primarily used in PostgreSQL's client-server communication protocol functions
- Should be used carefully as subsequent string operations may fail without proper null termination