# charrecv

## Location
src/backend/utils/adt/char.c: 94 - 104

## Overview
Converts a character value from PostgreSQL's external binary format to internal representation by reading a single byte from the message buffer.

## Definition


## Detailed Description
The charrecv function is the binary input (receive) function for PostgreSQL's "char" data type. It is part of the binary protocol support that allows efficient transmission of data between PostgreSQL clients and servers without character set conversion overhead.

The function reads exactly one byte from a StringInfo message buffer using pq_getmsgbyte() and returns it as a character value. No character set conversion is performed, making this suitable for applications that use the "char" type as a 1-byte binary data type rather than for textual characters.

This function is used when data is sent in PostgreSQL's binary format (format code 1) rather than text format (format code 0).

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro containing:
  - : StringInfo pointer to the message buffer containing the binary data

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_POINTER (to extract the StringInfo buffer)
  - [pq_getmsgbyte](../p/pq_getmsgbyte.md) (to read a single byte from the message buffer)
  - PG_RETURN_CHAR (to return the character result)
- Called from (representative examples):
  - PostgreSQL binary protocol handling
  - Client libraries using binary format for "char" type transmission

## Notes and Other Information
- No character set conversion is performed - the byte is used as-is
- This design choice reflects that many applications use "char" as a 1-byte binary type
- The function is part of PostgreSQL's type system infrastructure for binary I/O
- Works in conjunction with charsend() for round-trip binary serialization
- The external binary representation is exactly one byte
- Used by prepared statements and other binary protocol operations