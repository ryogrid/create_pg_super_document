# WriteStr

## Location
[src/bin/pg_dump/pg_backup_archiver.c:2170-2188](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_archiver.c#L2170-L2188)

## Overview
WriteStr serializes a C string to an archive stream with length-prefixed format, handling NULL strings appropriately.

## Definition

```c
struct stat st;
```
## Detailed Description
WriteStr serializes a C string to an archive stream using a length-prefixed format. For non-NULL strings, it first writes the string length as an integer using WriteInt, followed by the string content using the archive's bulk write function. For NULL strings, it writes -1 as the length indicator to distinguish them from empty strings. This design ensures that NULL strings can be properly reconstructed during deserialization.

## Parameters / Member Variables
- : Archive handle containing the output stream and function pointers for writing
- : Pointer to the C string to be written (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [WriteInt](WriteInt.md) (for writing the string length or NULL indicator)
  - strlen (for calculating string length)
  - AH->WriteBufPtr (function pointer for writing buffer data)
- Called from (representative examples):
  - [WriteToc](WriteToc.md) (extensively used for table of contents string fields)
  - [WriteHead](WriteHead.md) (for archive header string fields)
  - appendByteaLiteralAHX (for bytea literal handling)
  - [_WriteExtraToc](_WriteExtraToc.md) (across multiple archive format implementations)

## Notes and Other Information
- Returns the total number of bytes written (length + string content)
- Uses WriteInt for length encoding, ensuring portable integer serialization
- NULL strings are encoded as length -1 to distinguish from empty strings
- Empty strings (length 0) are handled correctly with zero-length content
- Part of pg_dump's string serialization foundation used throughout archive formats