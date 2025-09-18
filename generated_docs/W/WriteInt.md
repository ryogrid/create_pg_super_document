# WriteInt

## Location
[src/bin/pg_dump/pg_backup_archiver.c:2112-2142](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_archiver.c#L2112-L2142)

## Overview
WriteInt serializes a signed integer to an archive stream in a portable, endian-independent format with explicit sign handling.

## Definition


## Detailed Description
WriteInt is a fundamental serialization function in pg_dump's archiver that writes a signed integer to an archive stream. The function uses a custom binary format that explicitly handles the sign bit to ensure portability across different architectures and integer representations. It first writes a sign byte (1 for negative, 0 for positive), then writes the absolute value in little-endian byte order. This approach avoids dependencies on platform-specific two's complement representation.

## Parameters / Member Variables
- : Archive handle containing the output stream and configuration (including intSize)
- : The signed integer value to be written to the archive

## Dependencies
- Functions called/Symbols referenced:
  - AH->WriteBytePtr (function pointer for writing single bytes)
  - AH->intSize (archive handle member specifying integer size)
- Called from (representative examples):
  - [WriteStr](WriteStr.md) (for string length serialization)
  - [WriteToc](WriteToc.md) (for table of contents serialization)
  - [WriteHead](WriteHead.md) (for archive header serialization)
  - [_StartData](../S/_StartData.md), _EndData, _StartLO, _EndLO (archive section markers)
  - appendByteaLiteralAHX (for bytea literal handling)

## Notes and Other Information
- Returns the total number of bytes written (AH->intSize + 1)
- Uses explicit sign byte followed by little-endian magnitude representation
- Designed to be format-independent and portable across architectures
- Comment warns about maintaining backward compatibility when modifying the format
- Part of pg_dump's custom archive format foundation for integer serialization