# WriteOffset

## Location
[src/bin/pg_dump/pg_backup_archiver.c:2031-2047](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_archiver.c#L2031-L2047)

## Overview
WriteOffset serializes a PostgreSQL file offset (pgoff_t) to an archive stream in a portable, endian-independent format.

## Definition


## Detailed Description
WriteOffset is a utility function in pg_dump's archiver module that writes a PostgreSQL file offset to an archive stream. The function ensures portability across different architectures by writing the offset in little-endian byte order (smallest byte first). It first writes a flag indicating whether the offset was set, then serializes the pgoff_t value byte by byte to prevent endian mismatch issues when the archive is read on different systems.

## Parameters / Member Variables
- : Archive handle containing the output stream and function pointers for writing
- : The pgoff_t offset value to be written to the archive
- : Flag indicating whether the offset value was previously set (written first as a single byte)

## Dependencies
- Functions called/Symbols referenced:
  - pgoff_t (PostgreSQL offset type)
  - AH->WriteBytePtr (function pointer for writing single bytes)
- Called from (representative examples):
  - appendByteaLiteralAHX
  - [_WriteExtraToc](_WriteExtraToc.md)

## Notes and Other Information
- Returns the total number of bytes written (sizeof(pgoff_t) + 1)
- Uses little-endian serialization to ensure cross-platform compatibility
- The wasSet flag allows the reader to distinguish between valid zero offsets and unset offsets
- Part of pg_dump's custom archive format implementation