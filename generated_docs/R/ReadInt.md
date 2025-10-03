# ReadInt

## Location
[src/bin/pg_dump/pg_backup_archiver.c:2143-2169](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_archiver.c#L2143-L2169)

## Overview
ReadInt deserializes a signed integer from an archive stream, handling both legacy and current archive format versions with explicit sign bit processing.

## Definition

```c
int
ReadInt(ArchiveHandle *AH)
```
## Detailed Description
ReadInt is the counterpart to WriteInt, deserializing a signed integer from an archive stream. The function handles backward compatibility with older archive versions (1.0 and earlier) that did not include a sign byte. For newer versions, it reads an explicit sign byte followed by the magnitude in little-endian byte order, then applies the sign to reconstruct the original integer value. The function reconstructs the integer by accumulating bytes with appropriate bit shifting.

## Parameters / Member Variables
- `*AH`: Archive handle containing the input stream and configuration (including version and intSize)
## Dependencies
- Functions called/Symbols referenced:
  - K_VERS_1_0 (archive version constant for backward compatibility)
  - AH->ReadBytePtr (function pointer for reading single bytes)
  - AH->version, AH->intSize (archive handle members)
- Called from (representative examples):
  - [ReadOffset](ReadOffset.md) (for backward compatibility with older offset formats)
  - [ReadStr](ReadStr.md) (for string length deserialization)
  - [ReadToc](ReadToc.md) (for table of contents deserialization)
  - [ReadHead](ReadHead.md) (for archive header deserialization)
  - [_ReadExtraToc](_ReadExtraToc.md), _LoadLOs, _skipLOs, _skipData (archive processing functions)
  - appendByteaLiteralAHX (for bytea literal handling)

## Notes and Other Information
- Returns the reconstructed signed integer value
- Handles backward compatibility with archive versions ≤ 1.0 (no sign byte)
- Uses little-endian byte reconstruction with explicit sign handling
- Accumulates byte values using bit shifting to rebuild the original magnitude
- Part of pg_dump's custom archive format foundation for integer deserialization