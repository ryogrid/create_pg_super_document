# ReadOffset

## Location
[src/bin/pg_dump/pg_backup_archiver.c:2048-2111](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_archiver.c#L2048-L2111)

## Overview
ReadOffset deserializes a PostgreSQL file offset (pgoff_t) from an archive stream, handling both legacy and current archive format versions.

## Definition

```c
int
ReadOffset(ArchiveHandle *AH, pgoff_t * o)
```
## Detailed Description
ReadOffset is the counterpart to WriteOffset, reading a PostgreSQL file offset from an archive stream. The function handles backward compatibility with older archive versions (pre-1.7) that stored offsets as integers using ReadInt. For newer versions, it reads a flag byte followed by the offset data in little-endian format. The function includes validation to ensure the offset data is within expected bounds and returns a status code indicating the offset state.

## Parameters / Member Variables
- `*AH`: Archive handle containing the input stream and function pointers for reading
- `*o`: Pointer to pgoff_t where the read offset value will be stored
## Dependencies
- Functions called/Symbols referenced:
  - [ReadInt](ReadInt.md) (for backward compatibility with older archive versions)
  - pgoff_t (PostgreSQL offset type)
  - K_VERS_1_7, K_OFFSET_POS_NOT_SET, K_OFFSET_NO_DATA, K_OFFSET_POS_SET (archive format constants)
  - AH->ReadBytePtr (function pointer for reading single bytes)
  - [pg_fatal](../p/pg_fatal.md) (error reporting function)
- Called from (representative examples):
  - appendByteaLiteralAHX
  - [_ReadExtraToc](_ReadExtraToc.md)

## Notes and Other Information
- Returns status flags: K_OFFSET_POS_SET, K_OFFSET_POS_NOT_SET, or K_OFFSET_NO_DATA
- Handles backward compatibility with archive versions < 1.7 which used ReadInt for offsets
- Validates offset data and fails with pg_fatal if offset is too large for the platform
- Reconstructs little-endian serialized data back into native pgoff_t format
- Part of pg_dump's custom archive format deserialization system

## Simplified Source

```c
int ReadOffset(ArchiveHandle *AH, pgoff_t *o) {
    int offsetFlg;

    // Initialize output to zero
    *o = 0;

    // Handle legacy archive versions (< 1.7)
    if (AH->version < K_VERS_1_7) {
        int i = ReadInt(AH);
        if (i < 0) return K_OFFSET_POS_NOT_SET;
        if (i == 0) return K_OFFSET_NO_DATA;
        *o = (pgoff_t) i;
        return K_OFFSET_POS_SET;
    }

    // Read offset state flag from archive
    offsetFlg = AH->ReadBytePtr(AH) & 0xFF;

    // Validate offset flag
    if (offsetFlg != K_OFFSET_POS_NOT_SET &&
        offsetFlg != K_OFFSET_NO_DATA &&
        offsetFlg != K_OFFSET_POS_SET) {
        pg_fatal("unexpected data offset flag %d", offsetFlg);
    }

    // Read offset bytes in little-endian format
    for (int off = 0; off < AH->offSize; off++) {
        if (off < sizeof(pgoff_t)) {
            *o |= ((pgoff_t)(AH->ReadBytePtr(AH))) << (off * 8);
        } else {
            // Ensure no overflow beyond pgoff_t size
            if (AH->ReadBytePtr(AH) != 0) {
                pg_fatal("file offset in dump file is too large");
            }
        }
    }

    return offsetFlg;
}
```