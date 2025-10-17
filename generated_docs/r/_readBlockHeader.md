# _readBlockHeader

## Location
[src/bin/pg_dump/pg_backup_custom.c:956-987](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_custom.c#L956-L987)

## Overview
This function reads the header of a data block from a custom-format archive file, handling version compatibility between different archive format versions.

## Definition
```c
static void _readBlockHeader(ArchiveHandle *AH, int *type, int *id)
```

## Detailed Description
_readBlockHeader centralizes the logic for reading data block headers from custom-format archive files. The function handles a format change that occurred in archive version 1.3. In pre-1.3 archives, only the block ID was stored (with an implicit BLK_DATA type), while version 1.3 and later archives store both the block type and ID. The function uses getc() to read the block type byte and ReadInt() to read the block ID. For EOF conditions, it sets the type to EOF and ensures the ID is initialized to prevent returning garbage values.

## Parameters / Member Variables
- `AH`: Archive handle containing version information and file handle for reading the archive
- `type`: Output parameter that receives the block type (BLK_DATA, BLK_BLOBS, or EOF)
- `id`: Output parameter that receives the block's dump ID for identification

## Dependencies
- Functions called/Symbols referenced:
  - K_VERS_1_3 (version constant for archive format 1.3)
  - BLK_DATA (constant representing data block type)
  - [ReadInt](../R/ReadInt.md) (function to read integer values from archive)
  - getc (standard library function for reading characters)
- Called from (representative examples):
  - [_PrintTocData](../P/_PrintTocData.md) (to read block headers during restore operations)

## Notes and Other Information
- This is a static function internal to pg_backup_custom.c
- Handles backward compatibility with pre-1.3 archive formats
- For pre-1.3 archives, assumes all blocks are BLK_DATA type
- Sets *type = EOF when end of file is reached during reading
- The function includes a note about potential pg_fatal() calls with very old 7.1 development files
- Part of the custom archive format's block-oriented data organization
- Block types include BLK_DATA (regular data) and BLK_BLOBS (large object data)
- The block ID corresponds to a dump ID that identifies which TOC entry the block belongs to

## Simplified Source

```c
static void
_readBlockHeader(ArchiveHandle *AH, int *type, int *id)
{
    int byte_value;

    // Handle format change in version 1.3
    if (AH->version < K_VERS_1_3) {
        // Pre-1.3: only block ID stored, type is always BLK_DATA
        *type = BLK_DATA;
    } else {
        // Version 1.3+: read block type first
        byte_value = getc(AH->FH);
        *type = byte_value;

        if (byte_value == EOF) {
            *id = 0;  // Initialize to prevent garbage values
            return;
        }
    }

    // Read block ID for both formats
    *id = ReadInt(AH);
}
```