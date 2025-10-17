# _PrintData

## Location
[src/bin/pg_dump/pg_backup_custom.c:569-579](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_custom.c#L569-L579)

## Overview
This function prints (outputs) data from the current file position in a PostgreSQL custom format archive, handling decompression transparently.

## Definition
```c
static void _PrintData(ArchiveHandle *AH)
```

## Detailed Description
_PrintData is a utility function in the custom archive format that handles the reading and decompression of data from the current position in the archive file. It creates a compressor state using the archive's compression specification, reads the compressed data through a custom read function, and properly cleans up the compression resources when done. This function abstracts the complexity of handling potentially compressed data streams, allowing callers to focus on higher-level restoration logic.

## Parameters / Member Variables
- `AH`: Archive handle containing the file position, compression settings, and other archive state information

## Dependencies
- Functions called/Symbols referenced:
  - [AllocateCompressor](../A/AllocateCompressor.md)
  - [_CustomReadFunc](../C/_CustomReadFunc.md)
  - [EndCompressor](../E/EndCompressor.md)
  - [CompressorState](../C/CompressorState.md) (type)
- Called from (representative examples):
  - [_PrintTocData](_PrintTocData.md) (for regular data blocks)
  - [_LoadLOs](../L/_LoadLOs.md) (during large object restoration)

## Notes and Other Information
This function is designed to work at the current file position and assumes that the caller has already positioned the file pointer correctly. It handles the full lifecycle of compression state management, from allocation through cleanup, ensuring no resource leaks occur during data restoration operations.

## Simplified Source
```c
static void _PrintData(ArchiveHandle *AH)
{
    // Set up decompressor for current compression settings
    CompressorState *cs = AllocateCompressor(AH->compression_spec, _CustomReadFunc, NULL);

    // Read and decompress data from current file position
    cs->readData(AH, cs);

    // Clean up compression resources
    EndCompressor(AH, cs);
}
```