# GetIncrementalFileSize

## Location
[src/backend/backup/basebackup_incremental.c:899-920](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_incremental.c#L899-L920)

## Overview
Computes the total size for a complete incremental backup file containing a specified number of blocks, including both header and data sections.

## Definition
size_t GetIncrementalFileSize(unsigned num_blocks_required)

## Detailed Description
This function calculates the total file size needed for an incremental backup file that will store a given number of data blocks. The calculation includes both the header size (computed by GetIncrementalHeaderSize) and the actual block data storage. The header contains metadata including magic number, truncation block length, block count, and block number arrays, while the data section stores the actual block contents.

## Parameters / Member Variables
- num_blocks_required: The number of blocks that the incremental file will contain

## Dependencies
- Functions called/Symbols referenced:
  - [GetIncrementalHeaderSize](GetIncrementalHeaderSize.md) (computes header size)
  - Assert (macro for assertion checking)
  - RELSEG_SIZE (constant defining maximum blocks per segment)
  - BLCKSZ (constant defining block size)
- Called from (representative examples):
  - [sendDir](../s/sendDir.md)

## Notes and Other Information
- The function includes overflow protection by asserting that num_blocks_required doesn't exceed RELSEG_SIZE
- Total file size = header size + (BLCKSZ * number of blocks)
- This is a key component in PostgreSQL's incremental backup size estimation
- Used during backup operations to determine storage requirements before file creation

## Simplified Source

```c
// Calculate total size needed for incremental backup file with specified blocks
extern size_t GetIncrementalFileSize(unsigned num_blocks_required)
{
    size_t result;

    // Prevent overflow - blocks must not exceed segment size
    Assert(num_blocks_required <= RELSEG_SIZE);

    /*
     * Total size = header + block data
     * Header contains: magic number, truncation block length, block count,
     * and array of block numbers (rounded to BLCKSZ boundary)
     * Block data section: BLCKSZ * number of blocks
     */
    result = GetIncrementalHeaderSize(num_blocks_required);
    result += BLCKSZ * num_blocks_required;

    return result;
}
```

**Key Points:**
- Calculates total size for incremental backup file: header + data
- Header size computed by GetIncrementalHeaderSize() includes metadata and block numbers
- Data section size is simply BLCKSZ × number of blocks
- Includes overflow protection via assertion check
- Essential for storage estimation before creating incremental backup files