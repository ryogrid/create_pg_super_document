# make_incremental_rfile

## Location
[src/bin/pg_combinebackup/reconstruct.c:455-509](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_combinebackup/reconstruct.c#L455-L509)

## Overview
Initializes and reads the header of an incremental backup file, creating an rfile structure with metadata about which blocks it contains.

## Definition

```c
static rfile *
make_incremental_rfile(char *filename)
```
## Detailed Description
This function creates an rfile structure for an incremental backup file by reading and parsing its header. The incremental file format includes a magic number for validation, the number of blocks contained in the file, the truncation block length (indicating the original file size), and an array of block numbers that specify which blocks are present in the incremental file.

The function performs validation on the magic number and ensures that block counts and truncation lengths don't exceed PostgreSQL's segment size limits. It also calculates the header length and aligns it to block boundaries to ensure proper data alignment for subsequent block reading operations.

## Parameters / Member Variables
- `filename`: Path to the incremental backup file to initialize

## Dependencies
- Functions called/Symbols referenced:
  - [make_rfile](make_rfile.md)
  - [read_bytes](../r/read_bytes.md)
  - [pg_malloc0](../p/pg_malloc0.md)
  - [pg_fatal](../p/pg_fatal.md)
  - INCREMENTAL_MAGIC
  - RELSEG_SIZE
  - BlockNumber
  - BLCKSZ
- Called from (representative examples):
  - [reconstruct_from_incremental_file](../r/reconstruct_from_incremental_file.md)

## Notes and Other Information
The function validates the incremental file format using a magic number and enforces PostgreSQL's segment size constraints. Header length is aligned to BLCKSZ boundaries only when the file contains actual block data, optimizing for both alignment requirements and storage efficiency. The resulting rfile structure contains all necessary metadata for subsequent block extraction operations during reconstruction.

## Simplified Source

```c
static rfile *make_incremental_rfile(char *filename)
{
    rfile *rf;
    unsigned magic;

    // Create basic rfile structure
    rf = make_rfile(filename, false);

    // Validate incremental file magic number
    read_bytes(rf, &magic, sizeof(magic));
    if (magic != INCREMENTAL_MAGIC)
        pg_fatal("file \"%s\" has bad incremental magic number (0x%x, expected 0x%x)",
                filename, magic, INCREMENTAL_MAGIC);

    // Read and validate block count
    read_bytes(rf, &rf->num_blocks, sizeof(rf->num_blocks));
    if (rf->num_blocks > RELSEG_SIZE)
        pg_fatal("file \"%s\" has block count %u in excess of segment size %u",
                filename, rf->num_blocks, RELSEG_SIZE);

    // Read and validate truncation block length
    read_bytes(rf, &rf->truncation_block_length, sizeof(rf->truncation_block_length));
    if (rf->truncation_block_length > RELSEG_SIZE)
        pg_fatal("file \"%s\" has truncation block length %u in excess of segment size %u",
                filename, rf->truncation_block_length, RELSEG_SIZE);

    // Read block numbers array if present
    if (rf->num_blocks > 0) {
        rf->relative_block_numbers = pg_malloc0(sizeof(BlockNumber) * rf->num_blocks);
        read_bytes(rf, rf->relative_block_numbers, sizeof(BlockNumber) * rf->num_blocks);
    }

    // Calculate header length
    rf->header_length = sizeof(magic) + sizeof(rf->num_blocks) +
                       sizeof(rf->truncation_block_length) +
                       sizeof(BlockNumber) * rf->num_blocks;

    // Align header to block boundaries if file contains blocks
    if (rf->num_blocks > 0 && (rf->header_length % BLCKSZ) != 0)
        rf->header_length += (BLCKSZ - (rf->header_length % BLCKSZ));

    return rf;
}
```