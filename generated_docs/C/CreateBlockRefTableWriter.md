# CreateBlockRefTableWriter

## Location
[src/common/blkreftable.c:790-816](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/blkreftable.c#L790-L816)

## Overview
CreateBlockRefTableWriter initializes a new BlockRefTableWriter for writing block reference table data incrementally to disk, setting up the necessary I/O callbacks and writing the magic number header.

## Definition
```c
BlockRefTableWriter *CreateBlockRefTableWriter(io_callback_fn write_callback, void *write_callback_arg)
```

## Detailed Description
This function creates and initializes a BlockRefTableWriter structure that allows incremental writing of block reference table data to disk. The function allocates memory for the writer structure, sets up the I/O callback functions provided by the caller, initializes a CRC32C checksum for data integrity, and writes the BLOCKREFTABLE_MAGIC header to mark the beginning of a block reference table file.

The writer is designed to work with sorted BlockRefTableEntry objects, and the caller is responsible for providing entries in the correct order (sorted by database, tablespace, relfilenumber, then fork number).

## Parameters / Member Variables
- `write_callback`: Function pointer for writing data to the output destination
- `write_callback_arg`: Argument to be passed to the write callback function

## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md) (memory allocation)
  - BlockRefTableWrite (writes data through the buffer)
  - INIT_CRC32C (initializes CRC checksum)
  - BLOCKREFTABLE_MAGIC (magic number constant)
  - BlockRefTableWriter (return type structure)

- Called from (representative examples):
  - Functions that need to create incremental block reference table writers
  - Backup and WAL-related utilities that generate block reference tables

## Notes and Other Information
- The caller must ensure that BlockRefTableEntry objects are supplied in sorted order
- The function automatically writes the magic number header to identify the file format
- Memory is allocated using palloc0, so the structure is zero-initialized
- The CRC32C checksum is initialized for data integrity verification
- This is part of PostgreSQL's block reference table system used for tracking modified blocks within LSN ranges