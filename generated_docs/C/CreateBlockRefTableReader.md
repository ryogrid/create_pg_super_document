# CreateBlockRefTableReader

## Location
[src/common/blkreftable.c:577-612](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/blkreftable.c#L577-L612)

## Overview
Creates and initializes a BlockRefTableReader for incrementally reading and parsing a serialized block reference table file with error handling and magic number verification.

## Definition


## Detailed Description
CreateBlockRefTableReader initializes a new BlockRefTableReader structure that provides sequential access to entries in a serialized block reference table file. The function sets up the necessary I/O callbacks, error handling mechanisms, and validates the file format by checking the magic number header. It prepares the internal buffer state and CRC calculation context for subsequent read operations. The reader maintains context for error reporting including the filename and callback function for malformed file detection.

## Parameters / Member Variables
- : I/O callback function for reading data from the underlying file or data source into the internal buffer
- : Opaque argument passed to the read callback function for maintaining context
- : Filename to include in error messages when the file is malformed (not copied, must remain valid)
- : Function called when the file format is found to be invalid or corrupted
- : Opaque argument passed to the error callback function

## Dependencies
- Functions called/Symbols referenced:
  - [BlockRefTableRead](../B/BlockRefTableRead.md)
  - INIT_CRC32C
  - [palloc0](../p/palloc0.md)
  - BLOCKREFTABLE_MAGIC
- Called from (representative examples):
  - [PrepareForIncrementalBackup](../P/PrepareForIncrementalBackup.md)
  - [pg_wal_summary_contents](../p/pg_wal_summary_contents.md)

## Notes and Other Information
- Validates file format by checking BLOCKREFTABLE_MAGIC number at the beginning
- Initializes CRC32C calculation context for data integrity verification during reading
- Error callback is specifically for file format errors, not I/O errors (which are handled by read_callback)
- The error_filename pointer is stored directly without copying, requiring caller to maintain its validity
- Returns a fully initialized reader ready for sequential entry processing
- Memory is allocated using palloc0 to ensure zero-initialization of the reader structure