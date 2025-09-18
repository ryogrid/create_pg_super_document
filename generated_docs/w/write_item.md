# write_item

## Location
src/backend/utils/cache/relcache.c: 6703 - 6725

## Overview
A utility function that writes a data chunk to a binary file, preceded by its length, for the relation cache initialization file format.

## Definition


## Detailed Description
This is a helper function used by write_relcache_init_file() to write individual data structures to the binary initialization file. It implements a simple serialization format where each data item is preceded by its size, enabling the corresponding read operation to know how much data to read.

The function first writes the length value as a Size type, then writes the actual data if the length is greater than zero. This format allows for variable-length data structures and handles cases where data might be NULL (len=0).

The function reports FATAL errors if write operations fail, ensuring data integrity in the initialization file. This is critical since a corrupted initialization file would cause backend startup failures.

## Parameters / Member Variables
- : Pointer to the data to be written (can be NULL if len is 0)
- : Size of the data in bytes (Size type)  
- : File pointer to the output file

## Dependencies
- Functions called/Symbols referenced:
  - fwrite (standard C library)
  - ereport/errcode_for_file_access (PostgreSQL error reporting)
- Called from (representative examples):
  - [write_relcache_init_file](write_relcache_init_file.md) (multiple calls for different data structures)

## Notes and Other Information
- Uses PostgreSQL's Size type for length field, ensuring consistency with memory allocation functions
- Handles NULL data gracefully when len is 0
- Fatal errors ensure initialization file integrity - partial writes would corrupt the file
- Simple binary format: [Size length][Data bytes] for each item
- Counterpart reading is done in load_relcache_init_file() which reads length first, then data
- File location: src/backend/utils/cache/relcache.c:6703-6725