# TarMethodFile

## Location
[src/bin/pg_basebackup/walmethods.c:691-697](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/walmethods.c#L691-L697)

## Overview
TarMethodFile is a structure that represents an individual file handle for tar-based WAL writing in PostgreSQL's pg_basebackup utility, extending the base Walfile structure with tar-specific file management data.

## Definition
```c
typedef struct TarMethodFile
{
    Walfile     base;
    pgoff_t     ofs_start;      /* Where does the *header* for this file start */
    char        header[TAR_BLOCK_SIZE];
    size_t      pad_to_size;
} TarMethodFile;
```

## Detailed Description
TarMethodFile extends the base Walfile structure to provide tar archive-specific file handling capabilities. It stores the necessary data for managing an individual WAL file when using the tar storage method. The structure maintains the tar header information, tracks the starting offset of the file within the tar archive, and handles padding requirements for tar format compliance. This allows the tar method to create properly formatted tar archives containing WAL files while maintaining compatibility with standard tar utilities.

## Parameters / Member Variables
- `base`: The base Walfile structure containing the write method reference, current position, and pathname
- `ofs_start`: File offset where the tar header for this file begins in the archive (in bytes)
- `header`: Buffer containing the tar header block (512 bytes as defined by TAR_BLOCK_SIZE)
- `pad_to_size`: Size to which the file should be padded, ensuring proper tar block alignment

## Dependencies
- Functions called/Symbols referenced:
  - Walfile (base structure)
  - pgoff_t (PostgreSQL offset type)
  - TAR_BLOCK_SIZE (tar block size constant, 512 bytes)
- Called from (representative examples):
  - [TarMethodData](TarMethodData.md) (contains references to TarMethodFile instances)
  - [tar_write_padding_data](../t/tar_write_padding_data.md) (uses TarMethodFile for padding operations)
  - [tar_open_for_write](../t/tar_open_for_write.md) (creates and initializes TarMethodFile instances)
  - tar_close (finalizes and writes TarMethodFile to tar archive)

## Notes and Other Information
- This structure follows PostgreSQL's object-oriented programming pattern in C, with the base Walfile embedded as the first member
- The tar format requires 512-byte block alignment, which is handled through the header buffer and padding mechanisms
- The ofs_start field is crucial for updating tar headers when file sizes become known
- Used specifically for tar-based WAL storage in pg_basebackup streaming operations
- Tar headers must be properly formatted according to POSIX tar standards
- File padding ensures that all files in the tar archive are properly aligned to block boundaries
- The header buffer stores the complete tar header which includes filename, permissions, size, and other metadata