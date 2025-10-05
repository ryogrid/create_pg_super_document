# read_binary_file

## Location
[src/backend/utils/adt/genfile.c:103-154](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/genfile.c#L103-L154)

## Overview
Reads a section of a file and returns it as a bytea (binary data) object, with support for seeking to specific offsets and reading specified byte ranges.

## Definition
static bytea *read_binary_file(const char *filename, int64 seek_offset, int64 bytes_to_read, bool missing_ok)

## Detailed Description
This function provides low-level binary file reading capabilities with flexible positioning and size control. It opens a file in binary mode, seeks to a specified offset, and reads either a specified number of bytes or the entire remaining file content. The function handles both positive seek offsets (from beginning) and negative offsets (from end). When bytes_to_read is negative, it reads the entire remaining file from the current position using a dynamic string buffer. The function includes comprehensive error handling for file access issues and respects the missing_ok flag to allow graceful handling of non-existent files.

## Parameters / Member Variables
- : Path to the file to be read
- : Position in file to start reading from (positive from start, negative from end)
- : Number of bytes to read (-1 means read to end of file)
- : If true, return NULL for non-existent files instead of throwing error

## Dependencies
- Functions called/Symbols referenced:
  - MaxAllocSize: Maximum allowed allocation size for security
  - ereport: Error reporting mechanism
  - [AllocateFile](../A/AllocateFile.md): PostgreSQL file allocation wrapper
  - PG_BINARY_R: Binary read mode constant
  - fseeko: File seeking with 64-bit offsets
  - VARHDRSZ: Variable-length data header size
  - VARDATA: Access variable-length data content
  - [palloc](../p/palloc.md): PostgreSQL memory allocator
  - [initStringInfo](../i/initStringInfo.md): Initialize dynamic string buffer
  - [appendBinaryStringInfo](../a/appendBinaryStringInfo.md): Append binary data to string buffer
  - [FreeFile](../F/FreeFile.md): Release file handle
- Called from (representative examples):
  - [read_text_file](read_text_file.md): For text file reading with encoding validation
  - [pg_read_binary_file_common](../p/pg_read_binary_file_common.md): Higher-level binary file reading wrapper

## Notes and Other Information
- The caller is responsible for all permissions checking before calling this function
- File size requests are clamped to MaxAllocSize - VARHDRSZ to prevent memory exhaustion
- Uses PostgreSQL's AllocateFile/FreeFile wrappers instead of direct fopen/fclose
- Supports both exact-size reads and read-to-EOF operations
- Returns bytea format compatible with PostgreSQL's binary data handling
- Error messages include filename for better debugging
- The function properly handles partial reads and file errors

## Simplified Source

```c
static bytea *
read_binary_file(const char *filename, int64 seek_offset, int64 bytes_to_read,
                 bool missing_ok)
{
    bytea *buf;
    size_t nbytes = 0;
    FILE *file;

    // Validate request size to prevent memory exhaustion
    if (bytes_to_read > (int64) (MaxAllocSize - VARHDRSZ))
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("requested length too large")));

    // Open file for binary reading
    if ((file = AllocateFile(filename, PG_BINARY_R)) == NULL) {
        if (missing_ok && errno == ENOENT)
            return NULL;
        else
            ereport(ERROR,
                    (errcode_for_file_access(),
                     errmsg("could not open file \"%s\" for reading: %m", filename)));
    }

    // Seek to specified position (from start if positive, from end if negative)
    if (fseeko(file, (off_t) seek_offset,
               (seek_offset >= 0) ? SEEK_SET : SEEK_END) != 0)
        ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("could not seek in file \"%s\": %m", filename)));

    if (bytes_to_read >= 0) {
        // Read specified number of bytes
        buf = (bytea *) palloc((Size) bytes_to_read + VARHDRSZ);
        nbytes = fread(VARDATA(buf), 1, (size_t) bytes_to_read, file);
    } else {
        // Read entire remaining file using dynamic buffer
        StringInfoData sbuf;
        initStringInfo(&sbuf);
        sbuf.len += VARHDRSZ;  // Reserve space for varlena header

        while (!(feof(file) || ferror(file))) {
            size_t rbytes;

            // Check for file size limit
            if (sbuf.len == MaxAllocSize - 1) {
                char rbuf[1];
                if (fread(rbuf, 1, 1, file) != 0 || !feof(file))
                    ereport(ERROR,
                            (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                             errmsg("file length too large")));
                else
                    break;
            }

            // Ensure buffer has space for reading
            enlargeStringInfo(&sbuf, MIN_READ_SIZE);

            // Read available data into buffer
            rbytes = fread(sbuf.data + sbuf.len, 1,
                          (size_t) (sbuf.maxlen - sbuf.len - 1), file);
            sbuf.len += rbytes;
            nbytes += rbytes;
        }

        // Use the stringinfo buffer as result
        buf = (bytea *) sbuf.data;
    }

    // Check for read errors
    if (ferror(file))
        ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("could not read file \"%s\": %m", filename)));

    // Set final size and cleanup
    SET_VARSIZE(buf, nbytes + VARHDRSZ);
    FreeFile(file);

    return buf;
}
```