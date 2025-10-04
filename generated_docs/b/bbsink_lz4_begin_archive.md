# bbsink_lz4_begin_archive

## Location
[src/backend/backup/basebackup_lz4.c:132-179](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_lz4.c#L132-L179)

## Overview
Prepares the LZ4 compression context for compressing a new archive file, initializing the compression context and writing the LZ4 frame header.

## Definition
```c
static void bbsink_lz4_begin_archive(bbsink *sink, const char *archive_name)
```

## Detailed Description
This function initializes the LZ4 compression for a new archive file within a base backup. It creates a new LZ4 compression context, writes the LZ4 frame header to the output buffer, tracks the number of bytes written, and modifies the archive name by appending the ".lz4" extension before passing it to the next sink in the chain.

The function handles LZ4 compression initialization using the LZ4F (frame) API, which provides a stream-oriented interface. Error handling is implemented for both context creation and header writing operations. The bytes_written counter is maintained to track the current position in the output buffer for subsequent compression operations.

## Parameters / Member Variables
- `sink`: Pointer to the base bbsink structure (cast to bbsink_lz4 internally)
- `archive_name`: Name of the archive file being compressed (without .lz4 extension)

## Dependencies
- Functions called/Symbols referenced:
  - LZ4F_createCompressionContext (LZ4 library function)
  - LZ4F_compressBegin (LZ4 library function)
  - LZ4F_isError (LZ4 library function)  
  - LZ4F_getErrorName (LZ4 library function)
  - elog (error logging)
  - [psprintf](../p/psprintf.md) (formatted string allocation)
  - [bbsink_begin_archive](bbsink_begin_archive.md) (calls next sink in chain)
  - [pfree](../p/pfree.md) (memory deallocation)
- Called from (representative examples):
  - Referenced through bbsink_lz4_ops function pointer table

## Notes and Other Information
- Static function, only accessible within the basebackup_lz4.c module
- Creates a new LZ4 compression context for each archive
- Automatically appends ".lz4" extension to archive names
- Writes LZ4 frame header directly to the next sink's buffer
- Maintains bytes_written counter for proper buffer management
- Part of the sink operation callbacks, called through function pointer indirection
- Error handling includes descriptive LZ4 error messages

## Simplified Source

```c
static void
bbsink_lz4_begin_archive(bbsink *sink, const char *archive_name)
{
    bbsink_lz4 *mysink = (bbsink_lz4 *) sink;
    char *lz4_archive_name;
    LZ4F_errorCode_t ctxError;
    size_t headerSize;

    // Create LZ4 compression context
    ctxError = LZ4F_createCompressionContext(&mysink->ctx, LZ4F_VERSION);
    if (LZ4F_isError(ctxError))
        elog(ERROR, "could not create lz4 compression context: %s",
             LZ4F_getErrorName(ctxError));

    // Write LZ4 frame header to output buffer
    headerSize = LZ4F_compressBegin(mysink->ctx,
                                   mysink->base.bbs_next->bbs_buffer,
                                   mysink->base.bbs_next->bbs_buffer_length,
                                   &mysink->prefs);

    if (LZ4F_isError(headerSize))
        elog(ERROR, "could not write lz4 header: %s",
             LZ4F_getErrorName(headerSize));

    // Track bytes written for buffer management
    mysink->bytes_written += headerSize;

    // Create archive name with .lz4 extension and pass to next sink
    lz4_archive_name = psprintf("%s.lz4", archive_name);
    bbsink_begin_archive(sink->bbs_next, lz4_archive_name);
    pfree(lz4_archive_name);
}
```