# local_buffer_write_error_callback

## Location
[src/backend/storage/buffer/bufmgr.c:5688-5707](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L5688-L5707)

## Overview
Provides error context information for errors occurring during local buffer write operations in PostgreSQL buffer management.

## Definition
```c
static void local_buffer_write_error_callback(void *arg)
```

## Detailed Description
This function serves as an error context callback specifically designed to provide meaningful error information when local buffer write operations fail. Similar to its shared buffer counterpart, it extracts buffer metadata from the provided BufferDesc argument and formats contextual information about which block and relation were being written when the error occurred. The key difference is that this function uses relpathbackend() instead of relpathperm() to construct the file path, as local buffers are backend-specific temporary files that include the process number in their path structure.

## Parameters / Member Variables
- `arg`: A void pointer that should point to a BufferDesc structure representing the local buffer being written when the error occurred

## Dependencies
- Functions called/Symbols referenced:
  - [BufferDesc](../B/BufferDesc.md) (structure type)
  - relpathbackend (converts relation file locator to backend-specific path)
  - [BufTagGetRelFileLocator](../B/BufTagGetRelFileLocator.md) (extracts relation file locator from buffer tag)
  - MyProcNumber (global variable for current process number)
  - [BufTagGetForkNum](../B/BufTagGetForkNum.md) (extracts fork number from buffer tag)
  - errcontext (adds context information to error reports)
- Called from (representative examples):
  - BufferIsPinned
  - [FlushRelationBuffers](../F/FlushRelationBuffers.md)

## Notes and Other Information
- This is a static function internal to bufmgr.c
- Handles local/temporary buffers as opposed to shared buffers
- Uses MyProcNumber to include process-specific information in the path
- Memory allocated by relpathbackend() is properly freed using pfree()
- Part of PostgreSQL error reporting infrastructure for local buffer management operations
- Provides critical debugging information for local buffer I/O failures

## Simplified Source

```c
static void
local_buffer_write_error_callback(void *arg)
{
    BufferDesc *bufHdr = (BufferDesc *) arg;

    if (bufHdr != NULL)
    {
        // Build backend-specific file path for the relation
        char *path = relpathbackend(BufTagGetRelFileLocator(&bufHdr->tag),
                                   MyProcNumber,
                                   BufTagGetForkNum(&bufHdr->tag));

        // Add error context with block number and relation path
        errcontext("writing block %u of relation %s",
                   bufHdr->tag.blockNum, path);

        pfree(path);
    }
}
```