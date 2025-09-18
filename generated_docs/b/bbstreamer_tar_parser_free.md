# bbstreamer_tar_parser_free

## Location
[src/bin/pg_basebackup/bbstreamer_tar.c:341-355](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/bbstreamer_tar.c#L341-L355)

## Overview
Frees memory associated with a tar parser, including its internal buffer and the next bbstreamer in the processing chain.

## Definition
```c
static void bbstreamer_tar_parser_free(bbstreamer *streamer)
```

## Detailed Description
This function performs cleanup operations for a tar parser bbstreamer when it is being destroyed. It deallocates the internal buffer that was used for accumulating partial tar data during parsing operations. The function also recursively frees the next bbstreamer in the processing chain, ensuring that the entire linked chain of bbstreamers is properly cleaned up. This follows the standard pattern for bbstreamer cleanup where each component is responsible for freeing its own resources and propagating the free operation to connected components.

## Parameters / Member Variables
- `streamer`: The bbstreamer instance to be freed, expected to be a bbstreamer_tar_parser

## Dependencies
- Functions called/Symbols referenced:
  - [pfree](../p/pfree.md)
  - [bbstreamer_free](bbstreamer_free.md)
- Called from (representative examples):
  - No direct references found (likely called through function pointer in ops structure)

## Notes and Other Information
- Part of the bbstreamer cleanup protocol where each component frees its own resources
- Frees the internal StringInfo buffer data that was allocated during parser initialization
- Recursively frees the next bbstreamer in the chain to ensure complete cleanup
- Does not free the streamer structure itself, only its associated resources
- Critical for preventing memory leaks in the backup streaming pipeline
- Follows PostgreSQL memory management conventions using pfree for allocated memory