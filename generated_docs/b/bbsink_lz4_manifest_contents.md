# bbsink_lz4_manifest_contents

## Location
[src/backend/backup/basebackup_lz4.c:274-284](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_lz4.c#L274-L284)

## Overview
A callback function for the LZ4 basebackup sink that handles manifest contents by copying them to the next sink in the chain without compression.

## Definition
```c
static void bbsink_lz4_manifest_contents(bbsink *sink, size_t len)
```

## Detailed Description
This function is part of the LZ4 basebackup sink implementation and serves as the manifest_contents callback in the bbsink_ops structure. Unlike archive contents which are compressed, manifest contents are deliberately not compressed but must still be handled properly within the LZ4 sink chain.

The function performs a simple but important task: it copies the manifest data from the current sink's buffer to the next sink's buffer, then delegates the actual manifest processing to the next sink in the chain. This is necessary because the LZ4 sink maintains its own buffer separate from the successor sink's buffer.

## Parameters / Member Variables
- `sink`: Pointer to the bbsink structure representing the LZ4 compression sink
- `len`: Size in bytes of the manifest contents to be processed

## Dependencies
- Functions called/Symbols referenced:
  - memcpy (standard C library function)
  - [bbsink_manifest_contents](bbsink_manifest_contents.md) (base sink function for manifest processing)
- Called from (representative examples):
  - Referenced in bbsink_lz4_ops.manifest_contents function pointer
  - Invoked through the basebackup sink callback mechanism

## Notes and Other Information
- This function is only available when PostgreSQL is compiled with LZ4 support (USE_LZ4 defined)
- The function explicitly does not compress manifest contents, as indicated by the comment
- Uses a simple memcpy approach to transfer data between sink buffers before forwarding to the next sink
- Part of a chain-of-responsibility pattern where each sink in the chain processes data and forwards it to the next sink
- The function is declared as static, making it internal to the basebackup_lz4.c module

## Simplified Source

```c
// Simplified version of bbsink_lz4_manifest_contents
static void bbsink_lz4_manifest_contents(bbsink *sink, size_t len)
{
    // Copy manifest data to next sink's buffer (manifest is not compressed)
    memcpy(sink->bbs_next->bbs_buffer, sink->bbs_buffer, len);

    // Forward to next sink in chain
    bbsink_manifest_contents(sink->bbs_next, len);
}
```