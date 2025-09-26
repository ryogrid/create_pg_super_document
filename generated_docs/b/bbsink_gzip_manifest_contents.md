# bbsink_gzip_manifest_contents

## Location
[src/backend/backup/basebackup_gzip.c:278-288](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_gzip.c#L278-L288)

## Overview
This function handles manifest contents processing in the gzip backup sink, specifically copying manifest data to the successor sink without compression.

## Definition
```c
static void bbsink_gzip_manifest_contents(bbsink *sink, size_t len)
```

## Detailed Description
The `bbsink_gzip_manifest_contents` function is part of the gzip backup sink infrastructure used in PostgreSQL's base backup functionality. Unlike archive contents which are compressed, manifest contents are passed through uncompressed. However, since the gzip sink maintains its own buffer, the function must copy the manifest data from its buffer to the next sink's buffer before forwarding the call.

This function serves as an implementation of the `manifest_contents` callback in the `bbsink_ops` structure for gzip-enabled backup sinks. It ensures proper data flow through the backup sink chain while maintaining the uncompressed nature of manifest data.

## Parameters / Member Variables
- `sink`: Pointer to the gzip backup sink structure containing the manifest data to be processed
- `len`: Size in bytes of the manifest content data to be copied and forwarded

## Dependencies
- Functions called/Symbols referenced:
  - memcpy (standard C library function)
  - [bbsink_manifest_contents](bbsink_manifest_contents.md) (forwards the call to the next sink)
- Called from (representative examples):
  - Used as callback in bbsink_gzip_ops structure
  - Invoked through the backup sink infrastructure

## Notes and Other Information
- This is a static function, internal to the basebackup_gzip.c module
- Manifest contents are intentionally not compressed, unlike archive contents
- The function performs a simple buffer copy followed by delegation to the next sink in the chain
- Part of the backup sink chain pattern used in PostgreSQL's streaming base backup functionality