# bbsink_zstd_manifest_contents

## Location
src/backend/backup/basebackup_zstd.c: 302 - 312

## Overview
Handles backup manifest content by copying it uncompressed to the next sink in the chain, as manifest contents are not compressed in the Zstandard backup sink.

## Definition
```c
static void bbsink_zstd_manifest_contents(bbsink *sink, size_t len)
```

## Detailed Description
This function processes backup manifest contents for the Zstandard backup sink. Unlike regular backup data which gets compressed, manifest contents are deliberately left uncompressed and are simply passed through to the next sink in the chain. The function copies the manifest data from the current sink's buffer to the next sink's buffer and then calls the manifest contents handler on the successor sink. This design ensures that backup manifests remain readable without requiring decompression.

## Parameters / Member Variables
- `sink`: Pointer to the base backup sink structure
- `len`: Size of the manifest content data in bytes

## Dependencies
- Functions called/Symbols referenced:
  - memcpy (standard C library function)
  - bbsink_manifest_contents
- Called from (representative examples):
  - (No direct callers found - likely called through function pointer in bbsink vtable)

## Notes and Other Information
- This is a static function, part of the internal implementation of the Zstandard backup sink
- Manifest contents are intentionally not compressed to maintain their accessibility
- The function performs a simple pass-through operation while maintaining the sink chain pattern
- Part of PostgreSQL's backup manifest system which provides metadata about backup contents