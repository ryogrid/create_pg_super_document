# bbsink_zstd_end_backup

## Location
[src/backend/backup/basebackup_zstd.c:282-301](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_zstd.c#L282-L301)

## Overview
Finalizes a Zstandard-compressed base backup operation by releasing compression resources and calling the next sink in the chain.

## Definition
```c
static void bbsink_zstd_end_backup(bbsink *sink, XLogRecPtr endptr, TimeLineID endtli)
```

## Detailed Description
This function is responsible for cleaning up resources associated with a Zstandard compression context after a base backup operation completes. It frees the ZSTD compression context that was used during the backup process and then forwards the end_backup call to the next sink in the chain. This is part of PostgreSQL's pluggable backup sink architecture where multiple sinks can be chained together to provide different functionalities (compression, encryption, etc.).

## Parameters / Member Variables
- `sink`: Pointer to the base backup sink structure
- `endptr`: WAL position where the backup ended
- `endtli`: Timeline ID at the end of the backup

## Dependencies
- Functions called/Symbols referenced:
  - ZSTD_freeCCtx (from libzstd)
  - [bbsink_forward_end_backup](bbsink_forward_end_backup.md)
- Called from (representative examples):
  - (No direct callers found - likely called through function pointer in bbsink vtable)

## Notes and Other Information
- This is a static function, part of the internal implementation of the Zstandard backup sink
- The function ensures proper cleanup by setting the context pointer to NULL after freeing
- Part of PostgreSQL's base backup infrastructure that supports multiple compression formats

## Simplified Source
```c
static void bbsink_zstd_end_backup(bbsink *sink, XLogRecPtr endptr, TimeLineID endtli) {
    bbsink_zstd *mysink = (bbsink_zstd *) sink;

    // Clean up compression resources
    if (mysink->cctx) {
        ZSTD_freeCCtx(mysink->cctx);
        mysink->cctx = NULL;
    }

    // Forward to next sink in chain
    bbsink_forward_end_backup(sink, endptr, endtli);
}
```