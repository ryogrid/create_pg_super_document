# bbsink_zstd_begin_archive

## Location
[src/backend/backup/basebackup_zstd.c:158-192](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_zstd.c#L158-L192)

## Overview
Prepares the zstd compression sink for compressing a new archive by resetting the compression context and setting up output buffers with an updated archive name.

## Definition

```c
static void
bbsink_zstd_begin_archive(bbsink *sink, const char *archive_name)
```
## Detailed Description
This function prepares the zstd compression sink to begin compressing a new archive file. It resets the zstd compression context using session-only reset to maintain compression parameters while starting fresh compression state. The function configures the output buffer to point to the next sink's buffer and appends the ".zst" extension to the archive name to indicate zstd compression before passing it to the next sink in the chain.

## Parameters / Member Variables
- `*sink`: Pointer to the bbsink structure (cast to bbsink_zstd internally) that will perform zstd compression
- `*archive_name`: Name of the archive being compressed (without extension)
## Dependencies
- Functions called/Symbols referenced:
  - ZSTD_CCtx_reset (resets compression context for new archive)
  - [psprintf](../p/psprintf.md) (formats string with .zst extension)
  - [bbsink_begin_archive](bbsink_begin_archive.md) (notifies next sink to begin archive)
  - [pfree](../p/pfree.md) (frees formatted archive name)
  - Assert (validates next sink is not NULL)
- Called from (representative examples):
  - Through bbsink_zstd_ops function pointer table

## Notes and Other Information
- Uses ZSTD_reset_session_only to preserve compression parameters across archives
- Automatically appends ".zst" extension to archive names to indicate compression
- Sets up zstd output buffer to write directly to the next sink's buffer
- Resets buffer position to 0 for new compression operation
- Function is static and called through the bbsink operations table
- Compression parameters (level, workers, etc.) persist across archive boundaries due to session-only reset

## Simplified Source

```c
static void bbsink_zstd_begin_archive(bbsink *sink, const char *archive_name) {
    bbsink_zstd *mysink = (bbsink_zstd *) sink;
    char *zstd_archive_name;

    // Reset compression context for new archive (preserves parameters)
    ZSTD_CCtx_reset(mysink->cctx, ZSTD_reset_session_only);

    // Setup output buffer pointing to next sink's buffer
    mysink->zstd_outBuf.dst = mysink->base.bbs_next->bbs_buffer;
    mysink->zstd_outBuf.size = mysink->base.bbs_next->bbs_buffer_length;
    mysink->zstd_outBuf.pos = 0;

    // Add .zst extension to archive name and pass to next sink
    zstd_archive_name = psprintf("%s.zst", archive_name);
    Assert(sink->bbs_next != NULL);
    bbsink_begin_archive(sink->bbs_next, zstd_archive_name);
    pfree(zstd_archive_name);
}
```