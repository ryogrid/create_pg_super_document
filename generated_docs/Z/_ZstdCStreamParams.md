# _ZstdCStreamParams

## Location
[src/bin/pg_dump/compress_zstd.c:73-93](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/compress_zstd.c#L73-L93)

## Overview
A static function that creates and configures a ZSTD compression stream with parameters specified in a PostgreSQL compression specification structure.

## Definition
```c
static ZSTD_CStream *_ZstdCStreamParams(pg_compress_specification compress)
```

## Detailed Description
This function creates a new ZSTD compression stream using `ZSTD_createCStream()` and configures it according to the compression parameters provided in the `pg_compress_specification` structure. It sets the compression level and optionally enables long-distance matching if specified in the compression options. The function uses the helper function `_Zstd_CCtx_setParam_or_die()` to safely set parameters with proper error handling.

## Parameters / Member Variables
- `compress`: A `pg_compress_specification` structure containing compression configuration options including level and optional long-distance matching settings

## Dependencies
- Functions called/Symbols referenced:
  - ZSTD_createCStream (from ZSTD library)
  - [pg_fatal](../p/pg_fatal.md) (PostgreSQL error handling)
  - [_Zstd_CCtx_setParam_or_die](_Zstd_CCtx_setParam_or_die.md) (internal helper function)
  - [pg_compress_specification](../p/pg_compress_specification.md) (PostgreSQL compression specification type)
  - PG_COMPRESSION_OPTION_LONG_DISTANCE (compression option flag)
- Called from (representative examples):
  - [InitCompressorZstd](../I/InitCompressorZstd.md)
  - [Zstd_write](Zstd_write.md)

## Notes and Other Information
- This is a static function internal to the compress_zstd.c module
- Returns a fully configured ZSTD compression stream ready for use
- Supports both basic compression level setting and advanced long-distance matching option
- Uses PostgreSQL's fatal error handling if stream creation fails
- Part of PostgreSQL's pg_dump utility's ZSTD compression support
- The returned stream must be properly cleaned up by the caller using ZSTD library functions

## Simplified Source

```c
static ZSTD_CStream *
_ZstdCStreamParams(pg_compress_specification compress)
{
    // Create ZSTD compression stream
    ZSTD_CStream *cstream = ZSTD_createCStream();
    if (cstream == NULL)
        pg_fatal("could not initialize compression library");

    // Set compression level
    _Zstd_CCtx_setParam_or_die(cstream, ZSTD_c_compressionLevel,
                               compress.level, "level");

    // Enable long-distance matching if requested
    if (compress.options & PG_COMPRESSION_OPTION_LONG_DISTANCE)
        _Zstd_CCtx_setParam_or_die(cstream,
                                   ZSTD_c_enableLongDistanceMatching,
                                   compress.long_distance, "long");

    return cstream;
}
```