# verify_manifest_checksum

## Location
[src/common/parse_manifest.c:812-888](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/parse_manifest.c#L812-L888)

## Overview
Verifies the integrity of a JSON manifest file by computing and comparing its SHA256 checksum against the expected checksum stored in the manifest's last line.

## Definition
```c
static void
verify_manifest_checksum(JsonManifestParseState *parse, const char *buffer,
                         size_t size, pg_cryptohash_ctx *incr_ctx)
```

## Detailed Description
This function is responsible for ensuring the integrity of PostgreSQL backup manifest files by verifying their checksums. The manifest checksum covers all content except the last line, which contains the checksum itself. The function:

1. **Line Structure Validation**: Locates the last two newlines to identify the checksum line and ensure proper file structure
2. **Format Validation**: Verifies the manifest has at least 2 lines and ends with a newline character
3. **Checksum Computation**: Uses SHA256 to compute the actual checksum of all manifest content except the last line
4. **Incremental Support**: Handles both incremental parsing (using provided cryptohash context) and complete parsing (creating new context)
5. **Expected Checksum Parsing**: Decodes the hex-encoded checksum from the manifest's last line
6. **Verification**: Compares computed and expected checksums to detect any data corruption or tampering

The function supports both incremental and non-incremental parsing modes, making it suitable for processing large manifest files in chunks or smaller files in their entirety.

## Parameters / Member Variables
- `parse`: Pointer to JsonManifestParseState structure containing parsing state and the expected manifest_checksum
- `buffer`: Pointer to the manifest data buffer to be checksummed
- `size`: Size of the buffer in bytes
- `incr_ctx`: Optional cryptohash context for incremental parsing (NULL for non-incremental)

## Dependencies
- Functions called/Symbols referenced:
  - `[json_manifest_parse_failure](../j/json_manifest_parse_failure.md)` - [error](../e/error.md) reporting for parsing failures
  - `[pg_cryptohash_create](../p/pg_cryptohash_create.md)` - creates new SHA256 cryptographic hash context
  - `[pg_cryptohash_init](../p/pg_cryptohash_init.md)` - initializes cryptographic hash context
  - `[pg_cryptohash_update](../p/pg_cryptohash_update.md)` - adds data to hash computation
  - `[pg_cryptohash_final](../p/pg_cryptohash_final.md)` - finalizes hash computation and retrieves result
  - `[pg_cryptohash_free](../p/pg_cryptohash_free.md)` - frees cryptographic hash context
  - `[hexdecode_string](../h/hexdecode_string.md)` - converts hex string to binary data
  - `strlen` - string length calculation
  - `memcmp` - memory comparison
  - `PG_SHA256` - SHA256 algorithm identifier constant
  - `PG_SHA256_DIGEST_LENGTH` - SHA256 digest length constant
  - `JsonManifestParseState` - parsing state structure
  - `[JsonManifestParseContext](../J/JsonManifestParseContext.md)` - parsing context structure
  - `[pg_cryptohash_ctx](../p/pg_cryptohash_ctx.md)` - cryptographic hash context type
- Called from (representative examples):
  - `[json_parse_manifest](../j/json_parse_manifest.md)` - complete manifest parsing function
  - `[json_parse_manifest_incremental_chunk](../j/json_parse_manifest_incremental_chunk.md)` - incremental chunk processing
  - Used in `JsonManifestParseIncrementalState` structure

## Notes and Other Information
- This is a static function, only accessible within the parse_manifest.c file
- Requires manifest files to have at least 2 lines with the last line containing the checksum
- Uses SHA256 for cryptographic integrity verification with 32-byte (256-bit) digest length
- The checksum covers all manifest content except the final line containing the checksum itself
- Supports incremental processing for large manifest files by accepting an existing cryptohash context
- Expected checksum must be exactly 64 hex characters (32 bytes × 2 hex chars per byte)
- Any checksum mismatch indicates potential data corruption, tampering, or transmission errors
- Part of PostgreSQL's backup verification infrastructure ensuring backup manifest integrity
- Memory management includes proper cleanup of cryptohash contexts to prevent resource leaks
- Critical security component for backup and restore operations, preventing use of corrupted manifests

## Simplified Source

```c
static void
verify_manifest_checksum(JsonManifestParseState *parse, const char *buffer,
                         size_t size, pg_cryptohash_ctx *incr_ctx)
{
    JsonManifestParseContext *context = parse->context;
    size_t i, number_of_newlines = 0;
    size_t ultimate_newline = 0, penultimate_newline = 0;
    pg_cryptohash_ctx *manifest_ctx;
    uint8 manifest_checksum_actual[PG_SHA256_DIGEST_LENGTH];
    uint8 manifest_checksum_expected[PG_SHA256_DIGEST_LENGTH];

    // Find the last two newlines in the file
    for (i = 0; i < size; ++i)
    {
        if (buffer[i] == '\n')
        {
            ++number_of_newlines;
            penultimate_newline = ultimate_newline;
            ultimate_newline = i;
        }
    }

    // Validate file structure
    if (number_of_newlines < 2)
        json_manifest_parse_failure(parse->context, "expected at least 2 lines");
    if (ultimate_newline != size - 1)
        json_manifest_parse_failure(parse->context, "last line not newline-terminated");

    // Initialize or use existing hash context
    if (incr_ctx == NULL)
    {
        manifest_ctx = pg_cryptohash_create(PG_SHA256);
        if (manifest_ctx == NULL)
            context->error_cb(context, "out of memory");
        if (pg_cryptohash_init(manifest_ctx) < 0)
            context->error_cb(context, "could not initialize checksum of manifest");
    }
    else
        manifest_ctx = incr_ctx;

    // Compute checksum of all content except last line
    if (pg_cryptohash_update(manifest_ctx, (const uint8 *) buffer, penultimate_newline + 1) < 0)
        context->error_cb(context, "could not update checksum of manifest");
    if (pg_cryptohash_final(manifest_ctx, manifest_checksum_actual,
                           sizeof(manifest_checksum_actual)) < 0)
        context->error_cb(context, "could not finalize checksum of manifest");

    // Verify checksum against expected value
    if (parse->manifest_checksum == NULL)
        context->error_cb(parse->context, "manifest has no checksum");
    if (strlen(parse->manifest_checksum) != PG_SHA256_DIGEST_LENGTH * 2 ||
        !hexdecode_string(manifest_checksum_expected, parse->manifest_checksum,
                         PG_SHA256_DIGEST_LENGTH))
        context->error_cb(context, "invalid manifest checksum: \"%s\"",
                         parse->manifest_checksum);
    if (memcmp(manifest_checksum_actual, manifest_checksum_expected,
               PG_SHA256_DIGEST_LENGTH) != 0)
        context->error_cb(context, "manifest checksum mismatch");

    pg_cryptohash_free(manifest_ctx);
}
```