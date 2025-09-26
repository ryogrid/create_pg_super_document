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