# pg_checksum_final

## Location
[src/common/checksum_helper.c:176-232](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/checksum_helper.c#L176-L232)

## Overview
Finalizes a checksum computation and outputs the computed checksum value to a provided buffer.

## Definition
```c
int pg_checksum_final(pg_checksum_context *context, uint8 *output)
```

## Detailed Description
This function completes the checksum computation process by finalizing the algorithm-specific calculations and copying the resulting checksum to the output buffer. For CRC32C, it calls FIN_CRC32C to complete the CRC calculation and copies the result. For SHA variants, it calls pg_cryptohash_final to compute the final hash digest and automatically frees the cryptographic context to prevent memory leaks.

The function includes comprehensive compile-time assertions to ensure that all supported checksum digest sizes fit within PG_CHECKSUM_MAX_LENGTH. It returns the actual number of bytes written to the output buffer, which varies by algorithm (4 bytes for CRC32C, different lengths for various SHA algorithms).

## Parameters / Member Variables
- `context`: Pointer to the initialized checksum context that has been updated with data
- `output`: Output buffer to receive the computed checksum (must be at least PG_CHECKSUM_MAX_LENGTH bytes)

## Dependencies
- Functions called/Symbols referenced:
  - FIN_CRC32C (macro for CRC32C finalization)
  - [pg_cryptohash_final](pg_cryptohash_final.md) (finalizes cryptographic hash computation)
  - [pg_cryptohash_free](pg_cryptohash_free.md) (frees cryptographic hash contexts)
  - StaticAssertDecl (compile-time assertions for digest size validation)
  - memcpy (for copying CRC32C result to output buffer)
  - PG_SHA*_DIGEST_LENGTH constants (digest size constants for SHA variants)
  - CHECKSUM_TYPE_* enumeration constants
- Called from (representative examples):
  - [AddFileToBackupManifest](../A/AddFileToBackupManifest.md) (in src/backend/backup/backup_manifest.c)
  - [process_directory_recursively](process_directory_recursively.md) (in src/bin/pg_combinebackup/pg_combinebackup.c)
  - [reconstruct_from_incremental_file](../r/reconstruct_from_incremental_file.md) (in src/bin/pg_combinebackup/reconstruct.c)
  - [verify_file_checksum](../v/verify_file_checksum.md) (in src/bin/pg_verifybackup/pg_verifybackup.c)

## Notes and Other Information
- Returns the number of bytes written to output buffer on success, -1 on failure
- Output buffer must be at least PG_CHECKSUM_MAX_LENGTH bytes to accommodate any checksum type
- For CHECKSUM_TYPE_NONE, returns 0 (no checksum computed)
- Automatically frees cryptographic contexts for SHA algorithms to prevent memory leaks
- Contains compile-time assertions ensuring all digest types fit in the maximum buffer size
- Must be called after pg_checksum_init and pg_checksum_update calls
- After calling this function, the context should not be reused without re-initialization

## Simplified Source

```c
int pg_checksum_final(pg_checksum_context *context, uint8 *output)
{
    int retval = 0;

    // Compile-time checks that all digest sizes fit in PG_CHECKSUM_MAX_LENGTH
    StaticAssertDecl(sizeof(pg_crc32c) <= PG_CHECKSUM_MAX_LENGTH, "CRC-32C digest too big");
    StaticAssertDecl(PG_SHA224_DIGEST_LENGTH <= PG_CHECKSUM_MAX_LENGTH, "SHA224 digest too big");
    StaticAssertDecl(PG_SHA256_DIGEST_LENGTH <= PG_CHECKSUM_MAX_LENGTH, "SHA256 digest too big");
    StaticAssertDecl(PG_SHA384_DIGEST_LENGTH <= PG_CHECKSUM_MAX_LENGTH, "SHA384 digest too big");
    StaticAssertDecl(PG_SHA512_DIGEST_LENGTH <= PG_CHECKSUM_MAX_LENGTH, "SHA512 digest too big");

    switch (context->type) {
        case CHECKSUM_TYPE_NONE:
            break;

        case CHECKSUM_TYPE_CRC32C:
            FIN_CRC32C(context->raw_context.c_crc32c);
            retval = sizeof(pg_crc32c);
            memcpy(output, &context->raw_context.c_crc32c, retval);
            break;

        case CHECKSUM_TYPE_SHA224:
            retval = PG_SHA224_DIGEST_LENGTH;
            if (pg_cryptohash_final(context->raw_context.c_sha2, output, retval) < 0)
                return -1;
            pg_cryptohash_free(context->raw_context.c_sha2);
            break;

        case CHECKSUM_TYPE_SHA256:
            retval = PG_SHA256_DIGEST_LENGTH;
            if (pg_cryptohash_final(context->raw_context.c_sha2, output, retval) < 0)
                return -1;
            pg_cryptohash_free(context->raw_context.c_sha2);
            break;

        case CHECKSUM_TYPE_SHA384:
            retval = PG_SHA384_DIGEST_LENGTH;
            if (pg_cryptohash_final(context->raw_context.c_sha2, output, retval) < 0)
                return -1;
            pg_cryptohash_free(context->raw_context.c_sha2);
            break;

        case CHECKSUM_TYPE_SHA512:
            retval = PG_SHA512_DIGEST_LENGTH;
            if (pg_cryptohash_final(context->raw_context.c_sha2, output, retval) < 0)
                return -1;
            pg_cryptohash_free(context->raw_context.c_sha2);
            break;
    }

    Assert(retval <= PG_CHECKSUM_MAX_LENGTH);
    return retval;
}
```