# pg_checksum_update

## Location
[src/common/checksum_helper.c:145-175](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/checksum_helper.c#L145-L175)

## Overview
Updates a checksum computation context with new input data for incremental checksum calculation.

## Definition
```c
int pg_checksum_update(pg_checksum_context *context, const uint8 *input, size_t len)
```

## Detailed Description
This function processes new data through the checksum algorithm previously initialized by pg_checksum_init. It supports streaming/incremental checksum computation, allowing large files or data streams to be processed in chunks without loading everything into memory at once. The function handles different checksum algorithms appropriately: for CRC32C it uses the COMP_CRC32C macro to update the running CRC, while for SHA variants it delegates to the cryptographic hash update function.

The function maintains the running checksum state in the context structure, accumulating results from multiple update calls until finalization.

## Parameters / Member Variables
- `context`: Pointer to the initialized checksum context containing algorithm state
- `input`: Pointer to the input data buffer to process
- `len`: Number of bytes to process from the input buffer

## Dependencies
- Functions called/Symbols referenced:
  - COMP_CRC32C (macro for CRC32C computation)
  - [pg_cryptohash_update](pg_cryptohash_update.md) (updates cryptographic hash with new data)
  - CHECKSUM_TYPE_* enumeration constants for algorithm dispatch
- Called from (representative examples):
  - [sendFileWithContent](../s/sendFileWithContent.md) (in src/backend/backup/basebackup.c)
  - [sendFile](../s/sendFile.md) (in src/backend/backup/basebackup.c)
  - [checksum_file](../c/checksum_file.md) (in src/bin/pg_combinebackup/copy_file.c)
  - [write_reconstructed_file](../w/write_reconstructed_file.md) (in src/bin/pg_combinebackup/reconstruct.c)
  - [verify_file_checksum](../v/verify_file_checksum.md) (in src/bin/pg_verifybackup/pg_verifybackup.c)

## Notes and Other Information
- Returns 0 on success, -1 on failure
- Can be called multiple times to process data incrementally
- Handles CHECKSUM_TYPE_NONE by doing nothing (no processing required)
- For SHA algorithms, failure typically indicates cryptographic library errors
- Must be called after pg_checksum_init and before pg_checksum_final
- Supports processing data in arbitrary-sized chunks for memory efficiency
- Widely used in backup operations where files are streamed rather than loaded entirely into memory