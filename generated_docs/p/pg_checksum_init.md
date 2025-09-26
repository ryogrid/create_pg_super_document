# pg_checksum_init

## Location
[src/common/checksum_helper.c:83-144](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/checksum_helper.c#L83-L144)

## Overview
Initializes a checksum context for computing checksums of a specified algorithm type.

## Definition
```c
int pg_checksum_init(pg_checksum_context *context, pg_checksum_type type)
```

## Detailed Description
This function initializes a pg_checksum_context structure to begin checksum computation for the specified algorithm. It sets up the internal state required for the chosen checksum type, handling the initialization differently for each supported algorithm. For CRC32C, it uses the INIT_CRC32C macro. For SHA variants (SHA224, SHA256, SHA384, SHA512), it creates and initializes a cryptographic hash context using PostgreSQL's cryptohash API.

The function provides proper error handling for cryptographic hash initialization failures, ensuring resource cleanup by freeing any partially allocated contexts on failure. This prevents memory leaks when initialization fails.

## Parameters / Member Variables
- `context`: Pointer to the checksum context structure to initialize
- `type`: The type of checksum algorithm to use for computation

## Dependencies
- Functions called/Symbols referenced:
  - INIT_CRC32C (macro for CRC32C initialization)
  - pg_cryptohash_create (creates cryptographic hash contexts)
  - pg_cryptohash_init (initializes cryptographic hash computation)
  - pg_cryptohash_free (frees cryptographic hash contexts on error)
  - PG_SHA224, PG_SHA256, PG_SHA384, PG_SHA512 (hash algorithm constants)
  - CHECKSUM_TYPE_* enumeration constants
- Called from (representative examples):
  - sendFileWithContent (in src/backend/backup/basebackup.c)
  - sendFile (in src/backend/backup/basebackup.c)
  - process_directory_recursively (in src/bin/pg_combinebackup/pg_combinebackup.c)
  - reconstruct_from_incremental_file (in src/bin/pg_combinebackup/reconstruct.c)
  - verify_file_checksum (in src/bin/pg_verifybackup/pg_verifybackup.c)

## Notes and Other Information
- Returns 0 on success, -1 on failure
- Handles CHECKSUM_TYPE_NONE by doing nothing (no initialization required)
- For SHA algorithms, performs proper error handling with resource cleanup
- Must be called before pg_checksum_update and pg_checksum_final
- Used extensively in backup, restore, and verification operations
- Failure typically indicates memory allocation issues or cryptographic library problems