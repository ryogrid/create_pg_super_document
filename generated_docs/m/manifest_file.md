# manifest_file

## Location
[src/bin/pg_verifybackup/pg_verifybackup.c:52-62](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_verifybackup/pg_verifybackup.c#L52-L62)

## Overview
A data structure that represents each file described by the backup manifest in PostgreSQL, containing file metadata and checksum information for verification purposes.

## Definition
```c
typedef struct manifest_file
{
    uint32          status;             /* hash status */
    const char     *pathname;
    size_t          size;
    pg_checksum_type checksum_type;
    int             checksum_length;
    uint8          *checksum_payload;
    bool            matched;
    bool            bad;
} manifest_file;
```

## Detailed Description
The `manifest_file` structure is used by PostgreSQL backup verification tools (pg_verifybackup, pg_combinebackup) to represent individual files within a backup manifest. Each instance contains comprehensive metadata about a single file, including its path, size, checksum information, and verification status. This structure is central to the backup verification process, allowing the system to track which files have been processed and whether they match expected checksums.

## Parameters / Member Variables
- `status`: Hash computation status indicator for the file
- `pathname`: Full path to the file being described
- `size`: Size of the file in bytes
- `checksum_type`: Type of checksum algorithm used (from pg_checksum_type enum)
- `checksum_length`: Length of the checksum data in bytes
- `checksum_payload`: Binary checksum data for verification
- `matched`: Boolean flag indicating whether the file was found and matched during verification
- `bad`: Boolean flag indicating whether the file failed verification checks

## Dependencies
- Functions called/Symbols referenced:
  - pg_checksum_type

- Called from (representative examples):
  - [ReceiveArchiveStream](../R/ReceiveArchiveStream.md) (src/bin/pg_basebackup/pg_basebackup.c:1297)
  - [verifybackup_per_file_cb](../v/verifybackup_per_file_cb.md) (src/bin/pg_verifybackup/pg_verifybackup.c:555)
  - [verify_backup_file](../v/verify_backup_file.md) (src/bin/pg_verifybackup/pg_verifybackup.c:678)
  - [combinebackup_per_file_cb](../c/combinebackup_per_file_cb.md) (src/bin/pg_combinebackup/load_manifest.c:274)

## Notes and Other Information
This structure is primarily used in PostgreSQL backup-related utilities and is essential for maintaining file integrity during backup operations. The structure supports various checksum algorithms and tracks verification state to ensure backup reliability.