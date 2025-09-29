# pg_checksum_type_name

## Location
[src/common/checksum_helper.c:56-82](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/checksum_helper.c#L56-L82)

## Overview
Converts a pg_checksum_type enumeration value to its canonical human-readable string representation.

## Definition
```c
char *pg_checksum_type_name(pg_checksum_type type)
```

## Detailed Description
This function provides the inverse operation to pg_checksum_parse_type, converting internal checksum type enumeration values back to their standard string representations. It uses a simple switch statement to map each supported checksum algorithm to its uppercase canonical name. The function is essential for displaying checksum information in logs, manifests, and user interfaces.

The function includes an assertion to catch invalid enumeration values during development, and returns a placeholder string "???" for unrecognized types in production builds.

## Parameters / Member Variables
- `type`: The pg_checksum_type enumeration value to convert to a string

## Dependencies
- Functions called/Symbols referenced:
  - CHECKSUM_TYPE_NONE
  - CHECKSUM_TYPE_CRC32C
  - CHECKSUM_TYPE_SHA224
  - CHECKSUM_TYPE_SHA256
  - CHECKSUM_TYPE_SHA384
  - CHECKSUM_TYPE_SHA512
  - Assert (for debugging invalid types)
- Called from (representative examples):
  - [AddFileToBackupManifest](../A/AddFileToBackupManifest.md) (in src/backend/backup/backup_manifest.c)
  - [copy_file](../c/copy_file.md) (in src/bin/pg_combinebackup/copy_file.c)
  - [write_reconstructed_file](../w/write_reconstructed_file.md) (in src/bin/pg_combinebackup/reconstruct.c)
  - [add_file_to_manifest](../a/add_file_to_manifest.md) (in src/bin/pg_combinebackup/write_manifest.c)

## Notes and Other Information
- Returns uppercase canonical names: "NONE", "CRC32C", "SHA224", "SHA256", "SHA384", "SHA512"
- Includes debug assertion to catch invalid enumeration values
- Returns "???" placeholder for unrecognized types in production builds
- Widely used in backup and manifest generation contexts where checksum types need to be serialized
- The returned strings are static and do not need to be freed by the caller

## Simplified Source

```c
char *pg_checksum_type_name(pg_checksum_type type)
{
    switch (type) {
        case CHECKSUM_TYPE_NONE:
            return "NONE";
        case CHECKSUM_TYPE_CRC32C:
            return "CRC32C";
        case CHECKSUM_TYPE_SHA224:
            return "SHA224";
        case CHECKSUM_TYPE_SHA256:
            return "SHA256";
        case CHECKSUM_TYPE_SHA384:
            return "SHA384";
        case CHECKSUM_TYPE_SHA512:
            return "SHA512";
    }

    Assert(false);
    return "???";
}
```