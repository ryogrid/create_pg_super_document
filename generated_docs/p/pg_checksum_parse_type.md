# pg_checksum_parse_type

## Location
[src/common/checksum_helper.c:28-55](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/checksum_helper.c#L28-L55)

## Overview
Parses a string representation of a checksum type and converts it to the corresponding pg_checksum_type enumeration value.

## Definition

```c
bool
pg_checksum_parse_type(char *name, pg_checksum_type *type)
```
## Detailed Description
This function performs case-insensitive string comparison to identify valid checksum algorithm names and maps them to their corresponding enumeration constants. It supports various checksum algorithms including CRC32C and multiple SHA variants. The function provides a standardized way to convert user-provided checksum type names into internal PostgreSQL checksum type representations.

The function uses pg_strcasecmp for case-insensitive comparison, allowing flexible input formatting. If the provided name doesn't match any recognized checksum type, the function sets the output type to CHECKSUM_TYPE_NONE and returns false.

## Parameters / Member Variables
- : Input string containing the checksum algorithm name to parse (case-insensitive)
- : Output parameter that receives the corresponding pg_checksum_type enumeration value

## Dependencies
- Functions called/Symbols referenced:
  - [pg_strcasecmp](pg_strcasecmp.md) (for case-insensitive string comparison)
  - CHECKSUM_TYPE_NONE
  - CHECKSUM_TYPE_CRC32C
  - CHECKSUM_TYPE_SHA224
  - CHECKSUM_TYPE_SHA256
  - CHECKSUM_TYPE_SHA384
  - CHECKSUM_TYPE_SHA512
- Called from (representative examples):
  - [parse_basebackup_options](parse_basebackup_options.md) (in src/backend/backup/basebackup.c)
  - [main](../m/main.md) (in src/bin/pg_combinebackup/pg_combinebackup.c)
  - [json_manifest_finalize_file](../j/json_manifest_finalize_file.md) (in src/common/parse_manifest.c)

## Notes and Other Information
- Supports the following checksum algorithms: none, crc32c, sha224, sha256, sha384, sha512
- [String](../S/String.md) comparison is case-insensitive, allowing input flexibility
- Returns true on successful parsing, false if the algorithm name is not recognized
- Always sets the output type parameter, even on failure (to CHECKSUM_TYPE_NONE)
- Used in backup and manifest processing contexts where checksum algorithms need to be specified by name