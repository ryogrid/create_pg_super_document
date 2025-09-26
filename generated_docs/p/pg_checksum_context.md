# pg_checksum_context

## Location
src/include/common/checksum_helper.h: 52 - 56

## Overview
A convenient wrapper structure that combines a checksum type identifier with its corresponding checksum context for unified checksum operations across PostgreSQL.

## Definition
```c
typedef struct pg_checksum_context
{
    pg_checksum_type type;
    pg_checksum_raw_context raw_context;
} pg_checksum_context;
```

## Detailed Description
The `pg_checksum_context` structure serves as a unified container for checksum operations in PostgreSQL, encapsulating both the type of checksum algorithm being used and the actual computation context for that algorithm. This design provides a clean abstraction layer that allows PostgreSQL code to work with different checksum algorithms (CRC32C, SHA224, SHA256, SHA384, SHA512) through a consistent interface.

The structure is primarily used in backup and verification operations, where different checksum algorithms may be employed depending on security requirements and performance considerations. It enables type-safe checksum operations by ensuring that the correct algorithm context is always paired with its corresponding type identifier.

This abstraction is particularly important in PostgreSQLs backup infrastructure, where checksums are computed for files during backup creation and later verified during backup validation. The unified interface allows the same code paths to handle different checksum algorithms without algorithm-specific branching at the application level.

## Parameters / Member Variables
- `type`: Specifies the checksum algorithm type (CHECKSUM_TYPE_NONE, CHECKSUM_TYPE_CRC32C, CHECKSUM_TYPE_SHA224, CHECKSUM_TYPE_SHA256, CHECKSUM_TYPE_SHA384, or CHECKSUM_TYPE_SHA512)
- `raw_context`: Union containing the actual checksum computation context, which varies depending on the selected algorithm (CRC32C state or SHA cryptographic hash context)

## Dependencies
- Functions called/Symbols referenced:
  - pg_checksum_type
  - pg_checksum_raw_context
- Called from (representative examples):
  - pg_checksum_init
  - pg_checksum_update  
  - pg_checksum_final
  - AddFileToBackupManifest
  - sendFileWithContent
  - copy_file
  - verify_file_checksum

## Notes and Other Information
- The structure is designed to be stack-allocated and passed by pointer to checksum functions
- Different checksum algorithms have varying computational costs: CRC32C is fastest but provides only error detection, while SHA variants provide cryptographic strength
- The union-based raw_context optimizes memory usage by sharing space between different algorithm contexts
- Used extensively in PostgreSQLs backup and verification infrastructure
- Supports both non-cryptographic (CRC32C) and cryptographic (SHA family) checksum algorithms
- The CHECKSUM_TYPE_NONE option allows for disabling checksum computation when not needed