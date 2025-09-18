# JsonManifestParseContext

## Location
[src/include/common/parse_manifest.h:40-58](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/common/parse_manifest.h#L40-L58)

## Overview
A callback-based context structure used for parsing PostgreSQL backup manifest files in JSON format, providing a framework for processing different sections of the manifest through user-defined callback functions.

## Definition


## Detailed Description
JsonManifestParseContext serves as a configuration and callback mechanism for parsing PostgreSQL backup manifest files. The structure implements a callback-driven architecture where different sections of a JSON backup manifest trigger specific callback functions. This design allows multiple tools (like pg_verifybackup and pg_combinebackup) to reuse the same parsing logic while implementing their own specific handling for each manifest component.

The parser processes backup manifests that contain metadata about backup files, WAL ranges, version information, and system identifiers. Each callback receives the context as its first parameter, allowing access to private data and enabling stateful processing across different manifest sections.

## Parameters / Member Variables
- : User-defined data pointer that gets passed to all callback functions, allowing tools to maintain state during parsing
- : Callback function invoked when the manifest version field is encountered, receives the manifest version number
- : Callback function called when the system identifier field is parsed, receives the 64-bit system identifier value
- : Callback function executed for each file entry in the manifest, receives file path, size, checksum type, length, and payload
- : Callback function triggered for each WAL range entry, receives timeline ID, start LSN, and end LSN
- : Error callback function called when parsing errors occur, receives formatted error message and terminates execution

## Dependencies
- Functions called/Symbols referenced:
  - [json_parse_manifest](../j/json_parse_manifest.md)
  - [json_parse_manifest_incremental_init](../j/json_parse_manifest_incremental_init.md)
  - [json_parse_manifest_incremental_chunk](../j/json_parse_manifest_incremental_chunk.md)
- Called from (representative examples):
  - [parse_manifest_file](../p/parse_manifest_file.md) (in pg_verifybackup)
  - [load_backup_manifest](../l/load_backup_manifest.md) (in pg_combinebackup)
  - basebackup incremental processing functions

## Notes and Other Information
- The context structure is designed to be stack-allocated and initialized by the calling application
- All callback functions are required except private_data which can be NULL if no state tracking is needed
- The error callback must not return (marked with pg_attribute_noreturn) as it indicates a fatal parsing error
- Used extensively in PostgreSQL's backup and recovery tools for processing backup metadata
- Supports both single-pass parsing (json_parse_manifest) and incremental/streaming parsing for large manifests
- The callback-based design allows for memory-efficient processing of large backup manifests without loading entire file lists into memory