# parse_basebackup_options

## Location
[src/backend/backup/basebackup.c:696-987](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup.c#L696-L987)

## Overview
 parses and validates the base backup options passed down by the SQL parser, populating a basebackup_options structure with the parsed values.

## Definition


## Detailed Description
This function processes a list of DefElem structures containing base backup options and converts them into a structured basebackup_options configuration. It handles all standard base backup options including label, progress reporting, checkpoint behavior, WAL inclusion, compression, manifest generation, and target specification. The function performs comprehensive validation, checking for duplicate options, valid value ranges, and logical consistency between related options.

Key features include:
- Duplicate option detection for all parameters
- Value validation and range checking (e.g., max_rate bounds)
- Cross-option validation (e.g., incremental backups requiring WAL summarization)
- Default value assignment for unspecified options
- Target system configuration (client vs external targets)
- Compression specification parsing and validation

The function ensures the basebackup_options structure is properly initialized with defaults before processing options, and performs comprehensive error reporting with specific error codes.

## Parameters / Member Variables
- : List of DefElem structures containing the raw option specifications from SQL parsing
- : Output basebackup_options structure to be populated with parsed and validated options

## Dependencies
- Functions called/Symbols referenced:
  - [defGetString](../d/defGetString.md)
  - [defGetBoolean](../d/defGetBoolean.md)  
  - [defGetInt64](../d/defGetInt64.md)
  - [parse_bool](parse_bool.md)
  - parse_compress_algorithm
  - parse_compress_specification
  - [validate_compress_specification](../v/validate_compress_specification.md)
  - pg_checksum_parse_type
  - [BaseBackupGetTargetHandle](../B/BaseBackupGetTargetHandle.md)
- Called from (representative examples):
  - [SendBaseBackup](../S/SendBaseBackup.md)

## Notes and Other Information
- Initializes basebackup_options with sensible defaults: CRC32C manifest checksums, no compression, "base backup" label
- Supports both boolean manifest options (yes/no) and special "force-encode" mode for testing
- Validates incremental backup prerequisites (WAL summarization must be enabled)
- Handles complex target system configuration including client delivery vs external target systems
- Provides detailed error messages with appropriate error codes for all validation failures
- The MAX_RATE_LOWER and MAX_RATE_UPPER constants define acceptable throttling limits