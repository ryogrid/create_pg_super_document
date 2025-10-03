# parse_basebackup_options

## Location
[src/backend/backup/basebackup.c:696-987](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup.c#L696-L987)

## Overview
 parses and validates the base backup options passed down by the SQL parser, populating a basebackup_options structure with the parsed values.

## Definition

```c
static void
parse_basebackup_options(List *options, basebackup_options *opt)
```
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
- `*options`: List of DefElem structures containing the raw option specifications from SQL parsing
- `*opt`: Output basebackup_options structure to be populated with parsed and validated options
## Dependencies
- Functions called/Symbols referenced:
  - [defGetString](../d/defGetString.md)
  - [defGetBoolean](../d/defGetBoolean.md)  
  - [defGetInt64](../d/defGetInt64.md)
  - [parse_bool](parse_bool.md)
  - [parse_compress_algorithm](parse_compress_algorithm.md)
  - [parse_compress_specification](parse_compress_specification.md)
  - [validate_compress_specification](../v/validate_compress_specification.md)
  - [pg_checksum_parse_type](pg_checksum_parse_type.md)
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

## Simplified Source

```c
// Simplified version of parse_basebackup_options
static void parse_basebackup_options(List *options, basebackup_options *opt) {
    ListCell *lopt;
    // Track which options have been specified to detect duplicates
    bool option_flags[16] = {false}; // Simplified tracking
    char *target_str = NULL;
    char *target_detail_str = NULL;
    char *compression_detail_str = NULL;

    // Initialize options structure with defaults
    MemSet(opt, 0, sizeof(*opt));
    opt->manifest = MANIFEST_OPTION_NO;
    opt->manifest_checksum_type = CHECKSUM_TYPE_CRC32C;
    opt->compression = PG_COMPRESSION_NONE;
    opt->compression_specification.algorithm = PG_COMPRESSION_NONE;

    // Process each option in the list
    foreach(lopt, options) {
        DefElem *defel = (DefElem *) lfirst(lopt);

        // Parse individual options with duplicate checking
        if (strcmp(defel->defname, "label") == 0) {
            check_duplicate_option("label", option_flags[0]);
            opt->label = defGetString(defel);
            option_flags[0] = true;
        }
        else if (strcmp(defel->defname, "progress") == 0) {
            check_duplicate_option("progress", option_flags[1]);
            opt->progress = defGetBoolean(defel);
            option_flags[1] = true;
        }
        // ... similar pattern for all other options ...
        // checkpoint, wait, wal, incremental, max_rate, tablespace_map,
        // verify_checksums, manifest, manifest_checksums, target,
        // target_detail, compression, compression_detail
        else {
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg("unrecognized base backup option: \"%s\"", defel->defname)));
        }
    }

    // Apply defaults and validate cross-option consistency
    if (opt->label == NULL)
        opt->label = "base backup";

    // Validate manifest and checksum consistency
    if (opt->manifest == MANIFEST_OPTION_NO && manifest_checksums_specified)
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                errmsg("manifest checksums require a backup manifest")));

    // Configure target system (client vs external)
    if (target_str == NULL) {
        if (target_detail_str != NULL)
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg("target detail cannot be used without target")));
        opt->use_copytblspc = true;
        opt->send_to_client = true;
    }
    else if (strcmp(target_str, "client") == 0) {
        opt->send_to_client = true;
    }
    else {
        opt->target_handle = BaseBackupGetTargetHandle(target_str, target_detail_str);
    }

    // Parse and validate compression specification
    if (compression_specified) {
        parse_compress_specification(opt->compression, compression_detail_str,
                                   &opt->compression_specification);
        char *error_detail = validate_compress_specification(&opt->compression_specification);
        if (error_detail != NULL)
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg("invalid compression specification: %s", error_detail)));
    }
}

// Helper function for duplicate option checking
static void check_duplicate_option(const char *option_name, bool already_seen) {
    if (already_seen)
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                errmsg("duplicate option \"%s\"", option_name)));
}
```

Key simplifications made:
- Consolidated duplicate option checking into a helper pattern
- Simplified the option parsing loop structure while preserving all validation
- Maintained all essential validation logic and error handling
- Preserved cross-option consistency checks
- Kept target system configuration intact
- Maintained compression parsing and validation
- Used simplified tracking arrays instead of individual boolean variables
- Preserved all default value assignments and error reporting