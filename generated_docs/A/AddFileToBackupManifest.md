# AddFileToBackupManifest

## Location
src/backend/backup/backup_manifest.c: 101 - 211

## Overview
Adds a file entry to the backup manifest with metadata including path, size, modification time, and checksum information formatted as JSON.

## Definition
```c
void AddFileToBackupManifest(backup_manifest_info *manifest, Oid spcoid,
                            const char *pathname, size_t size, pg_time_t mtime,
                            pg_checksum_context *checksum_ctx)
```

## Detailed Description
AddFileToBackupManifest creates and appends a JSON object representing a single file to the backup manifest. The function handles tablespace path conversion, UTF-8 encoding validation, JSON formatting with proper comma separation, timestamp formatting in GMT, and checksum information encoding. For tablespace files, it converts the relative path to a data-directory-relative format using the pg_tblspc/OID prefix. The function ensures proper JSON structure by managing commas between entries and handles non-UTF-8 filenames by hex-encoding them with an "Encoded-Path" field instead of the standard "Path" field.

## Parameters / Member Variables
- `manifest`: Pointer to backup_manifest_info structure for the active backup manifest
- `spcoid`: Object identifier for the tablespace (InvalidOid for non-tablespace files)
- `pathname`: Relative path to the file being added to the manifest
- `size`: Size of the file in bytes
- `mtime`: Last modification time of the file
- `checksum_ctx`: Context containing the computed checksum for the file

## Dependencies
- Functions called/Symbols referenced:
  - IsManifestEnabled (manifest enablement check)
  - OidIsValid (PostgreSQL OID validation)
  - snprintf (C standard library)
  - initStringInfo, appendStringInfo* (PostgreSQL string buffer management)
  - pg_verify_mbstr (PostgreSQL multibyte string validation)
  - escape_json (PostgreSQL JSON escaping)
  - hex_encode (PostgreSQL hexadecimal encoding)
  - pg_strftime, pg_gmtime (PostgreSQL time formatting)
  - pg_checksum_final, pg_checksum_type_name (PostgreSQL checksum functions)
  - AppendStringToManifest (internal manifest writing)
  - pfree (PostgreSQL memory management)
- Called from (representative examples):
  - sendFileWithContent (src/backend/backup/basebackup.c:1122)
  - sendFile (src/backend/backup/basebackup.c:1823)

## Notes and Other Information
- Returns early if manifest generation is disabled via IsManifestEnabled check
- Uses GMT timezone consistently for timestamp formatting to avoid confusion with changing timezone definitions
- Handles non-UTF-8 filenames by hex-encoding them and using "Encoded-Path" instead of "Path" in JSON
- Manages JSON comma separation by tracking first_file state to ensure proper formatting
- Supports all PostgreSQL checksum types, including CHECKSUM_TYPE_NONE for files without checksums
- Tablespace files are converted to data-directory-relative paths using pg_tblspc/OID format
- All string operations use PostgreSQL's StringInfo system for efficient buffer management