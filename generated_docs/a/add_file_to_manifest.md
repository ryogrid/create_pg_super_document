# add_file_to_manifest

## Location
[src/bin/pg_combinebackup/write_manifest.c:76-141](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_combinebackup/write_manifest.c#L76-L141)

## Overview
Adds a file entry to the backup manifest, encoding file metadata including path, size, modification time, and optional checksum information in JSON format.

## Definition

```c
void
add_file_to_manifest(manifest_writer *mwriter, const char *manifest_path,
					 size_t size, time_t mtime,
					 pg_checksum_type checksum_type,
					 int checksum_length,
					 uint8 *checksum_payload)
```
## Detailed Description
This function adds a complete file entry to the backup manifest being built by the manifest writer. It handles the JSON formatting for file metadata, including proper encoding of file paths (UTF-8 validation and hex encoding for non-UTF-8 paths), file size, modification timestamp, and optional checksum information. The function manages JSON syntax by tracking whether this is the first file entry and adding appropriate separators.

Key features include:
- UTF-8 path validation with fallback to hex-encoded paths for non-UTF-8 filenames
- ISO 8601 timestamp formatting for modification times
- Hex encoding of checksum payloads when present
- Automatic buffer flushing when the accumulated JSON exceeds 128KB
- Proper JSON comma separation between file entries

## Parameters / Member Variables
- : Manifest writer structure maintaining the JSON build state
- : Relative path of the file within the backup
- : File size in bytes
- : File modification time as Unix timestamp
- : Type of checksum algorithm used (if any)
- : Length of the checksum payload in bytes
- : Binary checksum data to be hex-encoded

## Dependencies
- Functions called/Symbols referenced:
  - [manifest_writer](../m/manifest_writer.md) (structure type)
  - pg_checksum_type (enum type)
  - [pg_encoding_verifymbstr](../p/pg_encoding_verifymbstr.md) (UTF-8 validation)
  - PG_UTF8 (encoding constant)
  - [escape_json](../e/escape_json.md) (JSON string escaping)
  - [enlargeStringInfo](../e/enlargeStringInfo.md) (buffer management)
  - [hex_encode](../h/hex_encode.md) (binary to hex conversion)
  - strftime (timestamp formatting)
  - [flush_manifest](../f/flush_manifest.md) (buffer flushing)
  - [pg_checksum_type_name](../p/pg_checksum_type_name.md) (checksum algorithm name)
- Called from (representative examples):
  - [write_backup_label](../w/write_backup_label.md) (in src/bin/pg_combinebackup/backup_label.c:188)
  - [process_directory_recursively](../p/process_directory_recursively.md) (in src/bin/pg_combinebackup/pg_combinebackup.c:1134)

## Notes and Other Information
- The function automatically flushes the manifest buffer when it exceeds 128KB to manage memory usage
- Non-UTF-8 file paths are hex-encoded and stored in an "Encoded-Path" field instead of "Path"
- Timestamps are formatted in GMT using the ISO format "%Y-%m-%d %H:%M:%S %Z"
- Checksum information is optional - if checksum_length is 0, no checksum fields are added
- This is similar to the backend's AddFileToBackupManifest but adapted for frontend use
- The JSON structure follows PostgreSQL's backup manifest specification