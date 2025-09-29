# AddFileToBackupManifest

## Location
[src/backend/backup/backup_manifest.c:101-211](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/backup_manifest.c#L101-L211)

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
  - [IsManifestEnabled](../I/IsManifestEnabled.md) (manifest enablement check)
  - OidIsValid (PostgreSQL OID validation)
  - snprintf (C standard library)
  - [initStringInfo](../i/initStringInfo.md), appendStringInfo* (PostgreSQL string buffer management)
  - [pg_verify_mbstr](../p/pg_verify_mbstr.md) (PostgreSQL multibyte string validation)
  - [escape_json](../e/escape_json.md) (PostgreSQL JSON escaping)
  - [hex_encode](../h/hex_encode.md) (PostgreSQL hexadecimal encoding)
  - [pg_strftime](../p/pg_strftime.md), pg_gmtime (PostgreSQL time formatting)
  - [pg_checksum_final](../p/pg_checksum_final.md), pg_checksum_type_name (PostgreSQL checksum functions)
  - [AppendStringToManifest](AppendStringToManifest.md) (internal manifest writing)
  - [pfree](../p/pfree.md) (PostgreSQL memory management)
- Called from (representative examples):
  - [sendFileWithContent](../s/sendFileWithContent.md) (src/backend/backup/basebackup.c:1122)
  - [sendFile](../s/sendFile.md) (src/backend/backup/basebackup.c:1823)

## Notes and Other Information
- Returns early if manifest generation is disabled via IsManifestEnabled check
- Uses GMT timezone consistently for timestamp formatting to avoid confusion with changing timezone definitions
- Handles non-UTF-8 filenames by hex-encoding them and using "Encoded-Path" instead of "Path" in JSON
- Manages JSON comma separation by tracking first_file state to ensure proper formatting
- Supports all PostgreSQL checksum types, including CHECKSUM_TYPE_NONE for files without checksums
- Tablespace files are converted to data-directory-relative paths using pg_tblspc/OID format
- All string operations use PostgreSQL's StringInfo system for efficient buffer management

## Simplified Source

```c
// Add file entry to backup manifest as JSON object
void AddFileToBackupManifest(backup_manifest_info *manifest, Oid spcoid,
                             const char *pathname, size_t size, pg_time_t mtime,
                             pg_checksum_context *checksum_ctx)
{
    char pathbuf[MAXPGPATH];
    int pathlen;
    StringInfoData buf;

    if (!IsManifestEnabled(manifest))
        return;

    // Convert tablespace file paths to data directory relative format
    if (OidIsValid(spcoid)) {
        snprintf(pathbuf, sizeof(pathbuf), "pg_tblspc/%u/%s", spcoid, pathname);
        pathname = pathbuf;
    }

    // Manage JSON comma separation between entries
    initStringInfo(&buf);
    if (manifest->first_file) {
        appendStringInfoChar(&buf, '\n');
        manifest->first_file = false;
    } else {
        appendStringInfoString(&buf, ",\n");
    }

    // Handle path encoding (UTF-8 vs hex-encoded)
    pathlen = strlen(pathname);
    if (!manifest->force_encode && pg_verify_mbstr(PG_UTF8, pathname, pathlen, true)) {
        // Standard UTF-8 path
        appendStringInfoString(&buf, "{ \"Path\": ");
        escape_json(&buf, pathname);
        appendStringInfoString(&buf, ", ");
    } else {
        // Non-UTF-8 path - hex encode it
        appendStringInfoString(&buf, "{ \"Encoded-Path\": \"");
        enlargeStringInfo(&buf, 2 * pathlen);
        buf.len += hex_encode(pathname, pathlen, &buf.data[buf.len]);
        appendStringInfoString(&buf, "\", ");
    }

    // Add file size
    appendStringInfo(&buf, "\"Size\": %zu, ", size);

    // Add last modification time in GMT
    appendStringInfoString(&buf, "\"Last-Modified\": \"");
    enlargeStringInfo(&buf, 128);
    buf.len += pg_strftime(&buf.data[buf.len], 128, "%Y-%m-%d %H:%M:%S %Z",
                          pg_gmtime(&mtime));
    appendStringInfoChar(&buf, '"');

    // Add checksum information if present
    if (checksum_ctx->type != CHECKSUM_TYPE_NONE) {
        uint8 checksumbuf[PG_CHECKSUM_MAX_LENGTH];
        int checksumlen;

        checksumlen = pg_checksum_final(checksum_ctx, checksumbuf);
        if (checksumlen < 0)
            elog(ERROR, "could not finalize checksum of file \"%s\"", pathname);

        appendStringInfo(&buf, ", \"Checksum-Algorithm\": \"%s\", \"Checksum\": \"",
                        pg_checksum_type_name(checksum_ctx->type));
        enlargeStringInfo(&buf, 2 * checksumlen);
        buf.len += hex_encode((char *) checksumbuf, checksumlen, &buf.data[buf.len]);
        appendStringInfoChar(&buf, '"');
    }

    // Close JSON object and append to manifest
    appendStringInfoString(&buf, " }");
    AppendStringToManifest(manifest, buf.data);

    // Clean up memory
    pfree(buf.data);
}
```

**Key Points:**
- Creates JSON entries for backup manifest with file metadata
- Handles tablespace paths by converting to pg_tblspc/OID format
- Manages UTF-8 encoding vs hex-encoding for non-UTF-8 filenames
- Uses GMT timezone consistently for timestamps
- Includes optional checksum information when available
- Manages JSON comma separation between file entries