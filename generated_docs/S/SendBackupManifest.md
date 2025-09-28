# SendBackupManifest

## Location
[src/backend/backup/backup_manifest.c:316-382](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/backup_manifest.c#L316-L382)

## Overview
Finalizes the backup manifest by computing its checksum and sends the completed manifest to the client via a backup sink.

## Definition
```c
void SendBackupManifest(backup_manifest_info *manifest, bbsink *sink)
```

## Detailed Description
`SendBackupManifest` is responsible for completing the backup manifest generation process. It performs several critical tasks:

1. **Checksum Finalization**: Computes a SHA-256 checksum of the entire manifest content to ensure integrity. This checksum is always SHA-256 regardless of the algorithm used for individual files.

2. **Manifest Completion**: Appends the computed checksum to the manifest in JSON format as "Manifest-Checksum" field, then closes the JSON structure.

3. **File Transmission**: Rewinds the manifest buffer file and streams its contents to the client through the provided backup sink in chunks.

4. **Resource Cleanup**: Closes the manifest buffer file and releases associated resources.

The function uses a streaming approach to handle potentially large manifest files efficiently, reading and sending data in buffer-sized chunks rather than loading the entire manifest into memory.

## Parameters / Member Variables
- `manifest`: Pointer to backup_manifest_info structure containing manifest state, buffer file, checksum context, and size information
- `sink`: Pointer to bbsink structure that handles the actual transmission of manifest data to the client

## Dependencies
- Functions called/Symbols referenced:
  - [IsManifestEnabled](../I/IsManifestEnabled.md)
  - [pg_cryptohash_final](../p/pg_cryptohash_final.md)
  - [pg_cryptohash_error](../p/pg_cryptohash_error.md)
  - [AppendStringToManifest](../A/AppendStringToManifest.md)
  - [hex_encode](../h/hex_encode.md)
  - [BufFileSeek](../B/BufFileSeek.md)
  - [BufFileReadExact](../B/BufFileReadExact.md)
  - [BufFileClose](../B/BufFileClose.md)
  - [bbsink_begin_manifest](../b/bbsink_begin_manifest.md)
  - [bbsink_manifest_contents](../b/bbsink_manifest_contents.md)
  - [bbsink_end_manifest](../b/bbsink_end_manifest.md)
- Called from (representative examples):
  - [perform_base_backup](../p/perform_base_backup.md) (src/backend/backup/basebackup.c:648)

## Notes and Other Information
- The function returns early if manifest generation is disabled via IsManifestEnabled check
- SHA-256 is hardcoded for manifest checksums to avoid requiring clients to determine the algorithm dynamically
- Uses a streaming approach with buffered I/O to handle large manifests efficiently
- The manifest file is rewound after writing to prepare for reading and transmission
- Error handling includes specific messages for checksum finalization failures and file access issues

## Simplified Source

```c
// Simplified version of SendBackupManifest
void SendBackupManifest(backup_manifest_info *manifest, bbsink *sink) {
    uint8 checksumbuf[PG_SHA256_DIGEST_LENGTH];
    char checksumstringbuf[PG_SHA256_DIGEST_STRING_LENGTH];
    size_t manifest_bytes_done = 0;

    // Early exit if manifest is disabled
    if (!IsManifestEnabled(manifest))
        return;

    // Finalize manifest checksum (always SHA-256)
    manifest->still_checksumming = false;
    if (pg_cryptohash_final(manifest->manifest_ctx, checksumbuf,
                            sizeof(checksumbuf)) < 0)
        elog(ERROR, "failed to finalize checksum of backup manifest: %s",
             pg_cryptohash_error(manifest->manifest_ctx));

    // Append checksum to manifest
    AppendStringToManifest(manifest, "\"Manifest-Checksum\": \"");
    hex_encode((char *) checksumbuf, sizeof checksumbuf, checksumstringbuf);
    checksumstringbuf[PG_SHA256_DIGEST_STRING_LENGTH - 1] = '\0';
    AppendStringToManifest(manifest, checksumstringbuf);
    AppendStringToManifest(manifest, "\"}\n");

    // Rewind file for reading and transmission
    if (BufFileSeek(manifest->buffile, 0, 0L, SEEK_SET) != 0)
        elog(ERROR, "could not rewind temporary file: %m");

    // Begin manifest transmission
    bbsink_begin_manifest(sink);

    // Stream manifest contents in chunks
    while (manifest_bytes_done < manifest->manifest_size) {
        char buffer[65536];
        size_t bytes_to_read = Min(sizeof(buffer),
                                   manifest->manifest_size - manifest_bytes_done);

        BufFileReadExact(manifest->buffile, buffer, bytes_to_read);
        bbsink_manifest_contents(sink, buffer, bytes_to_read);
        manifest_bytes_done += bytes_to_read;
    }

    // Complete manifest transmission
    bbsink_end_manifest(sink);

    // Cleanup
    BufFileClose(manifest->buffile);
    manifest->buffile = NULL;
}
```

Key simplifications made:
- Removed detailed comments while preserving core logic
- Consolidated error handling pattern
- Simplified the streaming loop structure
- Maintained proper resource cleanup
- Preserved the essential checksum and transmission functionality