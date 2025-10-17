# ReceiveArchiveStreamChunk

## Location
[src/bin/pg_basebackup/pg_basebackup.c:1332-1515](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/pg_basebackup.c#L1332-L1515)

## Overview
Processes individual data chunks received as part of a COPY stream during archive reception, handling different message types for archives, manifest data, and progress reports.

## Definition

```c
enum > 0)
					progress_report(state->tablespacenum, true, false);
```
## Detailed Description
ReceiveArchiveStreamChunk is a callback function that processes individual chunks of data received through the COPY protocol during base backup operations. It acts as a message dispatcher, examining the type byte of each CopyData message and routing the processing accordingly.

The function handles five distinct message types:
- 'n' (New archive): Initializes processing for a new archive, including tablespace validation and streamer setup
- 'd' (Data): Processes actual archive or manifest content data
- 'p' (Progress): Updates progress tracking with byte counts from the server
- 'm' (Manifest): Prepares for receiving backup manifest data
- Default: Reports parsing errors for unrecognized message types

The function manages the complete lifecycle of archive processing, from initialization through data streaming to cleanup, while maintaining proper state transitions and error handling.

## Parameters / Member Variables
- `tablespacenum`: Size of the data chunk received in the copybuf
- `true`: Buffer containing the raw COPY data received from the server
- `false`: Void pointer to ArchiveStreamState structure containing processing state
## Dependencies
- Functions called/Symbols referenced:
  - [GetCopyDataByte](../G/GetCopyDataByte.md)
  - [GetCopyDataString](../G/GetCopyDataString.md)
  - [GetCopyDataUInt64](../G/GetCopyDataUInt64.md)
  - [GetCopyDataEnd](../G/GetCopyDataEnd.md)
  - [progress_report](../p/progress_report.md)
  - [bbstreamer_finalize](../b/bbstreamer_finalize.md)
  - [bbstreamer_free](../b/bbstreamer_free.md)
  - [bbstreamer_content](../b/bbstreamer_content.md)
  - [CreateBackupStreamer](../C/CreateBackupStreamer.md)
  - [ReportCopyDataParseError](ReportCopyDataParseError.md)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - [createPQExpBuffer](../c/createPQExpBuffer.md)
  - fopen
  - fwrite
- Called from (representative examples):
  - [ReceiveArchiveStream](ReceiveArchiveStream.md)
  - CompressionLocation

## Notes and Other Information
- This function implements a state machine that processes different phases of archive reception
- [Archive](../A/Archive.md) name validation prevents directory traversal and ensures safe file handling
- Progress reporting is forced on each tablespace transition and server progress message
- Manifest data can be either buffered in memory for injection into tarfiles or written directly to disk
- The function assumes PostgreSQL v15+ protocol features (recovery GUCs support)
- Proper error handling includes setting errno to ENOSPC when fwrite() fails without setting errno
- State management ensures that archives are processed before manifest data (sanity check enforced)

## Simplified Source

```c
static void
ReceiveArchiveStreamChunk(size_t r, char *copybuf, void *callback_data)
{
    ArchiveStreamState *state = callback_data;
    size_t cursor = 0;

    // Process message based on type byte
    switch (GetCopyDataByte(r, copybuf, &cursor))
    {
        case 'n':  // New archive
        {
            char *archive_name, *spclocation;

            // Update progress for previous tablespace
            if (++state->tablespacenum > 0)
                progress_report(state->tablespacenum, true, false);

            // Parse archive information
            archive_name = GetCopyDataString(r, copybuf, &cursor);
            spclocation = GetCopyDataString(r, copybuf, &cursor);
            GetCopyDataEnd(r, copybuf, cursor);

            // Validate archive name
            if (archive_name[0] == '\0' || archive_name[0] == '.' ||
                strchr(archive_name, '/') != NULL ||
                strchr(archive_name, '\\') != NULL)
                pg_fatal("invalid archive name: \"%s\"", archive_name);

            // Handle empty spclocation
            if (spclocation[0] == '\0')
                spclocation = NULL;

            // Cleanup previous streamer
            if (state->streamer != NULL) {
                bbstreamer_finalize(state->streamer);
                bbstreamer_free(state->streamer);
                state->streamer = NULL;
            }

            // Create new backup streamer if needed
            if (backup_target == NULL) {
                state->streamer = CreateBackupStreamer(archive_name, spclocation,
                                                     &state->manifest_inject_streamer,
                                                     true, false, state->compress);
            }
            break;
        }

        case 'd':  // Data content
        {
            if (state->manifest_buffer != NULL) {
                // Buffer manifest data in memory
                appendPQExpBuffer(state->manifest_buffer, copybuf + 1, r - 1);
            }
            else if (state->manifest_file != NULL) {
                // Write manifest data to file
                if (fwrite(copybuf + 1, r - 1, 1, state->manifest_file) != 1) {
                    if (errno == 0) errno = ENOSPC;
                    pg_fatal("could not write to file \"%s\": %m", state->manifest_filename);
                }
            }
            else if (state->streamer != NULL) {
                // Stream archive data
                bbstreamer_content(state->streamer, NULL, copybuf + 1, r - 1, BBSTREAMER_UNKNOWN);
            }
            else {
                pg_fatal("unexpected payload data");
            }
            break;
        }

        case 'p':  // Progress report
        {
            totaldone = GetCopyDataUInt64(r, copybuf, &cursor);
            GetCopyDataEnd(r, copybuf, cursor);
            progress_report(state->tablespacenum, true, false);
            break;
        }

        case 'm':  // Manifest preparation
        {
            GetCopyDataEnd(r, copybuf, cursor);

            if (backup_target == NULL) {
                if (state->manifest_inject_streamer != NULL) {
                    // Buffer manifest in memory for injection
                    state->manifest_buffer = createPQExpBuffer();
                }
                else {
                    // Write manifest to temporary file
                    snprintf(state->manifest_filename, sizeof(state->manifest_filename),
                            "%s/backup_manifest.tmp", basedir);
                    state->manifest_file = fopen(state->manifest_filename, "wb");
                    if (state->manifest_file == NULL)
                        pg_fatal("could not create file \"%s\": %m", state->manifest_filename);
                }
            }
            break;
        }

        default:
            ReportCopyDataParseError(r, copybuf);
            break;
    }
}
```