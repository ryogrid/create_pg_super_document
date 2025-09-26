# HandleUploadManifestPacket

## Location
[src/backend/replication/walsender.c:747-822](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/walsender.c#L747-L822)

## Overview
Processes individual packets received during an UPLOAD_MANIFEST operation, handling the COPY protocol messages and appending manifest data to the incremental backup information structure.

## Definition

```c
static bool
HandleUploadManifestPacket(StringInfo buf, off_t *offset,
						   IncrementalBackupInfo *ib)
```
## Detailed Description
HandleUploadManifestPacket is a helper function that processes one packet at a time during the UPLOAD_MANIFEST replication command. It implements the server-side handling of PostgreSQL's COPY protocol for receiving manifest data:

1. Reads and validates incoming message types from the client
2. Handles different COPY protocol message types (CopyData, CopyDone, CopyFail, Flush, Sync)
3. For CopyData messages, appends the received data to the incremental backup manifest
4. Returns a boolean indicating whether more packets should be processed

The function uses interrupt handling to ensure safe processing of network messages and proper error reporting for protocol violations or connection failures.

## Parameters / Member Variables
- : A StringInfo buffer used as scratch space for message processing; contents are overwritten by the function
- : A pointer to an offset value (parameter appears unused in the current implementation)
- : Pointer to an IncrementalBackupInfo structure where manifest data is accumulated

## Dependencies
- Functions called/Symbols referenced:
  - HOLD_CANCEL_INTERRUPTS
  - [pq_startmsgread](../p/pq_startmsgread.md)
  - [pq_getbyte](../p/pq_getbyte.md)
  - [pq_getmessage](../p/pq_getmessage.md)
  - RESUME_CANCEL_INTERRUPTS
  - [AppendIncrementalManifestData](../A/AppendIncrementalManifestData.md)
  - [pq_getmsgstring](../p/pq_getmsgstring.md)
  - ereport
- Called from:
  - [UploadManifest](../U/UploadManifest.md)

## Notes and Other Information
- The function is static and only used within the walsender module
- Returns true to continue processing more packets, false when UPLOAD_MANIFEST operation is complete
- Handles COPY protocol message size limits (PQ_LARGE_MESSAGE_LIMIT for data, PQ_SMALL_MESSAGE_LIMIT for control messages)
- Implements proper error handling for connection failures and protocol violations
- Ignores Flush and Sync messages during COPY mode, consistent with other PostgreSQL COPY implementations
- Uses interrupt protection to ensure atomic message processing
- The offset parameter appears to be included for interface consistency but is not currently used in the implementation