# UploadManifest

## Location
[src/backend/replication/walsender.c:683-746](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/walsender.c#L683-L746)

## Overview
Handles the UPLOAD_MANIFEST replication command by receiving incremental backup manifest data from the client via PostgreSQL's COPY protocol and processing it for subsequent incremental backup operations.

## Definition

```c
static void
UploadManifest(void)
```
## Detailed Description
UploadManifest implements the server-side handling of the UPLOAD_MANIFEST replication command, which is part of PostgreSQL's incremental backup functionality. The function:

1. Sets up a resource owner and memory context for manifest processing
2. Sends a CopyInResponse message to initiate COPY protocol communication
3. Receives manifest data packets from the client using HandleUploadManifestPacket
4. Finalizes the manifest processing and stores it in a persistent memory context
5. Cleans up old manifest data and resources

The function uses PostgreSQL's COPY protocol to efficiently transfer potentially large manifest files from backup clients. The manifest contains metadata about files in previous backups, enabling incremental backup operations by identifying which files have changed.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [ResourceOwnerCreate](../R/ResourceOwnerCreate.md)
  - AllocSetContextCreate
  - CreateIncrementalBackupInfo
  - [pq_beginmessage](../p/pq_beginmessage.md)
  - [pq_sendbyte](../p/pq_sendbyte.md)
  - [pq_sendint16](../p/pq_sendint16.md)
  - [pq_endmessage_reuse](../p/pq_endmessage_reuse.md)
  - pq_flush
  - [HandleUploadManifestPacket](../H/HandleUploadManifestPacket.md)
  - [FinalizeIncrementalManifest](../F/FinalizeIncrementalManifest.md)
  - [MemoryContextDelete](../M/MemoryContextDelete.md)
  - [MemoryContextSetParent](../M/MemoryContextSetParent.md)
  - [WalSndResourceCleanup](../W/WalSndResourceCleanup.md)
- Called from:
  - [exec_replication_command](../e/exec_replication_command.md)

## Notes and Other Information
- The function is static and only used within the walsender module
- Requires a resource owner for cryptohash operations during manifest parsing
- Uses a temporary memory context that is later reparented to CacheMemoryContext for persistence
- Manages global variables uploaded_manifest and uploaded_manifest_mcxt to store the processed manifest
- Part of PostgreSQL's incremental backup infrastructure introduced for efficient backup operations
- The manifest data received contains file metadata that helps determine which files need to be included in incremental backups

## Simplified Source

```c
// Simplified version of UploadManifest
static void
UploadManifest(void)
{
    MemoryContext manifest_context;
    IncrementalBackupInfo *backup_info;
    off_t data_offset = 0;
    StringInfoData message_buffer;

    // Step 1: Set up resource owner for cryptohash operations
    Assert(CurrentResourceOwner == NULL);
    CurrentResourceOwner = ResourceOwnerCreate(NULL, "base backup");

    // Step 2: Create memory context and backup info structure
    manifest_context = AllocSetContextCreate(CurrentMemoryContext,
                                           "incremental backup information",
                                           ALLOCSET_DEFAULT_SIZES);
    backup_info = CreateIncrementalBackupInfo(manifest_context);

    // Step 3: Send CopyInResponse to initiate COPY protocol
    pq_beginmessage(&message_buffer, PqMsg_CopyInResponse);
    pq_sendbyte(&message_buffer, 0);      // Binary format
    pq_sendint16(&message_buffer, 0);     // No column format codes
    pq_endmessage_reuse(&message_buffer);
    pq_flush();

    // Step 4: Receive and process manifest data packets from client
    while (HandleUploadManifestPacket(&message_buffer, &data_offset, backup_info)) {
        // Continue receiving packets until done
    }

    // Step 5: Complete manifest processing
    FinalizeIncrementalManifest(backup_info);

    // Step 6: Replace old manifest with new one
    if (uploaded_manifest_mcxt != NULL) {
        MemoryContextDelete(uploaded_manifest_mcxt);  // Clean up old manifest
    }
    MemoryContextSetParent(manifest_context, CacheMemoryContext);  // Make persistent
    uploaded_manifest = backup_info;
    uploaded_manifest_mcxt = manifest_context;

    // Step 7: Clean up resources
    WalSndResourceCleanup(true);
}
```

Key simplifications made:
- Used more descriptive variable names (manifest_context, backup_info, message_buffer)
- Added step-by-step comments explaining the main phases
- Simplified the COPY protocol setup explanation
- Consolidated the manifest replacement logic with clearer comments
- Focused on the main execution path while preserving all essential operations