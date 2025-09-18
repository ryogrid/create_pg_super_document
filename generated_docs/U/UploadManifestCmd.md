# UploadManifestCmd

## Location
[src/include/nodes/replnodes.h:127-130](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/replnodes.h#L127-L130)

## Overview
UploadManifestCmd is a command structure used for uploading manifest files in PostgreSQL's replication system, likely related to backup and recovery operations.

## Definition
```c
typedef struct UploadManifestCmd
{
    NodeTag      type;
} UploadManifestCmd;
```

## Detailed Description
UploadManifestCmd represents the UPLOAD_MANIFEST command in PostgreSQL's streaming replication protocol. This structure appears to be a minimal command that triggers the upload of a manifest file, which typically contains metadata about backup files, WAL segments, or other replication-related resources. The command structure only contains the basic NodeTag identifier, suggesting it's a simple trigger command without additional parameters.

## Parameters / Member Variables
- `type`: NodeTag identifier for this command structure

## Dependencies
- Functions called/Symbols referenced:
  - (No direct symbol references)
- Called from (representative examples):
  - (No current references found in the codebase)

## Notes and Other Information
- This appears to be a recently added or specialized command with minimal current usage
- Manifest files in PostgreSQL context typically contain metadata for backup/recovery operations
- The simple structure suggests this is a trigger command that initiates a predefined upload process
- May be part of newer backup and recovery functionality or cloud-related features
- Part of the replication protocol command set defined in replnodes.h