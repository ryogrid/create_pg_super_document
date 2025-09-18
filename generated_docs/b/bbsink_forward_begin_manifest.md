# bbsink_forward_begin_manifest

## Location
[src/backend/backup/basebackup_sink.c:76-88](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_sink.c#L76-L88)

## Overview
Forwards the begin_manifest callback to the next bbsink in a chain, enabling the initiation of manifest processing in a cascaded bbsink architecture.

## Definition
```c
void bbsink_forward_begin_manifest(bbsink *sink)
```

## Detailed Description
This function implements a forwarding pattern for the begin_manifest callback within PostgreSQL's base backup sink infrastructure. It is designed to propagate the manifest initialization signal through a chain of bbsink implementations. When called, it forwards the begin_manifest operation to the next bbsink in the chain (sink->bbs_next), ensuring that all bbsinks in the chain are properly notified when manifest processing begins.

The manifest in PostgreSQL's backup system contains metadata about the backup, including file listings, checksums, and other backup-related information. This forwarding mechanism allows multiple bbsink implementations to be chained together where each can perform specific operations related to manifest handling while ensuring that the manifest initialization is properly propagated through the entire chain. This is particularly useful for bbsinks that need to prepare for manifest processing or perform setup operations before the actual manifest data is processed.

## Parameters / Member Variables
- `sink`: Pointer to the bbsink structure that is forwarding the begin_manifest operation to its successor

## Dependencies
- Functions called/Symbols referenced:
  - bbsink_begin_manifest
  - bbsink (type reference)
- Called from (representative examples):
  - [bbsink_server_begin_manifest](bbsink_server_begin_manifest.md) (src/backend/backup/basebackup_server.c:246)

## Notes and Other Information
- The function performs an assertion to ensure that bbs_next is properly initialized before forwarding
- This is a utility function that simplifies the implementation of bbsink types that need to forward manifest operations
- The manifest processing is a distinct phase in the backup process, separate from archive processing
- This forwarding pattern is essential for maintaining the chain of responsibility in composite bbsink architectures during manifest handling
- The function enables coordinated manifest processing across multiple bbsink implementations in a pipeline