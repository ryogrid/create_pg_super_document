# bbsink_forward_begin_archive

## Location
src/backend/backup/basebackup_sink.c: 37 - 53

## Overview
Forwards the begin_archive callback to the next bbsink in a chain, enabling the initiation of archive processing in a cascaded bbsink architecture.

## Definition
```c
void bbsink_forward_begin_archive(bbsink *sink, const char *archive_name)
```

## Detailed Description
This function implements a forwarding pattern for the begin_archive callback within PostgreSQL's base backup sink infrastructure. It is designed to propagate the archive initialization signal through a chain of bbsink implementations. When called, it forwards the begin_archive operation to the next bbsink in the chain (sink->bbs_next) along with the archive name parameter.

This forwarding mechanism allows multiple bbsink implementations to be chained together, where each can perform specific operations while ensuring that the archive initialization is properly propagated through the entire chain. The function is commonly used by bbsink implementations that need to perform preprocessing or setup operations before passing control to the next bbsink.

## Parameters / Member Variables
- `sink`: Pointer to the bbsink structure that is forwarding the begin_archive operation
- `archive_name`: The name of the archive being initialized, passed through to the next bbsink in the chain

## Dependencies
- Functions called/Symbols referenced:
  - bbsink_begin_archive
  - bbsink (type reference)
- Called from (representative examples):
  - bbsink_server_begin_archive (src/backend/backup/basebackup_server.c:153)

## Notes and Other Information
- The function performs an assertion to ensure that bbs_next is properly initialized before forwarding
- This is a utility function that simplifies the implementation of bbsink types that need to forward archive operations
- The archive_name parameter is passed through unchanged to maintain the original archive identification
- This forwarding pattern is essential for maintaining the chain of responsibility in composite bbsink architectures