# bbsink_forward_end_manifest

## Location
[src/backend/backup/basebackup_sink.c:101-110](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_sink.c#L101-L110)

## Overview
A forwarding function that passes the end manifest signal to the next backup sink in a chain, used in PostgreSQL's base backup system to indicate completion of manifest processing.

## Definition
void bbsink_forward_end_manifest(bbsink *sink)

## Detailed Description
This function is part of PostgreSQL's base backup sink forwarding mechanism. It forwards the end_manifest callback to the next sink in a chained configuration of backup sinks. The function signals that manifest processing has completed and should be finalized by calling the appropriate end_manifest operation on the next sink in the chain.

The function performs an assertion to ensure there is a valid next sink in the chain before forwarding the end_manifest operation. This is a critical step in the backup process as it ensures proper cleanup and finalization of the backup manifest.

This forwarding pattern maintains the chain of responsibility design, allowing different backup sink implementations to perform their specific end-of-manifest processing while ensuring the signal propagates through the entire chain.

## Parameters / Member Variables
- : Pointer to the current bbsink structure in the chain

## Dependencies
- Functions called/Symbols referenced:
  - [bbsink_end_manifest](bbsink_end_manifest.md)
  - [bbsink](bbsink.md) (structure type)
- Called from (representative examples):
  - [bbsink_server_end_manifest](bbsink_server_end_manifest.md)

## Notes and Other Information
- This function is called when manifest processing is complete and needs to be finalized
- Part of the callback-based architecture for chaining backup sink operations
- The function includes an assertion to ensure proper sink chain configuration
- Critical for proper cleanup and finalization of backup manifest operations in PostgreSQL base backups

## Simplified Source

```c
// Simplified version of bbsink_forward_end_manifest
void bbsink_forward_end_manifest(bbsink *sink) {
    // Validate chain exists
    Assert(sink->bbs_next != NULL);

    // Forward end manifest to next sink
    bbsink_end_manifest(sink->bbs_next);
}
```

Key simplifications made:
- Preserved essential chain validation
- Maintained manifest finalization forwarding
- Focused on core manifest end forwarding