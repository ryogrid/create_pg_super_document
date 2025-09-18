# bbsink_server_end_manifest

## Location
[src/backend/backup/basebackup_server.c:287-309](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_server.c#L287-L309)

## Overview
Finalizes the backup manifest file by closing, syncing, and atomically renaming it to its final location during base backup completion.

## Definition
```c
static void bbsink_server_end_manifest(bbsink *sink)
```

## Detailed Description
This function completes the manifest writing process for server-side base backups. It performs the critical final steps to ensure the backup manifest file is properly closed, synchronized to disk, and atomically moved to its final location. The function uses a temporary file approach followed by atomic rename to ensure the manifest file is never in a partially written state that could be read by other processes.

The atomic rename operation (via durable_rename) also ensures the file is properly fsynced to disk, providing durability guarantees. After completing the local file operations, it forwards the end manifest signal to any chained sinks in the pipeline.

## Parameters / Member Variables
- `sink`: Pointer to the base bbsink structure, which is cast to bbsink_server for access to server-specific fields including the file handle and pathname

## Dependencies
- Functions called/Symbols referenced:
  - FileClose: Closes the temporary manifest file
  - [psprintf](../p/psprintf.md): Constructs the temporary and final filenames 
  - [durable_rename](../d/durable_rename.md): Atomically renames the file and ensures it's synced to disk
  - [pfree](../p/pfree.md): Frees the allocated filename strings
  - [bbsink_forward_end_manifest](bbsink_forward_end_manifest.md): Forwards the end manifest signal to chained sinks
- Called from (representative examples):
  - This is a static function used as a callback in the bbsink interface (no direct callers found)

## Notes and Other Information
- This is a static function specific to the server-side base backup implementation
- Uses atomic rename semantics to ensure the manifest file is never in a partially written state
- The durable_rename function handles both the rename and fsync operations automatically
- Temporary files use the .tmp extension and are renamed to the final backup_manifest filename
- Part of the bbsink callback interface pattern used throughout the base backup system
- The file handle is set to 0 after closing to prevent accidental reuse
- Memory management includes proper cleanup of dynamically allocated filename strings