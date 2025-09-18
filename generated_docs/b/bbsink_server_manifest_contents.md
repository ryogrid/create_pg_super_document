# bbsink_server_manifest_contents

## Location
[src/backend/backup/basebackup_server.c:253-286](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_server.c#L253-L286)

## Overview
Writes manifest data chunks to a file during a base backup operation as part of the server-side base backup sink implementation.

## Definition


## Detailed Description
This function is a callback implementation for the bbsink interface that handles writing manifest contents to a file during base backup operations. It writes data from the sink's buffer to the associated file, performs error checking for write operations, updates the file position, and forwards the manifest contents to any chained sinks in the pipeline.

The function ensures data integrity by verifying that all requested bytes are successfully written to disk. If a write operation fails or results in a partial write, it reports appropriate errors with helpful hints about checking disk space.

## Parameters / Member Variables
- : Pointer to the base bbsink structure, which is cast to bbsink_server for access to server-specific fields
- : Number of bytes to write from the sink's buffer to the file

## Dependencies
- Functions called/Symbols referenced:
  - [FileWrite](../F/FileWrite.md): Performs the actual file write operation
  - [FilePathName](../F/FilePathName.md): Retrieves the file path for error messages
  - [bbsink_forward_manifest_contents](bbsink_forward_manifest_contents.md): Forwards manifest contents to chained sinks
  - ereport: Reports errors with appropriate error codes and messages
- Called from (representative examples):
  - This is a static function used as a callback in the bbsink interface (no direct callers found)

## Notes and Other Information
- This is a static function specific to the server-side base backup implementation
- Error handling includes specific checks for disk space issues and provides helpful hints
- The function maintains the file position counter (filepos) for proper sequential writing
- Uses PostgreSQL's file I/O abstraction layer (FileWrite) rather than standard C file operations
- Part of the bbsink callback interface pattern used throughout the base backup system