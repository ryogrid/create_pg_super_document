# WriteManifestState

## Location
src/bin/pg_basebackup/pg_basebackup.c: 73 - 77

## Overview
A simple state management structure for handling backup manifest file operations during base backup, managing file path and file handle information.

## Definition


## Detailed Description
WriteManifestState is a minimal state management structure used in pg_basebackup specifically for handling backup manifest file operations. This structure provides a clean abstraction for managing the manifest file writing process, encapsulating both the file path information and the active file handle in a single state object.

The structure is designed to be simple and focused, dealing exclusively with manifest file operations without the complexity of stream processing or compression that is present in other state structures. It represents the essential state needed to write backup manifest data to disk during base backup operations.

## Parameters / Member Variables
- : File path for the manifest file being written (maximum MAXPGPATH characters)
- : FILE handle for the open manifest file, used for writing manifest data

## Dependencies
- Functions called/Symbols referenced:
  - (No direct symbol references - uses standard C FILE operations)
- Called from (representative examples):
  - [ReceiveBackupManifest](../R/ReceiveBackupManifest.md)
  - [ReceiveBackupManifestChunk](../R/ReceiveBackupManifestChunk.md)

## Notes and Other Information
- This is the simplest of the state management structures in pg_basebackup, focusing solely on file operations
- Used specifically for managing the backup manifest file writing process
- The structure provides a clean separation between file path management and file handle management
- Typically used in conjunction with functions that receive and process manifest data chunks
- The MAXPGPATH limit ensures compatibility with PostgreSQL's path length restrictions
- Unlike other state structures, this one doesn't involve streaming or compression, just direct file I/O
- Represents a focused approach to manifest file management within the broader backup operation context