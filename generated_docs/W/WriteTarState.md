# WriteTarState

## Location
[src/bin/pg_basebackup/pg_basebackup.c:67-71](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/pg_basebackup.c#L67-L71)

## Overview
A lightweight state management structure for handling TAR file generation during base backup operations, tracking tablespace information and stream processing.

## Definition


## Detailed Description
WriteTarState is a streamlined state management structure used in pg_basebackup for coordinating TAR file generation and processing. This structure provides a simpler alternative to the more complex ArchiveStreamState, focusing specifically on TAR file operations without the additional complexity of compression and manifest handling.

The structure serves as a state holder for TAR-specific backup operations, maintaining the essential information needed to process TAR streams during base backup. It represents a focused approach to stream processing where only the core streaming functionality and tablespace identification are required.

## Parameters / Member Variables
- : Identifier for the tablespace being processed, used to track which tablespace the current TAR stream represents
- : bbstreamer instance responsible for handling the TAR stream processing and data flow

## Dependencies
- Functions called/Symbols referenced:
  - [bbstreamer](../b/bbstreamer.md) (stream processing component)
- Called from (representative examples):
  - [ReceiveTarFile](../R/ReceiveTarFile.md)
  - [ReceiveTarCopyChunk](../R/ReceiveTarCopyChunk.md)

## Notes and Other Information
- This structure is simpler than ArchiveStreamState, focusing specifically on TAR file operations
- Used when the backup process needs to handle TAR format specifically rather than other archive formats
- The tablespacenum allows for proper identification and organization of TAR files by tablespace
- Provides a lightweight state management solution for TAR-based backup operations
- The single streamer design indicates a more straightforward processing pipeline compared to archive streams
- Typically used in scenarios where compression and manifest injection are not required or handled separately