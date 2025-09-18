# bbsink_copystream

## Location
src/backend/backup/basebackup_copy.c: 39 - 62

## Overview
A specialized base backup sink structure that handles streaming backup data to clients or other destinations via PostgreSQL's COPY protocol messaging system.

## Definition


## Detailed Description
The  structure is a concrete implementation of the  base backup sink interface, specifically designed for streaming backup archives and manifests through PostgreSQL's COPY protocol. It extends the base  structure with additional fields needed for protocol message handling and progress tracking.

This sink is responsible for formatting backup data into COPY protocol messages and can either send data directly to the client or redirect it elsewhere based on the  flag. The structure maintains its own message buffer for assembling CopyData protocol messages, where the first byte is set to 'd' to indicate data content, and the actual backup data is placed starting from the second byte.

The sink also tracks progress reporting by maintaining timestamps and byte counts, allowing for periodic progress updates during long-running backup operations.

## Parameters / Member Variables
- : The inherited  structure containing common sink functionality (callback operations, buffer management, state tracking, and chaining to next sink)
- : Boolean flag indicating whether archives should be sent to the client or redirected to another destination
- : Protocol message buffer used to construct CopyData messages; the first character is set to 'd' for data messages, with actual content starting at the second position
- : Timestamp of the last progress report sent to track reporting intervals
- : Number of bytes processed at the time of the last progress check, used for calculating progress rates

## Dependencies
- Functions called/Symbols referenced:
  - bbsink (base structure)
  - TimestampTz (PostgreSQL timestamp type)
- Called from (representative examples):
  - bbsink_copystream_new
  - bbsink_copystream_begin_backup
  - bbsink_copystream_archive_contents
  - bbsink_copystream_end_archive
  - bbsink_copystream_manifest_contents

## Notes and Other Information
- This structure is defined in 
- It's part of PostgreSQL's modular base backup sink architecture, where multiple sinks can be chained together for different processing steps (compression, throttling, progress reporting, etc.)
- The COPY protocol integration allows for efficient streaming of backup data without requiring intermediate storage
- Progress tracking functionality enables user feedback during long backup operations
- The message buffer design optimizes protocol message construction by pre-allocating space for the protocol header