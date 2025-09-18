# bbsink_copystream_begin_archive

## Location
src/backend/backup/basebackup_copy.c: 165 - 182

## Overview
Sends a CopyData protocol message to announce the beginning of a new archive in the backup stream.

## Definition
static void bbsink_copystream_begin_archive(bbsink *sink, const char *archive_name)

## Detailed Description
This function creates and sends a CopyData message that signals the start of a new archive within the backup stream. The message contains a type byte 'n' (for "New archive") followed by the archive name and the corresponding tablespace path. This allows the client to distinguish between different archives in the continuous COPY stream and properly organize the received backup data. The function retrieves the current tablespace information from the backup state to include the appropriate path information with the archive announcement.

## Parameters / Member Variables
- : Pointer to the base bbsink structure containing backup state information
- : Name of the archive being started (typically a tar filename)

## Dependencies
- Functions called/Symbols referenced:
  - list_nth
  - pq_beginmessage
  - pq_sendbyte
  - pq_sendstring (called twice)
  - pq_endmessage
  - PqMsg_CopyData
  - tablespaceinfo
  - bbsink_state
- Called from (representative examples):
  - Referenced by bbsink_copystream_ops structure as the begin_archive handler

## Notes and Other Information
- Sends a 'n' type byte to identify this as a new archive message in the protocol stream
- Includes both the archive name and tablespace path in the message payload
- Uses an empty string for the tablespace path if ti->path is NULL (typically for the main tablespace)
- This message format allows clients to reconstruct the proper directory structure for the backup
- The function accesses the current tablespace using state->tablespace_num as an index into the tablespaces list