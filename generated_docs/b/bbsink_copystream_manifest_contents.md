# bbsink_copystream_manifest_contents

## Location
src/backend/backup/basebackup_copy.c: 273 - 287

## Overview
Sends chunks of backup manifest data to the client using CopyData protocol messages in a copystream-based backup sink.

## Definition
static void bbsink_copystream_manifest_contents(bbsink *sink, size_t len)

## Detailed Description
This function transmits manifest content data by sending CopyData protocol messages to the client. It casts the generic bbsink to a bbsink_copystream structure and checks if data should be sent to the client. When sending is enabled, it uses pq_putmessage to transmit the manifest data buffer with a 'd' type indicator, adding one to the length to account for the leading type byte that identifies the message as data content.

## Parameters / Member Variables
- `sink`: Pointer to the base bbsink structure representing the copystream backup sink
- `len`: Size in bytes of the manifest content to be sent

## Dependencies
- Functions called/Symbols referenced:
  - [bbsink_copystream](bbsink_copystream.md) (structure cast)
  - pq_putmessage
- Called from (representative examples):
  - Used as callback function during manifest data transmission in bbsink copystream operations

## Notes and Other Information
- This is a static function internal to the basebackup_copy.c module
- Part of the bbsink copystream implementation for PostgreSQL base backups
- Only sends data when send_to_client flag is true in the bbsink_copystream structure
- Uses the msgbuffer from the bbsink_copystream structure to store protocol message data
- The function adds 1 to the length to account for the leading 'd' type byte
- Located in src/backend/backup/basebackup_copy.c:273-287