# bbsink_copystream_begin_manifest

## Location
[src/backend/backup/basebackup_copy.c:260-272](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_copy.c#L260-L272)

## Overview
Sends a CopyData protocol message announcing the beginning of the backup manifest in a copystream-based backup sink.

## Definition
static void bbsink_copystream_begin_manifest(bbsink *sink)

## Detailed Description
This function initiates the backup manifest transmission by sending a CopyData protocol message with a manifest type indicator. It constructs a PostgreSQL wire protocol message that signals to the client that manifest data will follow. The function uses PostgreSQL's internal message buffer system to assemble and send the protocol message containing a single 'm' byte to indicate manifest data type.

## Parameters / Member Variables
- : Pointer to the base bbsink structure representing the copystream backup sink

## Dependencies
- Functions called/Symbols referenced:
  - [pq_beginmessage](../p/pq_beginmessage.md)
  - [pq_sendbyte](../p/pq_sendbyte.md)  
  - [pq_endmessage](../p/pq_endmessage.md)
  - PqMsg_CopyData
- Called from (representative examples):
  - Used as callback function in bbsink copystream operations

## Notes and Other Information
- This is a static function internal to the basebackup_copy.c module
- Part of the bbsink copystream implementation for PostgreSQL base backups
- Sends only a type indicator byte 'm' to mark the start of manifest transmission
- Located in src/backend/backup/basebackup_copy.c:260-272