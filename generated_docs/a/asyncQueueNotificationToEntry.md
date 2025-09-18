# asyncQueueNotificationToEntry

## Location
src/backend/commands/async.c: 1320 - 1355

## Overview
Converts a Notification structure into an AsyncQueueEntry format suitable for storage in the asynchronous notification queue.

## Definition


## Detailed Description
This function transforms an internal Notification object into the format required for storage in the shared memory notification queue. It performs several key operations:

1. **Length Calculation**: Computes the total entry length including channel name, payload, and required alignment
2. **Validation**: Ensures channel name and payload lengths are within allowed limits
3. **Metadata Population**: Sets database OID, transaction ID, and source process ID
4. **Data Copying**: Copies the channel name and payload data into the queue entry structure

The function applies proper alignment (QUEUEALIGN) to ensure efficient memory access and maintains compatibility with the queue's storage format. The resulting entry contains all information needed by listening backends to process the notification.

## Parameters / Member Variables
- : Pointer to source Notification structure containing channel name and payload
- : Pointer to destination AsyncQueueEntry structure to be filled

## Dependencies
- Functions called/Symbols referenced:
  - [GetCurrentTransactionId](../G/GetCurrentTransactionId.md) (to get current transaction ID)
  - QUEUEALIGN (alignment macro for queue entries)
  - AsyncQueueEntryEmptySize (base size of empty queue entry)
  - NAMEDATALEN (maximum channel name length)
  - NOTIFY_PAYLOAD_MAX_LENGTH (maximum payload length)
  - MyDatabaseId (current database OID)
  - MyProcPid (current process ID)
  - memcpy (to copy channel and payload data)

- Called from:
  - [asyncQueueAddEntries](asyncQueueAddEntries.md) (when adding notifications to the queue)

## Notes and Other Information
- This is a static function internal to async.c
- Includes assertions to validate channel and payload length constraints
- The entry length calculation includes space for null terminators (already accounted for in AsyncQueueEntryEmptySize)
- Data copying includes both channel name and payload plus 2 bytes for null terminators
- The transaction ID is captured to enable proper cleanup of undelivered notifications
- Part of the serialization process for PostgreSQL's LISTEN/NOTIFY messaging system