# AsyncQueueEntry

## Location
src/backend/commands/async.c: 177 - 184

## Overview
AsyncQueueEntry is a structure that represents an entry in the global notify queue, used by PostgreSQL's asynchronous notification system.

## Definition


## Detailed Description
The AsyncQueueEntry structure represents a single notification entry in PostgreSQL's asynchronous notification queue system. This structure is designed with variable length - while the declaration shows the maximum possible size, actual queue entries only allocate enough space in the data area for the actual channel and payload strings (both null-terminated). The structure is carefully designed to ensure proper memory alignment, with the length field always rounded up to the next QUEUEALIGN multiple.

## Parameters / Member Variables
- : Total allocated length of the entry, always rounded up to QUEUEALIGN multiple for proper alignment
- : Database OID of the sender who issued the notification
- : Transaction ID of the sender's transaction
- : Process ID of the backend that sent the notification
- : Variable-length data area containing channel name and payload strings (null-terminated)

## Dependencies
- Functions called/Symbols referenced:
  - NAMEDATALEN
  - NOTIFY_PAYLOAD_MAX_LENGTH
- Called from (representative examples):
  - AsyncQueueEntryEmptySize
  - NotificationHash
  - asyncQueueNotificationToEntry
  - asyncQueueAddEntries
  - asyncQueueReadAllNotifications
  - asyncQueueProcessPageEntries

## Notes and Other Information
- The data area size is calculated as NAMEDATALEN + NOTIFY_PAYLOAD_MAX_LENGTH, representing the maximum possible channel name and payload size
- AsyncQueueEntryEmptySize represents the minimum possible entry size when both channel and payload strings are empty
- The structure is part of PostgreSQL's LISTEN/NOTIFY mechanism for asynchronous inter-backend communication
- Proper alignment is crucial for performance and correctness across different architectures