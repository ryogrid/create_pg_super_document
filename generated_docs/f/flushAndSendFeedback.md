# flushAndSendFeedback

## Location
[src/bin/pg_basebackup/pg_recvlogical.c:1025-1041](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/pg_recvlogical.c#L1025-L1041)

## Overview
A function that synchronizes output data to disk and sends a feedback message to the PostgreSQL server during logical replication streaming.

## Definition


## Detailed Description
The  function is responsible for ensuring data durability and maintaining communication with the PostgreSQL server during logical replication. It performs two critical operations in sequence: first, it forces any buffered output data to be written to disk using  to ensure durability, and then it sends a feedback message to the server indicating the current flush position. The function updates the provided timestamp to reflect the current time just before sending feedback, which helps maintain accurate timing information for replication lag monitoring. This function is essential for maintaining data consistency and providing the server with up-to-date information about the client's progress in processing the logical replication stream.

## Parameters / Member Variables
- : A pointer to the PostgreSQL connection object used for communication with the server
- : A pointer to a TimestampTz variable that gets updated with the current timestamp before sending feedback

## Dependencies
- Functions called/Symbols referenced:
  - [OutputFsync](../O/OutputFsync.md) (forces buffered data to disk)
  - [feGetCurrentTimestamp](feGetCurrentTimestamp.md) (gets current timestamp)
  - [sendFeedback](../s/sendFeedback.md) (sends feedback message to server)
- Called from (representative examples):
  - [StreamLogicalLog](../S/StreamLogicalLog.md) (multiple locations in logical replication processing)

## Notes and Other Information
- This is a static function, meaning it's only visible within its compilation unit
- Returns true on success, false if either fsync or feedback sending fails
- The function ensures data durability before reporting progress to the server, which is crucial for consistency
- Used specifically in the pg_recvlogical utility for logical replication WAL streaming
- The timestamp update ensures that feedback contains the most current timing information
- Error handling is simplified: any failure in either operation causes the entire function to return false