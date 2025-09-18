# ReceiveXlogStream

## Location
[src/bin/pg_basebackup/receivelog.c:453-698](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/receivelog.c#L453-L698)

## Overview
Main function for receiving and processing a PostgreSQL WAL (Write-Ahead Log) stream from a server, handling timeline transitions and continuous streaming until a stop condition is met.

## Definition


## Detailed Description
 is the core function that orchestrates PostgreSQL streaming replication. It establishes and maintains a continuous WAL stream from the primary server, handling the complete lifecycle of replication including server version validation, system identifier verification, timeline history management, and automatic timeline transitions. The function runs in a loop, continuously streaming WAL data until explicitly stopped by a callback or server shutdown. It manages both physical and logical replication scenarios and handles various edge cases like timeline switches and partial WAL records at timeline boundaries.

The function supports both synchronous and asynchronous replication modes, handles replication slots for reliable delivery, and implements proper error handling and cleanup procedures.

## Parameters / Member Variables
- : PostgreSQL connection handle for the replication session
- : StreamCtl structure containing all streaming parameters and callbacks including start position, timeline, stop conditions, and output methods

## Dependencies
- Functions called/Symbols referenced:
  - [CheckServerVersionForStreaming](../C/CheckServerVersionForStreaming.md)
  - [RunIdentifySystem](RunIdentifySystem.md)
  - [existsTimeLineHistoryFile](../e/existsTimeLineHistoryFile.md)
  - [writeTimeLineHistoryFile](../w/writeTimeLineHistoryFile.md)
  - [HandleCopyStream](../H/HandleCopyStream.md)
  - [ReadEndOfStreamingResult](ReadEndOfStreamingResult.md)
  - [PQexec](../P/PQexec.md)
  - [PQgetResult](../P/PQgetResult.md)
  - [pg_free](../p/pg_free.md)
  - XLogSegmentOffset
- Called from (representative examples):
  - [LogStreamerMain](../L/LogStreamerMain.md)
  - [StreamLog](../S/StreamLog.md)

## Notes and Other Information
- Requires WAL start position to be at a log segment boundary
- Automatically fetches missing timeline history files
- Supports replication slots for guaranteed WAL retention
- Handles timeline transitions by parsing server responses and restarting streaming on new timelines
- Implements flush position reporting for synchronous replication eligibility
- Validates system identifier and timeline consistency when specified
- Uses callback-based architecture for flexible stop conditions and data processing
- Critical component for pg_basebackup, pg_receivewal, and other replication tools