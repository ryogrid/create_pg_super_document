# StreamLog

## Location
src/bin/pg_basebackup/pg_receivewal.c: 500 - 616

## Overview
The main function responsible for establishing and managing the WAL (Write-Ahead Log) streaming connection from a PostgreSQL server in pg_receivewal.

## Definition


## Detailed Description
StreamLog is the core function that orchestrates the entire WAL streaming process. It handles the complete lifecycle of WAL streaming including:

1. **Connection Management**: Establishes a replication connection to the PostgreSQL server and validates server version compatibility
2. **Server Identification**: Retrieves server system identifier, current timeline, and server position using RunIdentifySystem
3. **Stream Position Resolution**: Determines the optimal starting position for streaming through multiple fallback strategies:
   - First attempts to find resume point from existing local WAL files via FindStreamingStart
   - If no local files exist, tries to get position from replication slot (PostgreSQL 15+)
   - Falls back to current server WAL flush position as last resort
4. **Stream Configuration**: Sets up a StreamCtl structure with all necessary parameters for WAL streaming
5. **WAL Reception**: Initiates actual WAL streaming using ReceiveXlogStream and manages the stream lifecycle
6. **Cleanup**: Properly finalizes the WAL method and closes connections

The function ensures streaming always begins at segment boundaries and provides comprehensive logging for monitoring purposes.

## Parameters / Member Variables
This function takes no parameters but operates on several global variables and creates local variables:
- Creates  structure to configure the streaming process
- Uses global variables like , , , 

## Dependencies
- Functions called/Symbols referenced:
  - GetConnection
  - CheckServerVersionForStreaming  
  - RunIdentifySystem
  - FindStreamingStart
  - GetSlotInformation
  - CreateWalDirectoryMethod
  - ReceiveXlogStream
  - PQserverVersion, PQfinish
  - XLogSegmentOffset
  - pg_log_info
- Called from (representative examples):
  - main (in pg_receivewal.c:908)

## Notes and Other Information
- This is a static function, only accessible within pg_receivewal.c
- Implements a sophisticated fallback strategy for determining streaming start position
- Always aligns streaming to segment boundaries using XLogSegmentOffset
- Supports replication slots for more reliable streaming resume (PostgreSQL 15+)
- Handles both synchronous and asynchronous streaming modes
- Integrates with PostgreSQL's WAL compression mechanisms
- Critical error conditions cause the program to exit rather than return, ensuring data integrity
- The function creates and manages the entire streaming context through the StreamCtl structure