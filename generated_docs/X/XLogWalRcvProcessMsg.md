# XLogWalRcvProcessMsg

## Location
src/backend/replication/walreceiver.c: 839 - 909

## Overview
Processes incoming replication messages from the XLOG stream, handling WAL records and keepalive messages from the primary server during streaming replication.

## Definition


## Detailed Description
This function is the core message processor for the WAL receiver process in PostgreSQL streaming replication. It handles two types of messages from the primary server:

1. **WAL records ('w' type)**: Contains actual Write-Ahead Logging data that needs to be written to local storage
2. **Keepalive messages ('k' type)**: Heartbeat messages to maintain connection and synchronize state

For WAL record messages, the function extracts header information including the data start LSN, WAL end LSN, and send timestamp, then delegates the actual writing to . For keepalive messages, it processes connection state information and may send an immediate reply if requested by the primary.

## Parameters / Member Variables
- : Message type identifier ('w' for WAL records, 'k' for keepalive)  
- : Raw message buffer containing the payload data
- : Length of the message buffer in bytes
- : Timeline ID for the WAL data being processed

## Dependencies
- Functions called/Symbols referenced:
  - [initReadOnlyStringInfo](../i/initReadOnlyStringInfo.md)
  - [pq_getmsgint64](../p/pq_getmsgint64.md)
  - [pq_getmsgbyte](../p/pq_getmsgbyte.md)
  - [ProcessWalSndrMessage](../P/ProcessWalSndrMessage.md)
  - [XLogWalRcvWrite](XLogWalRcvWrite.md)
  - [XLogWalRcvSendReply](XLogWalRcvSendReply.md)
- Called from (representative examples):
  - [WalReceiverMain](../W/WalReceiverMain.md)

## Notes and Other Information
- This is a static function internal to the walreceiver.c module
- Performs strict protocol validation, raising errors for invalid message types or malformed messages
- The function is critical for maintaining data consistency during streaming replication
- Located in src/backend/replication/walreceiver.c:839-909