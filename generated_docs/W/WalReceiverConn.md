# WalReceiverConn

## Location
src/backend/replication/libpqwalreceiver/libpqwalreceiver.c: 40 - 119

## Overview
WalReceiverConn is a structure that encapsulates the connection information and state for WAL (Write-Ahead Log) receiver functionality in PostgreSQL's logical and physical replication system.

## Definition


## Detailed Description
WalReceiverConn serves as the core data structure for managing WAL receiver connections in PostgreSQL's replication infrastructure. It is defined in the libpqwalreceiver module, which implements the WAL receiver functionality using libpq for communication with the primary server.

This structure maintains the essential state needed for both logical and physical replication:
- It holds the actual PostgreSQL connection object (PGconn) that handles the network communication
- It tracks whether the connection is being used for logical or physical replication
- It maintains a buffer for storing received WAL records during streaming

The structure is primarily used within the libpqwalreceiver.c module, which provides the concrete implementation of the WAL receiver interface functions defined in the PostgreSQL replication framework.

## Parameters / Member Variables
- : A pointer to the PGconn structure representing the active connection to the primary server. This is the libpq connection object used for all communication with the source database.
- : A boolean flag indicating whether this connection is being used for logical replication (true) or physical replication (false). This affects how the received data is interpreted and processed.
- : A character buffer used to store WAL records as they are received from the primary server during streaming replication.

## Dependencies
- Functions called/Symbols referenced:
  - libpqrcv_connect
  - libpqrcv_disconnect  
  - libpqrcv_startstreaming
  - libpqrcv_endstreaming
  - libpqrcv_receive
  - libpqrcv_send
  - libpqrcv_create_slot
  - libpqrcv_alter_slot
  - libpqrcv_exec
  - libpqrcv_get_conninfo
  - libpqrcv_get_senderinfo
  - libpqrcv_identify_system
  - libpqrcv_server_version
  - libpqrcv_readtimelinehistoryfile
  - libpqrcv_get_backend_pid
  - PGconn (libpq connection structure)
  - WalRcvStreamOptions
  - WalRcvExecResult

- Called from (representative examples):
  - CreateSubscription (for logical replication setup)
  - AlterSubscription (for subscription modifications)
  - DropSubscription (for subscription cleanup)
  - synchronize_slots (for slot synchronization)
  - ReplSlotSyncWorkerMain (for slot sync worker)
  - SyncReplicationSlots (for replication slot synchronization)
  - TransApplyAction (for transaction application)

## Notes and Other Information
- This structure is part of the libpqwalreceiver module, which is PostgreSQL's default WAL receiver implementation using libpq
- The structure is opaque to most of the PostgreSQL core - it's primarily manipulated through the WAL receiver function interface
- The  flag is crucial for determining the correct processing path for received data, as logical and physical replication have different data formats and handling requirements
- The  is used as a staging area for incoming WAL data before it's processed by the appropriate replication subsystem
- This structure is central to both subscription-based logical replication and traditional physical streaming replication
- The connection management functions (connect/disconnect) handle the lifecycle of the PGconn object and associated resources
- Error handling and connection recovery are managed through the various libpqrcv_* functions that operate on this structure