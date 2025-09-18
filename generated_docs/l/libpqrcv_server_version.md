# libpqrcv_server_version

## Location
src/backend/replication/libpqwalreceiver/libpqwalreceiver.c: 491 - 501

## Overview
A thin wrapper function that retrieves the server version number from a PostgreSQL primary server connection used in WAL receiver operations.

## Definition


## Detailed Description
This function provides a simple interface to obtain the version number of the PostgreSQL server that the WAL receiver is connected to. It acts as a wrapper around the libpq library's PQserverVersion function, maintaining consistency with the libpqwalreceiver module's API design. The version information is crucial for determining compatibility and enabling version-specific features during replication setup and operation.

The function returns the server version as an integer in the same format used throughout PostgreSQL, where the version number is encoded as a single integer (e.g., 90600 for version 9.6.0).

## Parameters / Member Variables
- : Pointer to WalReceiverConn structure containing the established connection to the primary server

## Dependencies
- Functions called/Symbols referenced:
  - [PQserverVersion](../P/PQserverVersion.md)
- Called from (representative examples):
  - [WalReceiverConn](../W/WalReceiverConn.md) (referenced in connection management routines)

## Notes and Other Information
- This is a static function internal to the libpqwalreceiver module
- Returns an integer representing the server version in PostgreSQL's standard version encoding format
- The function is a direct passthrough to libpq's PQserverVersion, ensuring consistent version reporting
- Located at src/backend/replication/libpqwalreceiver/libpqwalreceiver.c:491-501