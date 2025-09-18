# pg_stat_get_wal_receiver

## Location
src/backend/replication/walreceiver.c: 1401 - 1530

## Overview
Returns activity information of the WAL receiver process, including process ID, state, and WAL locations received from the WAL sender of another server in a PostgreSQL streaming replication setup.

## Definition


## Detailed Description
This function provides comprehensive monitoring information about the WAL receiver process, which is responsible for receiving WAL (Write-Ahead Log) records from a primary server in PostgreSQL streaming replication. The function returns a composite tuple containing detailed statistics about the receiver's current state, progress, and connection information.

The function implements security checks to ensure that only superusers and users with  privileges can view detailed information. Regular users can only see the process ID to determine if a WAL receiver is running, but cannot access sensitive details like connection information or replication progress.

The function uses spinlocks to ensure consistent reads of shared memory variables, with the exception of  which is read atomically without locks for performance reasons.

## Parameters / Member Variables
This function takes no parameters ( is the standard PostgreSQL function interface).

The returned tuple contains the following fields:
- : Process ID of the WAL receiver
- : Current state of the WAL receiver (e.g., 'streaming', 'stopped')
- : LSN from which the receiver started receiving
- : Timeline ID from which receiving started
- : LSN up to which WAL has been written to disk
- : LSN up to which WAL has been flushed to disk
- : Timeline ID currently being received
- : Timestamp of last message sent to sender
- : Timestamp of last message received from sender
- : Latest end-of-WAL location reported by sender
- : Timestamp of latest end-of-WAL location
- : Name of the replication slot being used
- : Hostname of the WAL sender
- : Port number of the WAL sender
- : Connection string used to connect to sender

## Dependencies
- Functions called/Symbols referenced:
  - [WalRcvGetStateString](../W/WalRcvGetStateString.md)
  - SpinLockAcquire/SpinLockRelease
  - [pg_atomic_read_u64](pg_atomic_read_u64.md)
  - [get_call_result_type](../g/get_call_result_type.md)
  - has_privs_of_role
  - XLogRecPtrIsInvalid
  - LSNGetDatum
  - TimestampTzGetDatum
  - CStringGetTextDatum
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - [HeapTupleGetDatum](../H/HeapTupleGetDatum.md)
  - strlcpy
- Called from (representative examples):
  - This function is typically accessed through PostgreSQL's function call mechanism via SQL queries to pg_stat_wal_receiver view

## Notes and Other Information
- The function returns NULL if no WAL receiver is running or if the receiver is not ready to display statistics
- Access to detailed information is restricted by privilege checks - only users with  role privileges can see full details
- The  field is read without holding a spinlock for performance reasons, which means it may not be perfectly consistent with other fields, but this inconsistency is acceptable for monitoring purposes
- This function is typically used by database administrators to monitor replication lag and connection status in streaming replication setups
- The function is designed to be safe for concurrent access and provides consistent snapshots of WAL receiver state
- Located in 