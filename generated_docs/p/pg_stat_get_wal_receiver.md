# pg_stat_get_wal_receiver

## Location
[src/backend/replication/walreceiver.c:1401-1530](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/walreceiver.c#L1401-L1530)

## Overview
Returns activity information of the WAL receiver process, including process ID, state, and WAL locations received from the WAL sender of another server in a PostgreSQL streaming replication setup.

## Definition

```c
Datum
pg_stat_get_wal_receiver(PG_FUNCTION_ARGS)
```
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
  - [has_privs_of_role](../h/has_privs_of_role.md)
  - XLogRecPtrIsInvalid
  - [LSNGetDatum](../L/LSNGetDatum.md)
  - [TimestampTzGetDatum](../T/TimestampTzGetDatum.md)
  - CStringGetTextDatum
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - [HeapTupleGetDatum](../H/HeapTupleGetDatum.md)
  - [strlcpy](../s/strlcpy.md)
- Called from (representative examples):
  - This function is typically accessed through PostgreSQL's function call mechanism via SQL queries to pg_stat_wal_receiver view

## Notes and Other Information
- The function returns NULL if no WAL receiver is running or if the receiver is not ready to display statistics
- Access to detailed information is restricted by privilege checks - only users with  role privileges can see full details
- The  field is read without holding a spinlock for performance reasons, which means it may not be perfectly consistent with other fields, but this inconsistency is acceptable for monitoring purposes
- This function is typically used by database administrators to monitor replication lag and connection status in streaming replication setups
- The function is designed to be safe for concurrent access and provides consistent snapshots of WAL receiver state
- Located in

## Simplified Source

```c
Datum pg_stat_get_wal_receiver(PG_FUNCTION_ARGS) {
    TupleDesc tupdesc;
    Datum *values;
    bool *nulls;

    // Read WAL receiver state under lock
    SpinLockAcquire(&WalRcv->mutex);
    int pid = WalRcv->pid;
    bool ready_to_display = WalRcv->ready_to_display;
    WalRcvState state = WalRcv->walRcvState;
    XLogRecPtr receive_start_lsn = WalRcv->receiveStart;
    TimeLineID receive_start_tli = WalRcv->receiveStartTLI;
    XLogRecPtr flushed_lsn = WalRcv->flushedUpto;
    TimeLineID received_tli = WalRcv->receivedTLI;
    TimestampTz last_send_time = WalRcv->lastMsgSendTime;
    TimestampTz last_receipt_time = WalRcv->lastMsgReceiptTime;
    XLogRecPtr latest_end_lsn = WalRcv->latestWalEnd;
    TimestampTz latest_end_time = WalRcv->latestWalEndTime;

    // Copy string fields
    char slotname[NAMEDATALEN], sender_host[NI_MAXHOST], conninfo[MAXCONNINFO];
    strlcpy(slotname, WalRcv->slotname, sizeof(slotname));
    strlcpy(sender_host, WalRcv->sender_host, sizeof(sender_host));
    int sender_port = WalRcv->sender_port;
    strlcpy(conninfo, WalRcv->conninfo, sizeof(conninfo));
    SpinLockRelease(&WalRcv->mutex);

    // Return NULL if no WAL receiver running
    if (pid == 0 || !ready_to_display)
        PG_RETURN_NULL();

    // Read written LSN atomically (may be slightly inconsistent)
    XLogRecPtr written_lsn = pg_atomic_read_u64(&WalRcv->writtenUpto);

    // Prepare result tuple
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        elog(ERROR, "return type must be a row type");

    values = palloc0(sizeof(Datum) * tupdesc->natts);
    nulls = palloc0(sizeof(bool) * tupdesc->natts);

    // Always include PID
    values[0] = Int32GetDatum(pid);

    // Check privileges for detailed information
    if (!has_privs_of_role(GetUserId(), ROLE_PG_READ_ALL_STATS)) {
        // Non-privileged users only get PID
        memset(&nulls[1], true, sizeof(bool) * (tupdesc->natts - 1));
    } else {
        // Populate all fields for privileged users
        values[1] = CStringGetTextDatum(WalRcvGetStateString(state));
        values[2] = XLogRecPtrIsInvalid(receive_start_lsn) ? (nulls[2] = true, 0) : LSNGetDatum(receive_start_lsn);
        values[3] = Int32GetDatum(receive_start_tli);
        values[4] = XLogRecPtrIsInvalid(written_lsn) ? (nulls[4] = true, 0) : LSNGetDatum(written_lsn);
        values[5] = XLogRecPtrIsInvalid(flushed_lsn) ? (nulls[5] = true, 0) : LSNGetDatum(flushed_lsn);
        values[6] = Int32GetDatum(received_tli);
        values[7] = last_send_time == 0 ? (nulls[7] = true, 0) : TimestampTzGetDatum(last_send_time);
        values[8] = last_receipt_time == 0 ? (nulls[8] = true, 0) : TimestampTzGetDatum(last_receipt_time);
        values[9] = XLogRecPtrIsInvalid(latest_end_lsn) ? (nulls[9] = true, 0) : LSNGetDatum(latest_end_lsn);
        values[10] = latest_end_time == 0 ? (nulls[10] = true, 0) : TimestampTzGetDatum(latest_end_time);
        values[11] = *slotname == '\0' ? (nulls[11] = true, 0) : CStringGetTextDatum(slotname);
        values[12] = *sender_host == '\0' ? (nulls[12] = true, 0) : CStringGetTextDatum(sender_host);
        values[13] = sender_port == 0 ? (nulls[13] = true, 0) : Int32GetDatum(sender_port);
        values[14] = *conninfo == '\0' ? (nulls[14] = true, 0) : CStringGetTextDatum(conninfo);
    }

    PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}
``` 