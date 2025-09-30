# LSNGetDatum

## Location
[src/include/utils/pg_lsn.h:28-32](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/pg_lsn.h#L28-L32)

## Overview
LSNGetDatum is an inline function that converts an XLogRecPtr (Log Sequence Number) to a PostgreSQL Datum value, enabling LSN values to be used within PostgreSQL's type system and SQL interface.

## Definition
static inline Datum LSNGetDatum(XLogRecPtr X)

## Detailed Description
LSNGetDatum performs the conversion from XLogRecPtr to Datum by first casting the XLogRecPtr to a signed 64-bit integer (int64) and then using Int64GetDatum() to wrap it as a Datum. This function is essential for exposing LSN values to PostgreSQL's SQL interface and type system, allowing functions to return LSN values that can be used in SQL queries and stored in tables.

The function is implemented as a static inline function, providing optimal performance by eliminating function call overhead through compile-time inlining. It serves as the counterpart to DatumGetLSN(), forming a complete conversion pair between the internal XLogRecPtr representation and the external Datum representation.

## Parameters / Member Variables
- `X`: An XLogRecPtr value representing a log sequence number to be converted to Datum format

## Dependencies
- Functions called/Symbols referenced:
  - [Int64GetDatum](../I/Int64GetDatum.md)
- Called from (representative examples):
  - [AddSubscriptionRelState](../A/AddSubscriptionRelState.md)
  - [UpdateSubscriptionRelStateEx](../U/UpdateSubscriptionRelStateEx.md)
  - [CreateSubscription](../C/CreateSubscription.md)
  - [pg_create_physical_replication_slot](../p/pg_create_physical_replication_slot.md)
  - [pg_create_logical_replication_slot](../p/pg_create_logical_replication_slot.md)
  - [pg_replication_slot_advance](../p/pg_replication_slot_advance.md)
  - [pg_stat_get_wal_receiver](../p/pg_stat_get_wal_receiver.md)
  - [pg_control_checkpoint](../p/pg_control_checkpoint.md)
  - PG_RETURN_LSN (macro)

## Notes and Other Information
- This function is the counterpart to DatumGetLSN(), which performs the reverse conversion
- It's commonly used through the PG_RETURN_LSN(x) macro for functions that need to return LSN values to SQL
- Extensively used throughout PostgreSQL's replication subsystem for exposing LSN values to users
- The conversion involves casting XLogRecPtr (which is typically uint64) to int64, maintaining the bit pattern while changing signedness
- Critical for SQL functions that report replication progress, WAL positions, and checkpoint information
- Being an inline function, it incurs no runtime performance penalty when compiler optimizations are enabled

## Simplified Source

```c
static inline Datum
LSNGetDatum(XLogRecPtr X)
{
    // Convert XLogRecPtr to Datum by casting to int64
    return Int64GetDatum((int64) X);
}
```