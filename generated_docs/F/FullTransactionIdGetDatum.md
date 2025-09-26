# FullTransactionIdGetDatum

## Location
[src/include/utils/xid8.h:24-28](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/xid8.h#L24-L28)

## Overview
Converts a FullTransactionId structure to a PostgreSQL Datum value, enabling the conversion of internal transaction ID structures to 8-byte SQL data types.

## Definition
```c
static inline Datum FullTransactionIdGetDatum(FullTransactionId X)
```

## Detailed Description
This inline function serves as a type conversion utility in PostgreSQL's xid8 Abstract Data Type (ADT) system, performing the inverse operation of DatumGetFullTransactionId. It takes a FullTransactionId structure (PostgreSQL's internal representation of 64-bit transaction IDs) and converts it to a Datum for use in SQL operations. This function is essential for returning transaction ID values from internal PostgreSQL functions back to the SQL layer.

The conversion is performed by first extracting the underlying 64-bit value from the FullTransactionId structure using `U64FromFullTransactionId`, then wrapping that value in a Datum using `UInt64GetDatum`. This maintains type safety while enabling seamless data exchange between PostgreSQL's internal transaction management and its SQL interface.

## Parameters / Member Variables
- `X`: A FullTransactionId structure containing the transaction ID that needs to be converted to a Datum for SQL use

## Dependencies
- Functions called/Symbols referenced:
  - U64FromFullTransactionId
  - [UInt64GetDatum](../U/UInt64GetDatum.md)
  - [FullTransactionId](FullTransactionId.md) (type)
- Called from (representative examples):
  - [pg_snapshot_xip](../p/pg_snapshot_xip.md)
  - PG_RETURN_FULLTRANSACTIONID

## Notes and Other Information
- This is a static inline function defined in src/include/utils/xid8.h:24-28
- Part of PostgreSQL's xid8 ADT which handles 64-bit transaction IDs
- Used primarily in SQL function implementations that need to return extended transaction IDs
- The function ensures type safety by preventing direct casting between FullTransactionId and Datum
- Commonly used through the PG_RETURN_FULLTRANSACTIONID macro for returning function results
- Complementary function to DatumGetFullTransactionId, forming a bidirectional conversion pair
- Essential for PostgreSQL functions that work with transaction snapshots and extended transaction IDs