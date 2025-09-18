# printsimple

## Location
src/backend/access/common/printsimple.c: 59 - 143

## Overview
Sends a DataRow message containing tuple data to the client using a simplified output format that supports only specific hardcoded data types.

## Definition
```c
bool printsimple(TupleTableSlot *slot, DestReceiver *self)
```

## Detailed Description
The `printsimple` function is responsible for converting tuple data from a TupleTableSlot into PostgreSQL wire protocol DataRow messages and sending them to the client. This function implements a simplified output mechanism that only supports a limited set of hardcoded data types (TEXT, INT4, INT8, OID) rather than using the full type output system.

The function first ensures all attributes in the slot are deconstructed, then constructs a DataRow message containing the number of columns followed by the data for each column. For NULL values, it sends -1 as the length indicator. For non-NULL values, it converts the internal Datum representation to string format using type-specific conversion functions and sends the resulting text.

This simplified approach is used in contexts where the full catalog system may not be available, requiring hardcoded knowledge of supported types rather than dynamic type lookup.

## Parameters / Member Variables
- `slot`: TupleTableSlot pointer containing the tuple data to be sent
- `self`: DestReceiver pointer (destination receiver object, unused in this function)

## Dependencies
- Functions called/Symbols referenced:
  - slot_getallattrs
  - pq_beginmessage
  - pq_sendint16
  - pq_sendint32
  - pq_sendcountedtext
  - pq_endmessage
  - DatumGetTextPP
  - DatumGetInt32
  - DatumGetInt64
  - ObjectIdGetDatum
  - pg_ltoa
  - pg_lltoa
  - pg_ultoa_n
  - TupleDescAttr
  - VARDATA_ANY
  - VARSIZE_ANY_EXHDR
  - PqMsg_DataRow
  - TEXTOID, INT4OID, INT8OID, OIDOID
  - MAXINT8LEN
  - elog
- Called from (representative examples):
  - donothingCleanup (referenced in dest.c)

## Notes and Other Information
- Only supports TEXTOID, INT4OID, INT8OID, and OIDOID data types
- Throws an ERROR for unsupported data types
- Cannot use regular type output functions due to potential lack of catalog access
- NULL values are represented by sending -1 as the column length
- Returns true on successful completion
- Part of the simplified result output system for scenarios with limited catalog access
- Uses hardcoded type conversion logic instead of the dynamic type system