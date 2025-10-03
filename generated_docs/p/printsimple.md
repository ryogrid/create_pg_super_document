# printsimple

## Location
[src/backend/access/common/printsimple.c:59-143](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/printsimple.c#L59-L143)

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
  - [slot_getallattrs](../s/slot_getallattrs.md)
  - [pq_beginmessage](pq_beginmessage.md)
  - [pq_sendint16](pq_sendint16.md)
  - [pq_sendint32](pq_sendint32.md)
  - [pq_sendcountedtext](pq_sendcountedtext.md)
  - [pq_endmessage](pq_endmessage.md)
  - DatumGetTextPP
  - [DatumGetInt32](../D/DatumGetInt32.md)
  - [DatumGetInt64](../D/DatumGetInt64.md)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - [pg_ltoa](pg_ltoa.md)
  - [pg_lltoa](pg_lltoa.md)
  - [pg_ultoa_n](pg_ultoa_n.md)
  - TupleDescAttr
  - VARDATA_ANY
  - VARSIZE_ANY_EXHDR
  - PqMsg_DataRow
  - TEXTOID, INT4OID, INT8OID, OIDOID
  - MAXINT8LEN
  - elog
- Called from (representative examples):
  - [donothingCleanup](../d/donothingCleanup.md) (referenced in dest.c)

## Notes and Other Information
- Only supports TEXTOID, INT4OID, INT8OID, and OIDOID data types
- Throws an ERROR for unsupported data types
- Cannot use regular type output functions due to potential lack of catalog access
- NULL values are represented by sending -1 as the column length
- Returns true on successful completion
- Part of the simplified result output system for scenarios with limited catalog access
- Uses hardcoded type conversion logic instead of the dynamic type system

## Simplified Source

```c
bool printsimple(TupleTableSlot *slot, DestReceiver *self) {
    TupleDesc tupdesc = slot->tts_tupleDescriptor;
    StringInfoData buf;
    int i;

    // Ensure all attributes are deconstructed
    slot_getallattrs(slot);

    // Begin DataRow message
    pq_beginmessage(&buf, PqMsg_DataRow);
    pq_sendint16(&buf, tupdesc->natts);

    // Send each column value
    for (i = 0; i < tupdesc->natts; ++i) {
        Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
        Datum value;

        // Handle NULL values
        if (slot->tts_isnull[i]) {
            pq_sendint32(&buf, -1);
            continue;
        }

        value = slot->tts_values[i];

        // Convert based on hardcoded type knowledge
        switch (attr->atttypid) {
            case TEXTOID: {
                text *t = DatumGetTextPP(value);
                pq_sendcountedtext(&buf, VARDATA_ANY(t), VARSIZE_ANY_EXHDR(t));
                break;
            }
            case INT4OID: {
                int32 num = DatumGetInt32(value);
                char str[12];  // sign, 10 digits and '\0'
                int len = pg_ltoa(num, str);
                pq_sendcountedtext(&buf, str, len);
                break;
            }
            case INT8OID: {
                int64 num = DatumGetInt64(value);
                char str[MAXINT8LEN + 1];
                int len = pg_lltoa(num, str);
                pq_sendcountedtext(&buf, str, len);
                break;
            }
            case OIDOID: {
                Oid num = ObjectIdGetDatum(value);
                char str[10];  // 10 digits
                int len = pg_ultoa_n(num, str);
                pq_sendcountedtext(&buf, str, len);
                break;
            }
            default:
                elog(ERROR, "unsupported type OID: %u", attr->atttypid);
        }
    }

    pq_endmessage(&buf);
    return true;
}
```