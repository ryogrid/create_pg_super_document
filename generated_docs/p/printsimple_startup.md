# printsimple_startup

## Location
[src/backend/access/common/printsimple.c:31-58](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/printsimple.c#L31-L58)

## Overview
Sends a RowDescription message to the client at startup time, providing metadata about the columns in the result set that will follow.

## Definition

```c
structed */
	slot_getallattrs(slot);
```
## Detailed Description
The  function is part of PostgreSQL's result destination receiver system. It constructs and sends a RowDescription message to the client during query startup, which describes the structure of the result set that will be returned. This message contains column metadata including names, types, lengths, and format information that allows the client to properly interpret the subsequent data rows.

The function iterates through each attribute in the tuple descriptor and sends the column information using the PostgreSQL wire protocol format. It sends a PostgreSQL message of type  containing the number of attributes followed by detailed information for each column.

## Parameters / Member Variables
- : DestReceiver pointer (destination receiver object, unused in this function)
- : Integer operation code (unused in this function) 
- : TupleDesc pointer containing the tuple descriptor with column metadata to send

## Dependencies
- Functions called/Symbols referenced:
  - [pq_beginmessage](pq_beginmessage.md)
  - [pq_sendint16](pq_sendint16.md)
  - [pq_sendstring](pq_sendstring.md)
  - [pq_sendint32](pq_sendint32.md)
  - [pq_endmessage](pq_endmessage.md)
  - TupleDescAttr
  - NameStr
  - PqMsg_RowDescription
  - [DestReceiver](../D/DestReceiver.md)
- Called from (representative examples):
  - [donothingCleanup](../d/donothingCleanup.md) (referenced in dest.c)

## Notes and Other Information
- This function is part of the "printsimple" destination receiver implementation
- Sends hardcoded values for table OID (0), attribute number (0), and format code (0) as this is a simplified output format
- The function follows PostgreSQL's wire protocol for RowDescription messages
- Part of the access/common subsystem for handling simple result output formatting

## Simplified Source

```c
void printsimple_startup(DestReceiver *self, int operation, TupleDesc tupdesc) {
    StringInfoData buf;
    int i;

    // Begin RowDescription message
    pq_beginmessage(&buf, PqMsg_RowDescription);
    pq_sendint16(&buf, tupdesc->natts);

    // Send column metadata for each attribute
    for (i = 0; i < tupdesc->natts; ++i) {
        Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

        pq_sendstring(&buf, NameStr(attr->attname));
        pq_sendint32(&buf, 0);  // table oid (hardcoded)
        pq_sendint16(&buf, 0);  // attnum (hardcoded)
        pq_sendint32(&buf, (int) attr->atttypid);
        pq_sendint16(&buf, attr->attlen);
        pq_sendint32(&buf, attr->atttypmod);
        pq_sendint16(&buf, 0);  // format code (hardcoded)
    }

    pq_endmessage(&buf);
}
```