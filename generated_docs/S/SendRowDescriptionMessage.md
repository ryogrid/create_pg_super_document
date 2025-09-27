# SendRowDescriptionMessage

## Location
[src/backend/access/common/printtup.c:166-249](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/printtup.c#L166-L249)

## Overview
The SendRowDescriptionMessage function sends a RowDescription message to the frontend client, providing metadata about the columns in a query result set according to PostgreSQL's wire protocol.

## Definition
```c
void SendRowDescriptionMessage(StringInfo buf, TupleDesc typeinfo, List *targetlist, int16 *formats)
```

## Detailed Description
This function constructs and sends a RowDescription message (T message) that informs the client about the structure of query results. It processes each attribute in the tuple descriptor, extracting column metadata including names, type information, and origin details. The function handles domain types by resolving them to their base types, skips resjunk columns from the target list, and includes format codes for each column. Performance is optimized by pre-allocating buffer space for the entire message and using efficient inline formatting functions. The message follows PostgreSQL's frontend/backend protocol specification for result set metadata.

## Parameters / Member Variables
- `buf`: StringInfo buffer where the RowDescription message will be constructed
- `typeinfo`: TupleDesc containing the tuple structure and attribute information  
- `targetlist`: List of TargetEntry nodes from the query plan (may be NIL for utility commands)
- `formats`: Array of format codes for each column (may be NULL, defaults to text format)

## Dependencies
- Functions called/Symbols referenced:
  - [pq_beginmessage_reuse](../p/pq_beginmessage_reuse.md) (message protocol initiation)
  - [pq_sendint16](../p/pq_sendint16.md) (protocol integer writing)
  - [enlargeStringInfo](../e/enlargeStringInfo.md) (buffer preallocation)
  - [getBaseTypeAndTypmod](../g/getBaseTypeAndTypmod.md) (domain type resolution)  
  - [list_head](../l/list_head.md), lnext (list traversal)
  - [pq_writestring](../p/pq_writestring.md), pq_writeint32, pq_writeint16 (protocol data writing)
  - [pq_endmessage_reuse](../p/pq_endmessage_reuse.md) (message protocol completion)
  - TupleDescAttr (attribute access macro)
- Called from (representative examples):
  - [printtup_startup](../p/printtup_startup.md)
  - [exec_describe_statement_message](../e/exec_describe_statement_message.md)
  - [exec_describe_portal_message](../e/exec_describe_portal_message.md)

## Notes and Other Information
- Handles domain types by sending the base type instead of the domain type to the client
- Skips resjunk columns which are internal to PostgreSQL execution but not part of user results
- Pre-allocates buffer space to avoid reallocations and improve performance with inline pqformat functions
- Accounts for character set conversion overhead when estimating column name sizes
- Sends zero values for table/column origin information when target list is not available
- Format codes default to 0 (text format) when formats array is NULL

## Simplified Source

```c
// Simplified version of SendRowDescriptionMessage
void SendRowDescriptionMessage(StringInfo buf, TupleDesc typeinfo,
                              List *targetlist, int16 *formats) {
    int natts = typeinfo->natts;
    ListCell *tlist_item = list_head(targetlist);

    // Start RowDescription message and send column count
    pq_beginmessage_reuse(buf, PqMsg_RowDescription);
    pq_sendint16(buf, natts);

    // Pre-allocate buffer space for performance
    enlargeStringInfo(buf, estimated_message_size * natts);

    // Process each column attribute
    for (int i = 0; i < natts; i++) {
        Form_pg_attribute att = TupleDescAttr(typeinfo, i);

        // Resolve domain types to base types
        Oid atttypid = att->atttypid;
        int32 atttypmod = att->atttypmod;
        atttypid = getBaseTypeAndTypmod(atttypid, &atttypmod);

        // Skip resjunk columns and get origin table info
        while (tlist_item && ((TargetEntry *) lfirst(tlist_item))->resjunk)
            tlist_item = lnext(targetlist, tlist_item);

        Oid resorigtbl = 0;
        AttrNumber resorigcol = 0;
        if (tlist_item) {
            TargetEntry *tle = (TargetEntry *) lfirst(tlist_item);
            resorigtbl = tle->resorigtbl;
            resorigcol = tle->resorigcol;
            tlist_item = lnext(targetlist, tlist_item);
        }

        // Get format code (default to text format if not specified)
        int16 format = formats ? formats[i] : 0;

        // Write column metadata to buffer
        pq_writestring(buf, NameStr(att->attname));  // Column name
        pq_writeint32(buf, resorigtbl);              // Origin table OID
        pq_writeint16(buf, resorigcol);              // Origin column number
        pq_writeint32(buf, atttypid);                // Data type OID
        pq_writeint16(buf, att->attlen);             // Type length
        pq_writeint32(buf, atttypmod);               // Type modifier
        pq_writeint16(buf, format);                  // Format code
    }

    // Finalize the message
    pq_endmessage_reuse(buf);
}
```

Key simplifications made:
- Removed detailed memory allocation calculation comments for clarity
- Simplified variable declarations and moved them closer to usage
- Added inline comments explaining the purpose of each major step
- Consolidated the origin table/column logic into a clearer flow
- Abstracted the complex memory size estimation with a descriptive variable name
- Focused on the main execution path while preserving all essential functionality