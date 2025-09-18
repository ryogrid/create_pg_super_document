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
  - pq_beginmessage_reuse (message protocol initiation)
  - [pq_sendint16](../p/pq_sendint16.md) (protocol integer writing)
  - enlargeStringInfo (buffer preallocation)
  - [getBaseTypeAndTypmod](../g/getBaseTypeAndTypmod.md) (domain type resolution)  
  - list_head, lnext (list traversal)
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