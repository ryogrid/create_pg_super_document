# xidrecv

## Location
[src/backend/utils/adt/xid.c:55-65](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xid.c#L55-L65)

## Overview
The xidrecv function converts external binary format data to PostgreSQL's internal xid (TransactionId) type, used in the binary protocol for data transmission.

## Definition
```c
Datum xidrecv(PG_FUNCTION_ARGS)
```

## Detailed Description
xidrecv serves as the binary input conversion function for the xid data type in PostgreSQL's binary protocol system. It receives binary data from a StringInfo buffer and converts it to the internal TransactionId format. This function is used when PostgreSQL receives xid values in binary format over network connections or from binary storage formats.

## Parameters / Member Variables
- `buf`: StringInfo buffer containing the binary representation of the transaction ID

## Dependencies
- Functions called/Symbols referenced:
  - [pq_getmsgint](../p/pq_getmsgint.md)
  - PG_RETURN_TRANSACTIONID
- Called from (representative examples):
  - No direct references found (likely called through PostgreSQL's type system infrastructure for binary protocol)

## Notes and Other Information
- This function is part of PostgreSQL's binary protocol support for the xid data type
- Uses the standard PostgreSQL function calling convention with PG_FUNCTION_ARGS
- Reads exactly sizeof(TransactionId) bytes from the input buffer using pq_getmsgint()
- Essential for client-server communication when using binary format for xid values
- Works in conjunction with xidsend() for bidirectional binary data conversion