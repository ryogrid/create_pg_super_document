# xidsend

## Location
[src/backend/utils/adt/xid.c:66-79](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xid.c#L66-L79)

## Overview
The xidsend function converts PostgreSQL's internal xid (TransactionId) type to external binary format for transmission over the binary protocol.

## Definition
```c
Datum xidsend(PG_FUNCTION_ARGS)
```

## Detailed Description
xidsend serves as the binary output conversion function for the xid data type in PostgreSQL's binary protocol system. It takes an internal TransactionId value and converts it to binary format suitable for network transmission or binary storage. The function uses PostgreSQL's standard binary serialization utilities to create a properly formatted binary representation.

## Parameters / Member Variables
- `arg1`: The TransactionId value to be converted to binary format
- `buf`: StringInfoData structure used as a buffer for building the binary output

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TRANSACTIONID
  - [pq_begintypsend](../p/pq_begintypsend.md)
  - [pq_sendint32](../p/pq_sendint32.md)
  - [pq_endtypsend](../p/pq_endtypsend.md)
  - PG_RETURN_BYTEA_P
- Called from (representative examples):
  - No direct references found (likely called through PostgreSQL's type system infrastructure for binary protocol)

## Notes and Other Information
- This function is part of PostgreSQL's binary protocol support for the xid data type
- Uses the standard PostgreSQL function calling convention with PG_FUNCTION_ARGS
- Sends the transaction ID as a 32-bit integer using pq_sendint32()
- Essential for client-server communication when using binary format for xid values
- Works in conjunction with xidrecv() for bidirectional binary data conversion
- Returns a bytea (binary array) type containing the serialized transaction ID

## Simplified Source

```c
Datum xidsend(PG_FUNCTION_ARGS) {
    TransactionId transaction_id = PG_GETARG_TRANSACTIONID(0);
    StringInfoData buf;

    // Initialize binary output buffer
    pq_begintypsend(&buf);

    // Write transaction ID as 32-bit integer
    pq_sendint32(&buf, transaction_id);

    // Return the binary data
    PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}
```