# tsquerysend

## Location
[src/backend/utils/adt/tsquery.c:1189-1226](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsquery.c#L1189-L1226)

## Overview
Serializes a TSQuery structure into binary format for network transmission or storage purposes.

## Definition

```c
Datum
tsquerysend(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL binary output function that converts a TSQuery data structure into its binary representation. This function is part of PostgreSQL's full-text search functionality and is used for efficiently transmitting TSQuery objects over the network or storing them in binary format.

The binary format includes:
- A uint32 containing the number of operators/operands in the query
- For each operand (QI_VAL): type, weight, prefix flag, and null-terminated operand text
- For each operator (QI_OPR): type, operator code (OP_AND, OP_PHRASE, OP_OR, OP_NOT), and distance for phrase operators

The function processes the query items in prefix notation, ensuring proper serialization of the entire query tree structure.

## Parameters / Member Variables
This function uses PostgreSQL's function call convention:
- Uses  to retrieve the TSQuery input parameter
- Returns serialized binary data via 

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TSQUERY: Extract TSQuery from function arguments
  - GETQUERY: Get query items array from TSQuery
  - GETOPERAND: Get operand string data from TSQuery
  - [pq_begintypsend](../p/pq_begintypsend.md): Initialize binary output buffer
  - [pq_sendint32](../p/pq_sendint32.md): Send 32-bit integer
  - [pq_sendint8](../p/pq_sendint8.md): Send 8-bit integer  
  - [pq_sendint16](../p/pq_sendint16.md): Send 16-bit integer
  - [pq_sendstring](../p/pq_sendstring.md): Send null-terminated string
  - [pq_endtypsend](../p/pq_endtypsend.md): Finalize binary output buffer
  - PG_FREE_IF_COPY: Free input if it's a copy
  - PG_RETURN_BYTEA_P: Return binary data

- Called from (representative examples):
  - No direct references found in codebase (likely called via PostgreSQL's type system)

## Notes and Other Information
- This is a standard PostgreSQL binary output function for the TSQuery type
- The binary format preserves all query structure information including operator types, operand weights, and phrase distances
- Used internally by PostgreSQL for network communication and binary storage
- Counterpart to  which deserializes the binary format back to TSQuery
- Located in src/backend/utils/adt/tsquery.c:1189-1226