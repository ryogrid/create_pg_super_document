# varbit_recv

## Location
src/backend/utils/adt/varbit.c: 636 - 680

## Overview
Converts external binary format to PostgreSQL's internal VarBit representation during data input operations.

## Definition
```c
Datum varbit_recv(PG_FUNCTION_ARGS)
```

## Detailed Description
The `varbit_recv` function handles the conversion of binary protocol data into PostgreSQL's internal VarBit format. This function is part of PostgreSQL's binary I/O system and is called when receiving bit string data through the binary protocol (as opposed to text protocol). The external binary format consists of the bit length as a 32-bit integer followed by the actual byte array containing the bit data.

The function performs comprehensive validation including length bounds checking against VARBITMAXLEN and optional type modifier validation. When an atttypmod (attribute type modifier) is provided and is positive, the function ensures the incoming bit string doesn't exceed the specified maximum length. After creating the internal VarBit structure, it ensures proper zero-padding of the last byte to maintain data integrity.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `buf`: StringInfo buffer containing the binary data via `PG_GETARG_POINTER(0)`
  - `typelem`: Element type OID via `PG_GETARG_OID(1)` (currently unused)
  - `atttypmod`: Type modifier specifying maximum length via `PG_GETARG_INT32(2)`

## Dependencies
- Functions called/Symbols referenced:
  - pq_getmsgint
  - pq_copymsgbytes
  - VARBITMAXLEN
  - VARBITTOTALLEN
  - SET_VARSIZE
  - VARBITLEN
  - VARBITS
  - VARBITBYTES
  - VARBIT_PAD
  - PG_RETURN_VARBIT_P
- Called from:
  - No direct callers found (likely called by PostgreSQL's type system)

## Notes and Other Information
- Validates bit length against both negative values and VARBITMAXLEN to prevent invalid data
- Supports optional type modifier validation for length constraints
- Properly handles zero-padding of the final byte using VARBIT_PAD macro
- Uses PostgreSQL's message buffer API (pq_getmsgint, pq_copymsgbytes) for binary protocol handling
- Raises appropriate errors for invalid lengths and string truncation scenarios
- Located in src/backend/utils/adt/varbit.c:636-680