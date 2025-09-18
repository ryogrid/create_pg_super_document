# bit_recv

## Location
src/backend/utils/adt/varbit.c: 331 - 375

## Overview
Converts PostgreSQL's external binary representation of bit strings back to internal VarBit format, used for binary protocol communication and data transfer.

## Definition
```c
Datum bit_recv(PG_FUNCTION_ARGS)
```

## Detailed Description
The bit_recv function is PostgreSQL's binary input function for the BIT data type. It deserializes bit string data from PostgreSQL's binary wire protocol format back into the internal VarBit representation. This function is the counterpart to bit_send and is used when clients communicate with PostgreSQL using the binary protocol instead of text protocol.

The function performs the following operations:
1. Reads the bit length from the binary message buffer
2. Validates the bit length against system limits and type modifiers
3. Allocates appropriate memory for the internal VarBit structure
4. Copies the binary bit data from the message buffer
5. Ensures proper zero-padding of the last byte for bit boundary alignment

The binary format consists of:
- A 4-byte integer specifying the bit length
- The actual bit data packed into bytes, with the last byte zero-padded if necessary

This function is critical for client-server communication when using prepared statements, COPY BINARY format, or other binary protocol operations involving bit data.

## Parameters / Member Variables
- `buf`: StringInfo buffer containing the binary message data (from PG_GETARG_POINTER(0))
- `typelem`: Element type OID (unused, from PG_GETARG_OID(1))
- `atttypmod`: Type modifier specifying expected bit length (from PG_GETARG_INT32(2))

## Dependencies
- Functions called/Symbols referenced:
  - VarBit (type definition)
  - pq_getmsgint (binary protocol integer reading)
  - VARBITMAXLEN (maximum bit string length constant)
  - VARBITTOTALLEN (macro for calculating storage size)
  - SET_VARSIZE, VARBITLEN, VARBITS, VARBITBYTES (VarBit manipulation macros)
  - pq_copymsgbytes (binary protocol data copying)
  - VARBIT_PAD (zero-padding macro)
  - palloc (memory allocation)
  - ereport (error reporting)
  - PG_RETURN_VARBIT_P (return macro)
- Called from (representative examples):
  - Binary protocol handlers (automatically invoked)

## Notes and Other Information
- This is a PostgreSQL built-in function used by the binary protocol system
- Complements the bit_send function for complete binary serialization/deserialization
- Performs comprehensive validation to prevent buffer overruns and invalid data
- Ensures proper bit boundary alignment with zero-padding in the last byte
- Used internally by PostgreSQL when clients use binary protocol for bit data transfer
- Critical for performance in high-throughput scenarios where binary protocol is preferred over text
- The function handles both fixed-length BIT and variable-length VARBIT through the same implementation
- Validation includes both system limits (VARBITMAXLEN) and type-specific constraints (atttypmod)