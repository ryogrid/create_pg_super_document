# cash_send

## Location
[src/backend/utils/adt/cash.c:601-615](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/cash.c#L601-L615)

## Overview
Converts a PostgreSQL cash value to binary format for transmission over network connections or storage in binary format.

## Definition

```c
Datum
cash_send(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is part of PostgreSQL's binary I/O system for the money/cash data type. It takes a cash value as input and converts it to a binary representation using PostgreSQL's standard binary output protocol. This function is typically called when cash values need to be transmitted to clients in binary format or when performing binary serialization operations.

The function uses PostgreSQL's standard binary output functions to create a properly formatted binary representation that can be safely transmitted over network connections and later reconstructed using the corresponding receive function.

## Parameters / Member Variables
- Input: A single cash value retrieved via

## Dependencies
- Functions called/Symbols referenced:
  -  (data type)
  -  (macro to extract cash argument)
  -  (initialize binary output buffer)
  -  (send 64-bit integer in binary format)
  -  (finalize binary output buffer)
  -  (return binary data)
- Called from: 
  - Used internally by PostgreSQL's type system for binary output operations

## Notes and Other Information
- The cash data type is internally represented as a 64-bit integer
- This function is part of the binary I/O interface for the money data type
- The binary format ensures platform-independent representation of cash values
- Located in src/backend/utils/adt/cash.c:597-609

## Simplified Source

```c
// Convert Cash value to binary format
Datum cash_send(PG_FUNCTION_ARGS) {
    Cash cash_value = PG_GETARG_CASH(0);
    StringInfoData buffer;

    // Initialize binary output buffer
    pq_begintypsend(&buffer);

    // Write cash as 64-bit integer to buffer
    pq_sendint64(&buffer, cash_value);

    // Return finalized binary data
    PG_RETURN_BYTEA_P(pq_endtypsend(&buffer));
}
```