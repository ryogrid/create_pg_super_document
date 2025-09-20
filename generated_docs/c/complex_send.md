# complex_send

## Location
[src/tutorial/complex.c:85-104](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/tutorial/complex.c#L85-L104)

## Overview
Binary output function for the  data type that serializes a complex number to PostgreSQL's binary message format for network transmission.

## Definition

```c
Datum
complex_send(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is responsible for converting PostgreSQL's internal  data type representation into binary format suitable for network transmission or binary storage. It serializes the complex number by writing the real and imaginary parts as consecutive 8-byte double-precision floating-point values in network byte order. This function provides efficient binary data transfer capabilities and is the counterpart to .

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that provides access to function arguments
  -  (accessed via ): Pointer to the Complex structure to be serialized

## Dependencies
- Functions called/Symbols referenced:
  - : Macro to extract pointer argument
  - : Function to initialize binary output buffer
  - : Function to append 8-byte float to message buffer
  - : Function to finalize binary output buffer
  - : Macro to return bytea (binary) value
- Called from (representative examples):
  - : Referenced in the same file for function registration

## Notes and Other Information
- Counterpart to  for binary serialization/deserialization
- Writes binary data in network byte order (big-endian) for platform independence
- More efficient than text-based input/output for bulk data operations and storage
- Uses PostgreSQL's standard binary message protocol functions
- Returns a bytea (variable-length binary string) containing the serialized data
- Part of the PostgreSQL tutorial demonstrating custom data type implementation
- Located in src/tutorial/complex.c:85-104