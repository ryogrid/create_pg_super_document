# outDatum

## Location
[src/backend/nodes/outfuncs.c:341-381](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/outfuncs.c#L341-L381)

## Overview
Serializes a Datum value to a string representation by printing its raw byte content as an array of integers, handling both pass-by-value and pass-by-reference data types.

## Definition

```c
void
outDatum(StringInfo str, Datum value, int typlen, bool typbyval)
```
## Detailed Description
The  function is a core utility in PostgreSQL's node serialization system that converts a Datum (PostgreSQL's universal data container) into a human-readable string format. It handles the fundamental distinction between pass-by-value and pass-by-reference data types in PostgreSQL's type system.

For pass-by-value types (typbyval=true), the function treats the Datum itself as the data container and prints all bytes of the Datum structure. For pass-by-reference types (typbyval=false), it dereferences the pointer stored in the Datum and prints the actual data bytes.

The output format is consistent:  where length is the total size in bytes, followed by each byte represented as an integer value.

## Parameters / Member Variables
- : StringInfo buffer where the serialized output is appended
- : The Datum value to be serialized
- : The length specification of the data type (-1 for variable length, >0 for fixed length)
- : Boolean indicating whether the type is passed by value (true) or by reference (false)

## Dependencies
- Functions called/Symbols referenced:
  - [datumGetSize](../d/datumGetSize.md)
  - [DatumGetPointer](../D/DatumGetPointer.md)  
  - PointerIsValid
  - appendStringInfo
  - appendStringInfoChar
  - appendStringInfoString
- Called from (representative examples):
  - [_outConst](_outConst.md)

## Notes and Other Information
This function is part of PostgreSQL's node output system and is primarily used for debugging and plan visualization. The raw byte representation allows developers to inspect the actual memory contents of PostgreSQL data values, which is particularly useful when debugging type-related issues or understanding how different data types are stored internally. The function handles null pointers gracefully by outputting "0 [ ]" for invalid pointers in pass-by-reference types.