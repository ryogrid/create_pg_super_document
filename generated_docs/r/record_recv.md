# record_recv

## Location
src/backend/utils/adt/rowtypes.c: 480 - 686

## Overview
Converts a binary representation of a composite type (record) from network/storage format into PostgreSQL's internal binary format.

## Definition


## Detailed Description
The  function serves as the binary input conversion function for any composite type in PostgreSQL. It reads binary data from a  buffer (typically from network protocols or binary storage) and converts it into the internal  format. This function is part of PostgreSQL's binary I/O protocol, handling the conversion of externally formatted binary record data into the server's internal representation.

The function performs extensive validation including column count verification, type OID checking for built-in types, and proper handling of null values. It uses the PostgreSQL message buffer protocol to read column count, individual column type OIDs, and column data lengths. Each column value is processed using the appropriate type-specific binary receive function.

## Parameters / Member Variables
- :  buffer containing the binary data to be parsed
- : OID identifying the composite type being parsed
- : Type modifier for the composite type (-1 for standard composite types)

## Dependencies
- Functions called/Symbols referenced:
  - check_stack_depth: Stack overflow protection for recursive calls
  - [lookup_rowtype_tupdesc](../l/lookup_rowtype_tupdesc.md): Retrieves tuple descriptor for the record type
  - [pq_getmsgint](../p/pq_getmsgint.md): Reads integer values from message buffer
  - [getTypeBinaryInputInfo](../g/getTypeBinaryInputInfo.md): Gets binary input function info for column types
  - [ReceiveFunctionCall](../R/ReceiveFunctionCall.md): Calls type-specific binary receive functions
  - [heap_form_tuple](../h/heap_form_tuple.md): Creates heap tuple from values array
  - [format_type_extended](../f/format_type_extended.md): Formats type names for error messages
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md): Memory allocation in function context
  - ReleaseTupleDesc: Releases tuple descriptor reference

- Called from (representative examples):
  - Type system as registered binary receive function for composite types
  - Binary protocol handlers processing composite type data

## Notes and Other Information
- Implements comprehensive type safety by validating column type OIDs for built-in types only
- Uses message buffer cursor management to efficiently process binary data without copying
- Performs strict column count validation between expected and actual column counts
- Handles null values through special length encoding (-1 indicates NULL)
- Uses function-local caching (fn_extra) to optimize repeated calls with same type
- Validates that receive functions consume exactly the expected amount of binary data
- Memory management ensures result can be safely freed by caller
- Part of PostgreSQL's binary protocol for efficient data transfer