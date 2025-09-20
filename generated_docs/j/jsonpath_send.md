# jsonpath_send

## Location
[src/backend/utils/adt/jsonpath.c:147-172](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath.c#L147-L172)

## Overview
The  function is a PostgreSQL binary send function for the jsonpath data type, responsible for serializing jsonpath values into PostgreSQL's binary protocol format for transmission.

## Definition

```c
Datum
jsonpath_send(PG_FUNCTION_ARGS)
```
## Detailed Description
 handles the binary serialization of jsonpath values for transmission over PostgreSQL's binary protocol. This function is the counterpart to  and is used when client applications request binary format data or during replication operations. The function implements a versioned binary format to ensure compatibility and allow for future format evolution.

The current implementation serializes the jsonpath as a version number followed by its text representation. This design provides a balance between efficiency and simplicity, allowing the reuse of existing text conversion logic while maintaining the binary protocol's performance benefits. The function first converts the jsonpath to its string representation using , then packages it with a version header for binary transmission.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro containing:
  - : The input JsonPath structure to be serialized for binary transmission

## Dependencies
- Functions called/Symbols referenced:
  - : PostgreSQL macro for extracting jsonpath arguments
  - : Core serialization function that converts jsonpath to string format
  - : PostgreSQL macro for calculating the size of variable-length data types
  - : Constant defining the current binary format version
  - : Initializes a StringInfo buffer for text conversion
  - : Begins binary type serialization
  - : Sends an 8-bit integer (version number) in binary format
  - : Sends text data in binary format
  - : Completes binary type serialization and returns the result
  - : Frees allocated memory
  - : PostgreSQL macro for returning binary data
  - : The internal structure type representing a compiled JSON path expression
- Called from (representative examples):
  - No direct references found (typically called automatically by PostgreSQL's type system during binary protocol operations)

## Notes and Other Information
- This function is automatically invoked by PostgreSQL's type system during binary protocol operations
- The versioned binary format allows for future enhancements while maintaining backward compatibility
- Currently uses version 1 format, which embeds the text representation with a version prefix
- Memory management includes proper cleanup of temporary StringInfo buffers
- Part of PostgreSQL's binary protocol support for efficient client-server communication
- Provides the serialization counterpart to  for complete binary protocol support