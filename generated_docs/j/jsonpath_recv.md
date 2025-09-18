# jsonpath_recv

## Location
[src/backend/utils/adt/jsonpath.c:115-133](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath.c#L115-L133)

## Overview
The  function is a PostgreSQL binary receive function for the jsonpath data type, responsible for deserializing jsonpath values from PostgreSQL's binary protocol format.

## Definition


## Detailed Description
 handles the binary deserialization of jsonpath values transmitted over PostgreSQL's binary protocol. Unlike the text-based  function, this function processes binary-encoded data received from client applications or during replication. The function implements a versioned binary format to allow for future format changes while maintaining backward compatibility.

Currently, only version 1 of the binary format is supported, which transmits the jsonpath as text prefixed with a version number. The function extracts the version number, validates it, then extracts the text representation and delegates parsing to . This design allows for efficient binary transmission while reusing the existing text parsing logic.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro containing:
  - : StringInfo buffer containing the binary-encoded jsonpath data
  - Version byte followed by the text representation of the JSON path expression

## Dependencies
- Functions called/Symbols referenced:
  - : Extracts the version number from the binary message buffer
  - : Extracts the text representation from the binary message buffer
  - : Core parsing function that converts the text to internal jsonpath format
  - : Constant defining the current supported binary format version
  - : PostgreSQL error logging function for unsupported versions
- Called from (representative examples):
  - No direct references found (typically called automatically by PostgreSQL's type system during binary protocol operations)

## Notes and Other Information
- This function is automatically invoked by PostgreSQL's type system during binary protocol operations
- The versioned binary format allows for future enhancements to the serialization format
- Currently only version 1 is supported; other versions trigger an error
- The binary format is essentially text-based with a version prefix, allowing reuse of existing parsing logic
- Memory allocation is handled through PostgreSQL's memory context system (NULL context passed to jsonPathFromCstring)
- Part of PostgreSQL's binary protocol support for efficient client-server communication