# regprocsend

## Location
[src/backend/utils/adt/regproc.c:208-223](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regproc.c#L208-L223)

## Overview
Converts a regproc value to external binary format, serving as the binary output function for the regproc data type.

## Definition

```c
Datum
regprocsend(PG_FUNCTION_ARGS)
```
## Detailed Description
The regprocsend function is the binary output conversion function for PostgreSQL's regproc data type. It handles the conversion of internal regproc values to binary format suitable for external transmission (such as network protocols, file storage, or client libraries).

Since regproc values are internally represented as OIDs, this function is implemented as a simple wrapper that delegates to the standard oidsend function. This approach ensures consistent binary format handling between regproc and OID data types while maintaining the type system distinctions at the SQL level.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Function call information structure containing the regproc value to be converted to binary format
## Dependencies
- Functions called/Symbols referenced:
  - : Standard OID binary output function that performs the actual conversion
- Called from (representative examples):
  - No direct references found in the indexed codebase

## Notes and Other Information
- Code sharing: Delegates entirely to oidsend since regproc and OID share the same binary representation
- Binary protocol support: Enables efficient transmission of regproc values in PostgreSQL's binary wire protocol
- Network optimization: Binary format is more efficient than text format for network communication and storage
- Type system consistency: Maintains separate function identity while sharing implementation with OID type
- Complementary function: Works in tandem with regprocrecv to provide complete binary I/O support for regproc type

## Simplified Source

```c
Datum regprocsend(PG_FUNCTION_ARGS) {
    // Convert regproc to binary format by delegating to OID binary output
    // Since regproc is internally stored as OID, we can reuse oidsend
    return oidsend(fcinfo);
}
```