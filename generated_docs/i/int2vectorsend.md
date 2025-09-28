# int2vectorsend

## Location
[src/backend/utils/adt/int.c:273-286](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int.c#L273-L286)

## Overview
Converts PostgreSQL's internal int2vector data type into binary format for network transmission or storage.

## Definition
```c
Datum int2vectorsend(PG_FUNCTION_ARGS)
```

## Detailed Description
This function provides the binary output protocol for int2vector data types, converting the internal representation into a binary format suitable for network transmission or persistent storage. It serves as a thin wrapper around the generic array_send function, delegating all the actual serialization work to the general array handling infrastructure. This design ensures consistency with other PostgreSQL array types while providing the specific entry point needed for int2vector binary output.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `fcinfo`: Function call information including the int2vector to be serialized

## Dependencies
- Functions called/Symbols referenced:
  - [array_send](../a/array_send.md) (generic array binary output function)
- Called from (representative examples):
  - PostgreSQL binary protocol handlers
  - Network communication and data storage systems

## Notes and Other Information
- Extremely simple wrapper function that delegates to array_send
- Maintains type system consistency by providing int2vector-specific entry point
- Essential for binary protocol communication and data serialization
- The generic array_send function handles all the complex binary formatting
- Part of the complete int2vector I/O function suite alongside int2vectorin, int2vectorout, and int2vectorrecv

## Simplified Source

```c
// Simplified version of int2vectorsend
Datum int2vectorsend(PG_FUNCTION_ARGS) {
    // Delegate entirely to generic array send function
    return array_send(fcinfo);
}
```

Key simplifications made:
- This function is already extremely simple - just a wrapper
- Added comment explaining the delegation pattern
- No further simplification needed as it's a pure passthrough function