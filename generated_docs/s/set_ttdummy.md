# set_ttdummy

## Location
[src/test/regress/regress.c:464-501](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/regress/regress.c#L464-L501)

## Overview
This function provides a control mechanism to enable or disable the temporal table functionality implemented by the ttdummy trigger function.

## Definition
```c
Datum set_ttdummy(PG_FUNCTION_ARGS)
```

## Detailed Description
set_ttdummy is a PostgreSQL function that controls the global state of the temporal table system by toggling the ttoff global variable. The function takes an integer parameter that determines whether to enable (non-zero) or disable (zero) the temporal functionality. When temporal functionality is disabled (ttoff = true), the ttdummy trigger function will bypass all temporal processing and simply return the original tuples unchanged. The function returns the previous state of the temporal system: 0 if it was previously OFF, 1 if it was previously ON. This allows applications to save and restore the temporal state as needed.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that encapsulates:
  - `on`: An int32 value where 0 means disable temporal functionality, non-zero means enable it

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT32 (extracts int32 from function arguments)
  - PG_RETURN_INT32 (returns int32 result)
  - ttoff (global boolean variable controlling temporal state)
- Called from (representative examples):
  - [ttdummy](../t/ttdummy.md) (references this function in error messages)

## Notes and Other Information
- This function is part of PostgreSQL's regression test suite for testing temporal table functionality
- Provides a simple on/off switch for the entire temporal table system
- Returns the previous state, allowing for proper state management and restoration
- When temporal functionality is OFF, the ttdummy trigger becomes a pass-through operation
- The global variable ttoff is shared between this function and the ttdummy trigger function
- Essential for testing scenarios where temporal functionality needs to be temporarily disabled
- Located in src/test/regress/regress.c, indicating it's primarily for testing temporal database patterns
- Demonstrates a common pattern for providing runtime control over complex trigger behavior

## Simplified Source

```c
Datum set_ttdummy(PG_FUNCTION_ARGS) {
    int32 on = PG_GETARG_INT32(0);

    if (ttoff) {
        // Currently OFF
        if (on == 0)
            PG_RETURN_INT32(0);  // Stay OFF, return previous state

        // Turn ON
        ttoff = false;
        PG_RETURN_INT32(0);  // Return previous state (was OFF)
    }

    // Currently ON
    if (on != 0)
        PG_RETURN_INT32(1);  // Stay ON, return previous state

    // Turn OFF
    ttoff = true;
    PG_RETURN_INT32(1);  // Return previous state (was ON)
}
```