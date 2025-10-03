# InjectionPointShmemSize

## Location
[src/backend/utils/misc/injection_point.c:232-247](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/injection_point.c#L232-L247)

## Overview
Returns the size of shared memory required for the injection points control structure, or 0 if injection points are not compiled in.

## Definition
```c
Size
InjectionPointShmemSize(void)
```

## Detailed Description
This function calculates and returns the shared memory size required for the injection points subsystem. When compiled with USE_INJECTION_POINTS defined, it returns the size of the InjectionPointsCtl structure, which contains the control data and array of injection point entries. When injection points are not compiled in (USE_INJECTION_POINTS not defined), it returns 0 to indicate no shared memory is needed. The function uses add_size() for safe size arithmetic to avoid overflow.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - [add_size](../a/add_size.md): Performs safe size addition to avoid arithmetic overflow
  - [InjectionPointsCtl](InjectionPointsCtl.md): The main shared memory control structure containing max_inuse counter and entries array
  - sizeof: Calculates the size of the InjectionPointsCtl structure
- Called from (representative examples):
  - [CalculateShmemSize](../C/CalculateShmemSize.md): Used during shared memory initialization to calculate total shared memory requirements

## Notes and Other Information
- The function is conditionally compiled based on USE_INJECTION_POINTS preprocessor macro
- [InjectionPointsCtl](InjectionPointsCtl.md) structure includes:
  - [pg_atomic_uint32](../p/pg_atomic_uint32.md) max_inuse: Optimization counter tracking highest used index + 1
  - [InjectionPointEntry](InjectionPointEntry.md) entries[MAX_INJECTION_POINTS]: Array of up to 128 injection point entries
- Returns 0 when injection points are disabled at compile time
- Part of the shared memory sizing infrastructure used during PostgreSQL startup
- The size calculation is straightforward since it's just a single structure with fixed-size array

## Simplified Source

```c
// Simplified version of InjectionPointShmemSize
Size InjectionPointShmemSize(void) {
#ifdef USE_INJECTION_POINTS
    // Calculate size needed for injection points control structure
    Size memory_size = 0;

    // Add size of the main control structure (contains counter + entries array)
    memory_size = add_size(memory_size, sizeof(InjectionPointsCtl));

    return memory_size;
#else
    // No injection points support compiled in - no memory needed
    return 0;
#endif
}
```

Key simplifications made:
- Added descriptive comments explaining each step
- Used more descriptive variable name (`memory_size` instead of `sz`)
- Separated the logic flow with clear comments
- Maintained the essential conditional compilation logic
- Kept the safe size arithmetic using `add_size()`