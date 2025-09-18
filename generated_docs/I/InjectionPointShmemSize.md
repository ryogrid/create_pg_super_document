# InjectionPointShmemSize

## Location
src/backend/utils/misc/injection_point.c: 232 - 247

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
- None (void function)

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