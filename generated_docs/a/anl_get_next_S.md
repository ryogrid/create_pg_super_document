# anl_get_next_S

## Location
src/backend/utils/misc/sampling.c: 296 - 304

## Overview
Computes the next skip value S for Vitter's Algorithm Z reservoir sampling, serving as a wrapper around reservoir_get_next_S for ANALYZE operations.

## Definition
```c
double anl_get_next_S(double t, int n, double *stateptr)
```

## Detailed Description
This function acts as an interface between ANALYZE operations and the low-level reservoir sampling implementation. It manages the W state value (probability multiplier) by storing it in the global oldrs structure, calling the core reservoir_get_next_S function to compute the next skip value S, and then retrieving the updated W value back to the caller's state pointer. This design allows ANALYZE operations to use Vitter's Algorithm Z without directly managing the ReservoirStateData structure.

## Parameters / Member Variables
- `t`: The current position in the data stream (number of records processed so far)
- `n`: The size of the reservoir (number of samples to maintain)
- `stateptr`: Pointer to a double containing the current W value, which will be updated with the new W value after computing the skip

## Dependencies
- Functions called/Symbols referenced:
  - reservoir_get_next_S
  - W (member of oldrs structure)
- Called from (representative examples):
  - Referenced in MAX_STATISTICS_TARGET context
  - Used in ReservoirState context

## Notes and Other Information
This function is part of PostgreSQL's implementation of Vitter's Algorithm Z for efficient reservoir sampling during table analysis. It provides a simplified interface that manages the W state value automatically, allowing ANALYZE operations to perform reservoir sampling by simply calling this function in a loop. The function updates the caller's state pointer with the new W value, which must be preserved between calls to maintain the correct sampling behavior. The skip value S returned indicates how many records should be skipped before selecting the next sample.