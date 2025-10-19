# chooseScript

## Location
[src/bin/pgbench/pgbench.c:3047-3068](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L3047-L3068)

## Overview
The chooseScript function selects a SQL script with weighted random choice from available scripts in pgbench.

## Definition

```c
static int
chooseScript(TState *thread)
```
## Detailed Description
This function implements weighted random selection of SQL scripts for pgbench execution. When multiple scripts are configured with different weights, it uses a random number generator to select one script based on the probability distribution defined by their relative weights. If only one script is available, it immediately returns index 0 without any random selection.

The algorithm works by:
1. Generating a random number within the total weight range
2. Iterating through scripts and subtracting each script's weight from the random number
3. Returning the index of the script where the remaining weight becomes negative

## Parameters / Member Variables
- `*thread`: Pointer to TState structure containing thread-specific state including the random state for script selection
## Dependencies
- Functions called/Symbols referenced:
  - [getrand](../g/getrand.md) (for generating weighted random numbers)
  - TState (thread state structure)
- Called from (representative examples):
  - [advanceConnectionState](../a/advanceConnectionState.md)

## Notes and Other Information
- This function is critical for load balancing in pgbench when multiple scripts with different weights are used
- The function assumes that  and  array are properly initialized globally
- For single script scenarios, it provides an optimization by avoiding random number generation
- The weighted selection ensures that scripts with higher weights are chosen more frequently

## Simplified Source

```c
static int chooseScript(TState *thread) {
    // Fast path for single script
    if (num_scripts == 1)
        return 0;

    // Generate random number in range [0, total_weight-1]
    int64 weight = getrand(&thread->ts_choose_rs, 0, total_weight - 1);

    // Find script by subtracting weights until negative
    int i = 0;
    do {
        weight -= sql_script[i++].weight;
    } while (weight >= 0);

    return i - 1;  // Return the selected script index
}
```