# chooseScript

## Location
src/bin/pgbench/pgbench.c: 3047 - 3068

## Overview
The chooseScript function selects a SQL script with weighted random choice from available scripts in pgbench.

## Definition


## Detailed Description
This function implements weighted random selection of SQL scripts for pgbench execution. When multiple scripts are configured with different weights, it uses a random number generator to select one script based on the probability distribution defined by their relative weights. If only one script is available, it immediately returns index 0 without any random selection.

The algorithm works by:
1. Generating a random number within the total weight range
2. Iterating through scripts and subtracting each script's weight from the random number
3. Returning the index of the script where the remaining weight becomes negative

## Parameters / Member Variables
- : Pointer to TState structure containing thread-specific state including the random state for script selection

## Dependencies
- Functions called/Symbols referenced:
  - getrand (for generating weighted random numbers)
  - TState (thread state structure)
- Called from (representative examples):
  - advanceConnectionState

## Notes and Other Information
- This function is critical for load balancing in pgbench when multiple scripts with different weights are used
- The function assumes that  and  array are properly initialized globally
- For single script scenarios, it provides an optimization by avoiding random number generation
- The weighted selection ensures that scripts with higher weights are chosen more frequently