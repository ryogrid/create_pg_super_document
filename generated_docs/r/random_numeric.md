# random_numeric

## Location
src/backend/utils/adt/numeric.c: 4244 - 4298

## Overview
Generates a random numeric value within a specified range [rmin, rmax] using a provided pseudo-random number generator state.

## Definition


## Detailed Description
The `random_numeric` function generates random numeric values within the specified bounds using PostgreSQL's internal pseudo-random number generation. It performs thorough validation on the input bounds, rejecting NaN and infinity values with appropriate error messages. The function converts the external Numeric inputs to internal NumericVar format, calls the internal `random_var` function to perform the actual random generation, and converts the result back to external Numeric format.

## Parameters / Member Variables
- `state`: Pointer to a pseudo-random number generator state (pg_prng_state)
- `rmin`: Lower bound (Numeric) - must be finite (not NaN or infinity)  
- `rmax`: Upper bound (Numeric) - must be finite (not NaN or infinity)

## Dependencies
- Functions called/Symbols referenced:
  - `NUMERIC_IS_SPECIAL` - Checks if numeric value is special (NaN, infinity)
  - `NUMERIC_IS_NAN` - Specifically checks for NaN values
  - `ereport` - Reports errors with detailed error codes and messages
  - `init_var_from_num` - Converts external Numeric to internal NumericVar
  - `init_var` - Initializes a NumericVar structure
  - `random_var` - Performs the actual random generation between NumericVars
  - `make_result` - Converts NumericVar back to external Numeric format
  - `free_var` - Releases memory allocated for NumericVar
- Called from (representative examples):
  - `numeric_random` - Public SQL function interface in pseudorandomfuncs.c
  - Referenced in numeric.h header for external use

## Notes and Other Information
- Strict validation prevents invalid bounds (NaN, infinity) with descriptive error messages
- Uses PostgreSQL's standard error reporting mechanism with appropriate error codes
- The function is part of PostgreSQL's random number generation infrastructure
- Memory management is properly handled with cleanup of temporary NumericVar structures
- The actual random generation algorithm is delegated to the internal `random_var` function
- Located in src/backend/utils/adt/numeric.c:4244-4298