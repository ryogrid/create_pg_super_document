# directBoolConsistentFn

## Location
src/backend/access/gin/ginlogic.c: 65 - 88

## Overview
A helper function that calls a regular binary logic consistent function for GIN index scans, providing a wrapper around the user-defined consistent function.

## Definition


## Detailed Description
This function serves as a wrapper for calling user-defined consistent functions in GIN (Generalized Inverted Index) operations. It handles the setup and invocation of the actual consistent function stored in the scan key's function manager info. The function initializes the recheckCurItem flag to true as a safe default (forcing heap-level rechecking) and then calls the user's consistent function with all necessary parameters using the PostgreSQL function call interface.

## Parameters / Member Variables
- : A GinScanKey pointer containing all the information needed for the consistency check, including the function to call, query parameters, and result storage.

## Dependencies
- Functions called/Symbols referenced:
  - [GinScanKey](../G/GinScanKey.md) (struct type)
  - [FunctionCall8Coll](../F/FunctionCall8Coll.md) (function call interface with collation)
  - [UInt16GetDatum](../U/UInt16GetDatum.md) (datum conversion for strategy)
  - [UInt32GetDatum](../U/UInt32GetDatum.md) (datum conversion for user entries count)
  - GinTernaryValue (enum type)
- Called from (representative examples):
  - [shimTriConsistentFn](../s/shimTriConsistentFn.md)
  - [ginInitConsistentFunction](../g/ginInitConsistentFunction.md)

## Notes and Other Information
- This is a static function, only visible within the ginlogic.c compilation unit
- Part of the GIN indexing infrastructure in PostgreSQL
- Sets recheckCurItem to true by default as a safety measure in case the user's consistent function doesn't properly set it
- Uses FunctionCall8Coll to invoke the user-defined consistent function with proper collation support
- Passes 8 parameters to the consistent function: entry results, strategy, query, number of user entries, extra data, recheck flag pointer, query values, and query categories
- The function assumes the user's consistent function returns a boolean value
- Located in src/backend/access/gin/ginlogic.c:65-88