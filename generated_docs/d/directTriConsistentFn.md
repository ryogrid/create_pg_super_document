# directTriConsistentFn

## Location
src/backend/access/gin/ginlogic.c: 89 - 107

## Overview
A helper function that calls a native ternary logic consistent function for GIN index scans, providing direct access to user-defined ternary consistent functions.

## Definition


## Detailed Description
This function serves as a wrapper for calling user-defined ternary consistent functions in GIN (Generalized Inverted Index) operations. Unlike the binary logic version (directBoolConsistentFn), this function calls the ternary version of the consistent function which can return three states: GIN_FALSE, GIN_MAYBE, or GIN_TRUE. It uses FunctionCall7Coll to invoke the user's ternary consistent function with the appropriate parameters and returns the result as a GinTernaryValue.

## Parameters / Member Variables
- : A GinScanKey pointer containing all the information needed for the ternary consistency check, including the ternary function to call and query parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [GinScanKey](../G/GinScanKey.md) (struct type)
  - [FunctionCall7Coll](../F/FunctionCall7Coll.md) (function call interface with collation for 7 parameters)
  - DatumGetGinTernaryValue (datum conversion to GinTernaryValue)
  - [UInt16GetDatum](../U/UInt16GetDatum.md) (datum conversion for strategy)
  - [UInt32GetDatum](../U/UInt32GetDatum.md) (datum conversion for user entries count)
- Called from (representative examples):
  - [ginInitConsistentFunction](../g/ginInitConsistentFunction.md)

## Notes and Other Information
- This is a static function, only visible within the ginlogic.c compilation unit
- Part of the GIN indexing infrastructure in PostgreSQL
- Uses FunctionCall7Coll instead of FunctionCall8Coll (used by directBoolConsistentFn) because ternary functions don't need the recheckCurItem parameter
- Calls the triConsistentFmgrInfo function instead of consistentFmgrInfo
- Returns a GinTernaryValue which allows for more nuanced consistency checking than simple boolean logic
- Passes 7 parameters to the ternary consistent function: entry results, strategy, query, number of user entries, extra data, query values, and query categories
- No recheckCurItem handling since ternary logic can express uncertainty directly through GIN_MAYBE
- Located in src/backend/access/gin/ginlogic.c:89-107