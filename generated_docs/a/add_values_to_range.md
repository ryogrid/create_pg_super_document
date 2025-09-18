# add_values_to_range

## Location
src/backend/access/brin/brin.c: 2196 - 2289

## Overview
Updates a BRIN range summary by incorporating new data values, modifying the summary statistics to ensure the range encompasses both existing and new values.

## Definition
static bool add_values_to_range(Relation idxRel, BrinDesc *bdesc, BrinMemTuple *dtup, const Datum *values, const bool *nulls)

## Detailed Description
This function is the core mechanism for updating BRIN index summaries when new data is inserted or when ranges need to be expanded. It takes new values and integrates them into an existing range summary, ensuring that the summary accurately represents all data within the range.

The function operates on a per-key basis, calling index access method specific addvalue functions for each indexed column. It handles several important aspects of BRIN summary maintenance:

1. **Empty range initialization**: If the range starts empty, it sets up initial summary values
2. **NULL value tracking**: Properly manages the presence of NULL values in the range using bv_hasnulls and bv_allnulls flags
3. **Per-key summary updates**: Calls operator class specific BRIN_PROCNUM_ADDVALUE functions to update column summaries
4. **Modification tracking**: Returns whether any changes were made to the summary, indicating if the tuple needs to be written back to disk

The function preserves existing NULL value information even when operator classes modify summary structures, ensuring no information is lost during updates.

## Parameters / Member Variables
- : The BRIN index relation containing metadata and operator information
- : BRIN descriptor with details about the index structure and column information  
- : In-memory BRIN tuple (BrinMemTuple) that will be modified with new summary values
- : Array of new data values to be incorporated into the range summary
- : Array of boolean flags indicating which values are NULL

## Dependencies
- Functions called/Symbols referenced:
  - [index_getprocinfo](../i/index_getprocinfo.md): Retrieves the addvalue function for each indexed column
  - [FunctionCall4Coll](../F/FunctionCall4Coll.md): Calls the operator class specific addvalue function with collation
  - [DatumGetBool](../D/DatumGetBool.md): Extracts boolean result from the addvalue function call
  - BRIN_PROCNUM_ADDVALUE: Procedure number for the addvalue function in BRIN operator classes
  - [BrinDesc](../B/BrinDesc.md), BrinMemTuple, BrinValues: Core BRIN data structures

- Called from (representative examples):
  - [brininsert](../b/brininsert.md): During insertion of new tuples into BRIN-indexed tables
  - [brinbuildCallback](../b/brinbuildCallback.md): During sequential BRIN index construction
  - [brinbuildCallbackParallel](../b/brinbuildCallbackParallel.md): During parallel BRIN index construction

## Notes and Other Information
- This is a static function, only accessible within the brin.c file
- Returns a boolean indicating whether the range summary was modified, allowing callers to determine if the tuple needs to be written back to storage
- Handles both regular NULL semantics and special BRIN null handling based on operator class configuration (oi_regular_nulls)
- Includes assertions to ensure logical consistency, such as verifying that empty ranges that become non-empty are marked as modified
- The function processes all keys even if early keys indicate modification, ensuring complete summary updates
- Critical for maintaining accurate BRIN index summaries as data is inserted or updated in the underlying table
- Preserves NULL value tracking information across operator class calls that might modify summary structures
- Part of the core BRIN index maintenance infrastructure used during both index construction and ongoing maintenance