# JsonbSubWorkspace

## Location
src/backend/utils/adt/jsonbsubs.c: 27 - 33

## Overview
JsonbSubWorkspace is a workspace structure used for PostgreSQL's JSONB subscripting operations, storing execution state and metadata needed during JSONB array/object element access and assignment operations.

## Definition


## Detailed Description
JsonbSubWorkspace serves as the workspace component of PostgreSQL's SubscriptingRefState for JSONB subscripting execution. This structure maintains the necessary state information when processing JSONB subscript operations like  for both fetch and assignment operations.

The workspace is dynamically allocated with additional space for arrays of Datum and Oid values corresponding to the number of subscript expressions. It's used throughout the JSONB subscripting execution pipeline to track whether the root JSONB value is expected to be an array, store the data types of subscript expressions, and hold the converted subscript values in Datum format.

The structure is particularly important for handling mixed subscript types (integers for array indices, text for object keys) and for maintaining type information needed during the coercion and conversion processes that occur during subscript evaluation.

## Parameters / Member Variables
- : Boolean flag indicating whether the JSONB root value is expected to be an array (set to true when the first subscript is an integer)
- : Pointer to an array of OIDs representing the data types of the subscript expressions (typically INT4OID for integers or TEXTOID for text)
- : Pointer to an array of Datum values containing the actual subscript values converted to appropriate format for JSONB operations

## Dependencies
- Functions called/Symbols referenced:
  - (No direct function calls from the struct definition)
- Called from (representative examples):
  - jsonb_subscript_check_subscripts (src/backend/utils/adt/jsonbsubs.c:180)
  - jsonb_subscript_fetch (src/backend/utils/adt/jsonbsubs.c:240)
  - jsonb_subscript_assign (src/backend/utils/adt/jsonbsubs.c:266)
  - jsonb_exec_setup (src/backend/utils/adt/jsonbsubs.c:357, 363, 366)

## Notes and Other Information
- The workspace is allocated with additional memory beyond the basic struct size to accommodate variable-length arrays for indexOid and index members
- Memory allocation assumes sizeof(Datum) >= sizeof(Oid) for proper pointer alignment
- The expectArray flag is used to construct appropriate empty JSONB structures when the source is NULL
- Integer subscripts are converted to text format during processing since JSONB operations expect text-based paths
- The structure supports unlimited nesting levels, unlike array subscripting which has defined limits
- Type coercion is handled to support both integer indices (for arrays) and text keys (for objects) within the same subscript expression sequence