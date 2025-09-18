# jsonb_set

## Location
src/backend/utils/adt/jsonfuncs.c: 4844 - 4892

## Overview
Sets a value at a specified path in a JSONB structure, with optional creation of missing path components.

## Definition


## Detailed Description
The  function is a SQL-callable function that sets a value at a specified path within a JSONB structure. It accepts a path array specifying the location where to set the value, a new JSONB value to set, and a boolean flag indicating whether to create missing path components. The function traverses the JSONB structure using the provided path and either replaces an existing value or creates new path elements as needed.

The function validates input parameters, ensures the root is not a scalar, and uses the internal  function to perform the actual modification. It returns a new JSONB structure with the modification applied, following PostgreSQL's immutable data approach.

## Parameters / Member Variables
- : The input JSONB structure to modify
- : Array of text elements defining the path to the target location
- : The new JSONB value to set at the specified path
- : Whether to create missing path components (true) or only replace existing ones (false)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_JSONB_P: Extract JSONB argument from function call
  - PG_GETARG_ARRAYTYPE_P: Extract array argument from function call
  - PG_GETARG_BOOL: Extract boolean argument from function call
  - JsonbToJsonbValue: Convert Jsonb to JsonbValue
  - ARR_NDIM: Get number of array dimensions
  - JB_ROOT_IS_SCALAR: Check if JSONB root is scalar
  - JB_ROOT_COUNT: Get count of root elements
  - deconstruct_array_builtin: Deconstruct PostgreSQL array
  - JsonbIteratorInit: Initialize JSONB iterator
  - setPath: Internal function to set value at path
  - JsonbValueToJsonb: Convert JsonbValue to Jsonb
  - PG_RETURN_JSONB_P: Return JSONB value from function
- Called from (representative examples):
  - jsonb_set_lax: Called for lax version of set operation

## Notes and Other Information
- Only accepts one-dimensional path arrays
- Cannot set paths in scalar JSONB values
- When  is false and the root is empty, returns the original JSONB unchanged
- Empty path arrays return the original JSONB unchanged
- Uses  or  modes based on the  parameter
- File location: src/backend/utils/adt/jsonfuncs.c:4844-4892