# findJsonbValueFromContainer

## Location
src/backend/utils/adt/jsonb_util.c: 341 - 394

## Overview
Searches for a value within a JSONB container (object or array) based on equality matching with a provided key/element value, supporting containment operations.

## Definition


## Detailed Description
This utility function facilitates "containment" operations by searching through JSONB containers to find matching values. For objects, it searches for values associated with keys that match the provided key (which must be a string). For arrays, it searches for elements that equal the provided value. The function supports both object and array container types through flag specification, but only processes one type per call based on the actual container type and requested flags.

The function performs different search strategies:
- For arrays: Iterates through all elements comparing them with the search key using equality
- For objects: Delegates to getKeyJsonValueFromContainer for key-based lookup

Returns a palloc()'d copy of the found value or NULL if not found. For objects, may return jbvBinary JsonbValue but never does so for arrays.

## Parameters / Member Variables
- : The JSONB container (object or array) to search within
- : Specifies container types of interest (JB_FARRAY for arrays, JB_FOBJECT for objects)
- : The JsonbValue to search for (must be string for object searches, any type for array searches)

## Dependencies
- Functions called/Symbols referenced:
  - JsonContainerSize
  - JsonContainerIsArray
  - JsonContainerIsObject
  - [fillJsonbValue](fillJsonbValue.md)
  - [equalsJsonbScalarValue](../e/equalsJsonbScalarValue.md)
  - [getKeyJsonValueFromContainer](../g/getKeyJsonValueFromContainer.md)
  - JBE_ADVANCE_OFFSET
- Called from (representative examples):
  - [jsonb_exists](../j/jsonb_exists.md)
  - [jsonb_exists_any](../j/jsonb_exists_any.md)
  - [jsonb_exists_all](../j/jsonb_exists_all.md)
  - [JsonbDeepContains](../J/JsonbDeepContains.md)
  - [executeItemOptUnwrapTarget](../e/executeItemOptUnwrapTarget.md)
  - [getJsonPathVariableFromJsonb](../g/getJsonPathVariableFromJsonb.md)

## Notes and Other Information
- Quick optimization: returns NULL immediately for empty containers without memory allocation
- Requires exactly one container type flag to be specified (JB_FARRAY or JB_FOBJECT)
- For object searches, the key parameter must be of type jbvString
- Memory management: caller is responsible for freeing the returned JsonbValue
- Falls through to return NULL if container type doesn't match the requested flags