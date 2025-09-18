# iterate_jsonb_values

## Location
src/backend/utils/adt/jsonfuncs.c: 5640 - 5707

## Overview
Iterates over jsonb values or elements according to specified flags and passes them to a callback function for processing.

## Definition


## Detailed Description
This function provides a generic mechanism for traversing a JSONB structure and applying a callback function to selected types of values. It uses the JSONB iterator infrastructure to walk through the entire JSONB structure, examining each token type and value type. Based on the provided flags, it selectively calls the action callback for keys, strings, numeric values, and boolean values. The function handles type conversion for numeric values (converting to string representation) and boolean values (converting to "true"/"false" strings) before passing them to the callback.

## Parameters / Member Variables
- : The JSONB structure to iterate over
- : Bitfield flags controlling which types of values to process (jtiKey, jtiString, jtiNumeric, jtiBool)
- : User-defined state object passed through to the callback function
- : Callback function of type JsonIterateStringValuesAction that processes each selected value

## Dependencies
- Functions called/Symbols referenced:
  - [JsonbIteratorInit](../J/JsonbIteratorInit.md)
  - [JsonbIteratorNext](../J/JsonbIteratorNext.md)
  - [DatumGetCString](../D/DatumGetCString.md)
  - DirectFunctionCall1
  - [numeric_out](../n/numeric_out.md)
  - [NumericGetDatum](../N/NumericGetDatum.md)
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - [jsonb_to_tsvector_worker](../j/jsonb_to_tsvector_worker.md)
  - pg_parse_json_or_ereport

## Notes and Other Information
The function processes different JSONB value types differently: strings are passed directly, numeric values are converted to their string representation using numeric_out, and boolean values are converted to literal "true" or "false" strings. Composite values (objects, arrays) are not passed to the callback. The iteration continues until the entire JSONB structure has been traversed.