# JsonbToCString

## Location
[src/backend/utils/adt/jsonb.c:473-481](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb.c#L473-L481)

## Overview
A public function that converts a JSONB container to its C-string representation without indentation, serving as a convenience wrapper for JsonbToCStringWorker.

## Definition


## Detailed Description
This function provides a simple interface for converting JSONB data to its string representation. It serves as a wrapper around JsonbToCStringWorker with indentation disabled (false). The function can either allocate a new string or append to an existing StringInfo buffer, making it flexible for different use cases. It is commonly used for JSONB output functions and when converting JSONB values to text format. The function ensures efficient memory usage by accepting an estimated length parameter for buffer pre-allocation.

## Parameters / Member Variables
- : Optional StringInfo buffer where the result will be stored; if NULL, a new string is allocated
- : Pointer to the JsonbContainer structure containing the JSONB data to be converted
- : Estimated length of the resulting string for buffer pre-allocation optimization

## Dependencies
- Functions called/Symbols referenced:
  - [JsonbToCStringWorker](JsonbToCStringWorker.md) (the core conversion function with indentation control)
  - [JsonbContainer](JsonbContainer.md) (JSONB container structure type)
- Called from (representative examples):
  - [jsonb_out](../j/jsonb_out.md) (primary JSONB output function)
  - [jsonb_send](../j/jsonb_send.md) (binary send function)
  - [JsonbUnquote](JsonbUnquote.md) (for unquoting JSONB strings)
  - [jsonb_get_element](../j/jsonb_get_element.md), JsonbValueAsText, populate_scalar (various JSONB utility functions)

## Notes and Other Information
- This is a public function (no static keyword) accessible to other modules
- Always calls JsonbToCStringWorker with indent=false for compact output
- Returns a C-string that the caller is responsible for managing (if out was NULL)
- The estimated_len parameter helps optimize memory allocation but is not mandatory for correctness
- Used extensively throughout PostgreSQL's JSONB subsystem for text conversion
- The function design allows for both in-place buffer writing and new string allocation
- Part of PostgreSQL's JSONB API for external consumption