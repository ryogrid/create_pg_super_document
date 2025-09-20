# JsonbInState

## Location
[src/backend/utils/adt/jsonb.c:28-34](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb.c#L28-L34)

## Overview
JsonbInState is a state structure used during the parsing and construction of JSONB values from input strings or data.

## Definition

```c
typedef struct JsonbInState
{
	JsonbParseState *parseState;
	JsonbValue *res;
	bool		unique_keys;
	Node	   *escontext;
} JsonbInState;
```
## Detailed Description
JsonbInState serves as a context structure that maintains the state during JSONB input processing operations. It encapsulates the parsing state, result storage, configuration options, and error handling context needed for converting various input formats into JSONB representation. This structure is extensively used throughout the JSONB input/output functions and aggregate operations to track parsing progress and maintain consistency during complex JSONB construction operations.

## Parameters / Member Variables
- : Pointer to JsonbParseState that tracks the current parsing state and manages the construction of JSONB containers
- : Pointer to JsonbValue that holds the final result of the parsing/construction operation
- : Boolean flag indicating whether duplicate keys should be eliminated during object construction
- : Pointer to Node structure used for error context handling and reporting

## Dependencies
- Functions called/Symbols referenced:
  - [JsonbParseState](JsonbParseState.md)
  - [JsonbValue](JsonbValue.md)
  - [Node](../N/Node.md)
- Called from (representative examples):
  - [jsonb_from_cstring](../j/jsonb_from_cstring.md)
  - [jsonb_agg_transfn_worker](../j/jsonb_agg_transfn_worker.md)
  - [jsonb_object_agg_transfn_worker](../j/jsonb_object_agg_transfn_worker.md)
  - datum_to_jsonb_internal
  - [jsonb_build_object_worker](../j/jsonb_build_object_worker.md)
  - [jsonb_build_array_worker](../j/jsonb_build_array_worker.md)

## Notes and Other Information
- This structure is fundamental to JSONB input processing and is used across many JSONB-related functions
- The unique_keys flag is particularly important for ensuring JSONB object key uniqueness requirements
- The escontext member enables proper error reporting during parsing operations
- Extensively used in aggregate functions like jsonb_agg() and jsonb_object_agg() for building JSONB results from multiple input values