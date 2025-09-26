# EachState

## Location
[src/backend/utils/adt/jsonfuncs.c:108-118](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L108-L118)

## Overview
EachState is a structure that maintains state information for the json_each functionality, which is used to decompose JSON objects into key-value pairs.

## Definition

```c
typedef struct EachState
{
	JsonLexContext *lex;
	Tuplestorestate *tuple_store;
	TupleDesc	ret_tdesc;
	MemoryContext tmp_cxt;
	const char *result_start;
	bool		normalize_results;
	bool		next_scalar;
	char	   *normalized_scalar;
} EachState;
```
## Detailed Description
EachState serves as a context structure for PostgreSQL's JSON each functionality. It encapsulates all the necessary state information required to process JSON data and convert it into a tabular format with key-value pairs. The structure coordinates JSON lexical parsing, tuple storage, memory management, and result normalization during the decomposition process.

## Parameters / Member Variables
- `*lex`: Pointer to JsonLexContext for JSON lexical analysis and parsing
- `*tuple_store`: Tuplestorestate for storing the resulting key-value tuples
- `ret_tdesc`: TupleDesc describing the structure of returned tuples
- `tmp_cxt`: MemoryContext for temporary memory allocation during processing
- `*result_start`: Pointer to the start of the current result string
- `normalize_results`: Boolean flag indicating whether results should be normalized
- `next_scalar`: Boolean flag indicating if the next value to process is a scalar
- `*normalized_scalar`: Pointer to the normalized scalar value string
## Dependencies
- Functions called/Symbols referenced:
  - [JsonLexContext](../J/JsonLexContext.md)
  - [Tuplestorestate](../T/Tuplestorestate.md)
- Called from (representative examples):
  - [each_worker](../e/each_worker.md)
  - [each_object_field_start](../e/each_object_field_start.md)
  - [each_object_field_end](../e/each_object_field_end.md)
  - [each_array_start](../e/each_array_start.md)
  - [each_scalar](../e/each_scalar.md)

## Notes and Other Information
This structure is specifically designed for the json_each family of functions in PostgreSQL, which allow users to extract key-value pairs from JSON objects in a tabular format. The structure facilitates both the parsing phase (via JsonLexContext) and the result storage phase (via Tuplestorestate) of the operation.