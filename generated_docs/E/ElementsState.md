# ElementsState

## Location
[src/backend/utils/adt/jsonfuncs.c:121-132](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L121-L132)

## Overview
ElementsState is a structure that maintains state information for the json_array_elements functionality, which is used to extract individual elements from JSON arrays.

## Definition


## Detailed Description
ElementsState serves as a context structure for PostgreSQL's JSON array elements functionality. It encapsulates all the necessary state information required to process JSON arrays and extract individual elements in a tabular format. The structure coordinates JSON lexical parsing, tuple storage, memory management, and result normalization during the array element extraction process.

## Parameters / Member Variables
- : Pointer to JsonLexContext for JSON lexical analysis and parsing
- : Name of the function being executed (for error reporting)
- : Tuplestorestate for storing the resulting array elements as tuples
- : TupleDesc describing the structure of returned tuples
- : MemoryContext for temporary memory allocation during processing
- : Pointer to the start of the current result string
- : Boolean flag indicating whether results should be normalized
- : Boolean flag indicating if the next value to process is a scalar
- : Pointer to the normalized scalar value string

## Dependencies
- Functions called/Symbols referenced:
  - [JsonLexContext](../J/JsonLexContext.md)
  - Tuplestorestate
- Called from (representative examples):
  - [elements_worker](../e/elements_worker.md)
  - [elements_array_element_start](../e/elements_array_element_start.md)
  - [elements_array_element_end](../e/elements_array_element_end.md)
  - [elements_object_start](../e/elements_object_start.md)
  - [elements_scalar](../e/elements_scalar.md)

## Notes and Other Information
This structure is specifically designed for the json_array_elements family of functions in PostgreSQL, which allow users to extract individual elements from JSON arrays in a tabular format. Unlike EachState which handles key-value pairs from objects, ElementsState focuses on extracting sequential elements from arrays. The function_name field is particularly useful for providing context-specific error messages during processing.