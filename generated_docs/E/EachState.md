# EachState

## Location
src/backend/utils/adt/jsonfuncs.c: 108 - 118

## Overview
EachState is a structure that maintains state information for the json_each functionality, which is used to decompose JSON objects into key-value pairs.

## Definition


## Detailed Description
EachState serves as a context structure for PostgreSQL's JSON each functionality. It encapsulates all the necessary state information required to process JSON data and convert it into a tabular format with key-value pairs. The structure coordinates JSON lexical parsing, tuple storage, memory management, and result normalization during the decomposition process.

## Parameters / Member Variables
- : Pointer to JsonLexContext for JSON lexical analysis and parsing
- : Tuplestorestate for storing the resulting key-value tuples
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
  - [each_worker](../e/each_worker.md)
  - [each_object_field_start](../e/each_object_field_start.md)
  - [each_object_field_end](../e/each_object_field_end.md)
  - [each_array_start](../e/each_array_start.md)
  - [each_scalar](../e/each_scalar.md)

## Notes and Other Information
This structure is specifically designed for the json_each family of functions in PostgreSQL, which allow users to extract key-value pairs from JSON objects in a tabular format. The structure facilitates both the parsing phase (via JsonLexContext) and the result storage phase (via Tuplestorestate) of the operation.