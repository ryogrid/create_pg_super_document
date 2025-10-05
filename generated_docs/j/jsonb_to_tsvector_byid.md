# jsonb_to_tsvector_byid

## Location
[src/backend/tsearch/to_tsany.c:328-343](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/to_tsany.c#L328-L343)

## Overview
Converts JSONB data to a text search vector (TSVector) using a specified text search configuration and customizable flags for controlling which JSONB components to process.

## Definition

```c
Datum
jsonb_to_tsvector_byid(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL built-in function that provides comprehensive control over converting JSONB data into text search vectors. Unlike the string-specific variants, this function can process various components of JSONB data (keys, values, strings, numbers, etc.) based on the flags parameter. The function takes a text search configuration ID and a flags specification, allowing users to precisely control which parts of the JSONB structure contribute to the resulting TSVector.

The flags parameter is provided as JSONB and is parsed by  to determine processing behavior. This makes it the most flexible of the JSONB-to-TSVector conversion functions.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - : Object ID of the text search configuration (retrieved via )
  - : JSONB data structure to process (retrieved via )
  - : JSONB structure specifying processing flags (retrieved via )

## Dependencies
- Functions called/Symbols referenced:
  - : Parses JSONB flags specification into internal flag format
  - : Core worker function that performs the JSONB-to-TSVector conversion
  - : Macro to extract OID argument
  - : Macro to extract JSONB arguments
  - : Memory management for JSONB inputs
  - : Macro for returning TSVector result
  - : Result data type
  - : Input data types
- Called from (representative examples):
  - No direct references found in the analyzed codebase

## Notes and Other Information
- This function is part of PostgreSQL's full-text search system for JSON data
- Provides maximum flexibility in controlling which JSONB components are processed
- Requires explicit specification of text search configuration ID and processing flags
- The flags parameter allows fine-grained control over keys vs values, string vs numeric content, etc.
- For simpler use cases, consider  (default config) or string-specific variants
- Located in 
- Memory management includes proper cleanup of both JSONB input parameters
- Most comprehensive of the JSONB-to-TSVector conversion functions

## Simplified Source

```c
Datum jsonb_to_tsvector_byid(PG_FUNCTION_ARGS) {
    Oid config_id = PG_GETARG_OID(0);
    Jsonb *jsonb_input = PG_GETARG_JSONB_P(1);
    Jsonb *jsonb_flags = PG_GETARG_JSONB_P(2);
    TSVector result;
    uint32 flags;

    // Parse processing flags from JSONB specification
    flags = parse_jsonb_index_flags(jsonb_flags);

    // Convert JSONB to TSVector using specified config and flags
    result = jsonb_to_tsvector_worker(config_id, jsonb_input, flags);

    // Clean up input parameters
    PG_FREE_IF_COPY(jsonb_input, 1);
    PG_FREE_IF_COPY(jsonb_flags, 2);

    PG_RETURN_TSVECTOR(result);
}
```