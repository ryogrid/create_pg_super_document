# OkeysState

## Location
[src/backend/utils/adt/jsonfuncs.c:54-61](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L54-L61)

## Overview
OkeysState is a structure that maintains state information for the json_object_keys function, which extracts and returns the keys from a JSON object.

## Definition
```c
typedef struct OkeysState
{
    JsonLexContext *lex;
    char      **result;
    int         result_size;
    int         result_count;
    int         sent_count;
} OkeysState;
```

## Detailed Description
The OkeysState structure is used internally by PostgreSQL JSON processing functions to track the state during the extraction of object keys from JSON data. It serves as a container for managing the lexical context, result storage, and counting mechanisms needed for the json_object_keys operation. The structure is designed to handle dynamic result collection where keys are discovered incrementally during JSON parsing.

## Parameters / Member Variables
- `lex`: Pointer to JsonLexContext structure that provides the lexical parsing context for JSON processing
- `result`: Dynamic array of string pointers to store the extracted JSON object keys
- `result_size`: The current allocated size of the result array (capacity)
- `result_count`: The actual number of keys found and stored in the result array
- `sent_count`: Counter tracking how many results have been sent/processed

## Dependencies
- Functions called/Symbols referenced:
  - [JsonLexContext](../J/JsonLexContext.md)
- Called from (representative examples):
  - [jsonb_object_keys](../j/jsonb_object_keys.md)
  - [json_object_keys](../j/json_object_keys.md)
  - [okeys_object_field_start](../o/okeys_object_field_start.md)
  - [okeys_array_start](../o/okeys_array_start.md)
  - [okeys_scalar](../o/okeys_scalar.md)

## Notes and Other Information
This structure is specifically designed for the json_object_keys functionality and is used in both JSON and JSONB variants. The structure supports incremental key discovery and maintains separate counters for allocated space, actual results, and sent results, allowing for efficient memory management and result tracking during JSON parsing operations.