# json_build_object_worker

## Location
src/backend/utils/adt/json.c: 1215 - 1308

## Overview
A worker function that constructs JSON objects from alternating key-value argument pairs, with support for key uniqueness validation and NULL value handling.

## Definition
```c
Datum json_build_object_worker(int nargs, const Datum *args, const bool *nulls, 
                              const Oid *types, bool absent_on_null, bool unique_keys)
```

## Detailed Description
This function serves as the core implementation for building JSON objects from variable-length argument lists. It processes pairs of arguments as key-value pairs, constructing a properly formatted JSON object string. The function supports advanced features including optional key uniqueness validation and configurable NULL value handling.

The function validates that the number of arguments is even (since each key must have a corresponding value), then iterates through the argument pairs. For each pair, it processes the key (which cannot be NULL) and the associated value, appending them to the result JSON string with proper formatting. When `unique_keys` is enabled, it maintains a hash table to detect duplicate keys and throws an error if duplicates are found. When `absent_on_null` is enabled, key-value pairs with NULL values are omitted from the output.

## Parameters / Member Variables
- `nargs`: Total number of arguments (must be even)
- `args`: Array of Datum arguments containing alternating keys and values
- `nulls`: Array indicating which arguments are NULL
- `types`: Array of Oid types for each argument
- `absent_on_null`: If true, skip pairs where the value is NULL
- `unique_keys`: If true, enforce key uniqueness and error on duplicates

## Dependencies
- Functions called/Symbols referenced:
  - makeStringInfo
  - json_unique_builder_init
  - json_unique_builder_get_throwawaybuf
  - add_json
  - json_unique_check_key
  - cstring_to_text_with_len
  - appendStringInfoChar
  - appendStringInfoString
  - pstrdup
- Types referenced:
  - JsonUniqueBuilderState
- Called from (representative examples):
  - json_build_object
  - ExecEvalJsonConstructor

## Notes and Other Information
- The function requires an even number of arguments and will error if this constraint is violated
- NULL keys are not allowed and will cause an error
- When key uniqueness checking is enabled, duplicate keys trigger a DUPLICATE_JSON_OBJECT_KEY_VALUE error
- The function handles memory management carefully, especially when reallocating StringInfo buffers during key uniqueness checks
- Supports PostgreSQL's standard JSON formatting with proper escaping and type conversion
- Used both directly through SQL functions and internally by the expression evaluator
- Located in src/backend/utils/adt/json.c:1215-1308