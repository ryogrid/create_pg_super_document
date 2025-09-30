# json_build_object_worker

## Location
[src/backend/utils/adt/json.c:1215-1308](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/json.c#L1215-L1308)

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
  - [makeStringInfo](../m/makeStringInfo.md)
  - [json_unique_builder_init](json_unique_builder_init.md)
  - [json_unique_builder_get_throwawaybuf](json_unique_builder_get_throwawaybuf.md)
  - [add_json](../a/add_json.md)
  - [json_unique_check_key](json_unique_check_key.md)
  - [cstring_to_text_with_len](../c/cstring_to_text_with_len.md)
  - [appendStringInfoChar](../a/appendStringInfoChar.md)
  - [appendStringInfoString](../a/appendStringInfoString.md)
  - [pstrdup](../p/pstrdup.md)
- Types referenced:
  - [JsonUniqueBuilderState](../J/JsonUniqueBuilderState.md)
- Called from (representative examples):
  - [json_build_object](json_build_object.md)
  - [ExecEvalJsonConstructor](../E/ExecEvalJsonConstructor.md)

## Notes and Other Information
- The function requires an even number of arguments and will error if this constraint is violated
- NULL keys are not allowed and will cause an error
- When key uniqueness checking is enabled, duplicate keys trigger a DUPLICATE_JSON_OBJECT_KEY_VALUE error
- The function handles memory management carefully, especially when reallocating StringInfo buffers during key uniqueness checks
- Supports PostgreSQL's standard JSON formatting with proper escaping and type conversion
- Used both directly through SQL functions and internally by the expression evaluator
- Located in src/backend/utils/adt/json.c:1215-1308

## Simplified Source

```c
Datum
json_build_object_worker(int nargs, const Datum *args, const bool *nulls,
                        const Oid *types, bool absent_on_null, bool unique_keys)
{
    StringInfo result = makeStringInfo();
    JsonUniqueBuilderState unique_check;
    const char *sep = "";

    // Validate even number of arguments (key-value pairs)
    if (nargs % 2 != 0)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("argument list must have even number of elements")));

    // Start JSON object
    appendStringInfoChar(result, '{');

    // Initialize uniqueness checking if required
    if (unique_keys)
        json_unique_builder_init(&unique_check);

    // Process key-value pairs
    for (int i = 0; i < nargs; i += 2) {
        StringInfo out;
        bool skip = absent_on_null && nulls[i + 1];

        // Handle NULL keys (not allowed)
        if (nulls[i])
            ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                    errmsg("null value not allowed for object key")));

        // Determine output buffer based on skip/unique_keys
        if (skip && unique_keys)
            out = json_unique_builder_get_throwawaybuf(&unique_check);
        else {
            if (!skip) {
                appendStringInfoString(result, sep);
                sep = ", ";
            }
            out = result;
        }

        // Process key and check uniqueness
        int key_offset = out->len;
        add_json(args[i], false, out, types[i], true);

        if (unique_keys) {
            const char *key = pstrdup(&out->data[key_offset]);
            if (!json_unique_check_key(&unique_check.check, key, 0))
                ereport(ERROR, (errcode(ERRCODE_DUPLICATE_JSON_OBJECT_KEY_VALUE),
                        errmsg("duplicate JSON object key value: %s", key)));

            if (skip) continue;
        }

        // Add separator and process value
        appendStringInfoString(result, " : ");
        add_json(args[i + 1], nulls[i + 1], result, types[i + 1], false);
    }

    // Close JSON object
    appendStringInfoChar(result, '}');

    return PointerGetDatum(cstring_to_text_with_len(result->data, result->len));
}
```