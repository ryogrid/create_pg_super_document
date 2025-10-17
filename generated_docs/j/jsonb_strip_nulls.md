# jsonb_strip_nulls

## Location
[src/backend/utils/adt/jsonfuncs.c:4525-4582](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L4525-L4582)

## Overview
Removes all key-value pairs with null values from a JSONB object, returning a new JSONB object without the null-valued fields.

## Definition
```c
Datum jsonb_strip_nulls(PG_FUNCTION_ARGS)
```

## Detailed Description
The `jsonb_strip_nulls` function is a SQL function that takes a JSONB value and returns a copy with all key-value pairs containing null values removed. It works by iterating through the JSONB structure using JsonbIterator and selectively copying only non-null key-value pairs to a new JSONB object.

The function handles scalar JSONB values by returning them unchanged since scalars cannot contain key-value pairs. For objects, it uses a state machine approach where it temporarily stores keys and only adds them to the result if the corresponding value is not null.

## Parameters / Member Variables
- `jb`: Input JSONB value from which null-valued key-value pairs will be stripped

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_JSONB_P
  - JB_ROOT_IS_SCALAR
  - [JsonbIteratorInit](../J/JsonbIteratorInit.md)
  - [JsonbIteratorNext](../J/JsonbIteratorNext.md)
  - [pushJsonbValue](../p/pushJsonbValue.md)
  - [JsonbValueToJsonb](../J/JsonbValueToJsonb.md)
- Called from (representative examples):
  - No direct callers found (exposed as SQL function)

## Notes and Other Information
- Only removes key-value pairs where the value is explicitly null (jbvNull)
- Scalar JSONB values are returned unchanged
- Uses delayed key processing to avoid adding keys for null values
- The function preserves the structure of nested objects and arrays while only removing null-valued pairs at all levels
- Exposed as the SQL function `jsonb_strip_nulls(jsonb)`

## Simplified Source

```c
Datum jsonb_strip_nulls(PG_FUNCTION_ARGS) {
    Jsonb *jb = PG_GETARG_JSONB_P(0);
    JsonbIterator *it;
    JsonbParseState *parseState = NULL;
    JsonbValue *res = NULL;
    JsonbValue v, k;
    JsonbIteratorToken type;
    bool last_was_key = false;

    // Return scalar values unchanged
    if (JB_ROOT_IS_SCALAR(jb))
        PG_RETURN_POINTER(jb);

    it = JsonbIteratorInit(&jb->root);

    // Iterate through JSONB structure
    while ((type = JsonbIteratorNext(&it, &v, false)) != WJB_DONE) {
        if (type == WJB_KEY) {
            // Store key temporarily until we check the value
            k = v;
            last_was_key = true;
            continue;
        }

        if (last_was_key) {
            last_was_key = false;

            // Skip null values (don't add key-value pair)
            if (type == WJB_VALUE && v.type == jbvNull)
                continue;

            // Add the key now that we know value is not null
            pushJsonbValue(&parseState, WJB_KEY, &k);
        }

        // Add value or other token types
        if (type == WJB_VALUE || type == WJB_ELEM)
            res = pushJsonbValue(&parseState, type, &v);
        else
            res = pushJsonbValue(&parseState, type, NULL);
    }

    PG_RETURN_POINTER(JsonbValueToJsonb(res));
}
```