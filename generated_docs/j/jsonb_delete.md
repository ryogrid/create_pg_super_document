# jsonb_delete

## Location
[src/backend/utils/adt/jsonfuncs.c:4640-4692](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L4640-L4692)

## Overview
Removes a specified key-value pair or array element from a JSONB value, returning a new JSONB object without the deleted item.

## Definition
```c
Datum jsonb_delete(PG_FUNCTION_ARGS)
```

## Detailed Description
The `jsonb_delete` function removes a specified key from a JSONB object or a specified element from a JSONB array. It takes a JSONB value and a text key/index, then returns a new JSONB value with the matching element removed.

The function performs string comparison to match the key or array element. For objects, it removes the key-value pair where the key matches. For arrays, it removes the element that matches the provided value. The function cannot operate on scalar JSONB values and will throw an error if attempted.

## Parameters / Member Variables
- `in`: Input JSONB value from which to delete the specified item
- `key`: Text value specifying the key (for objects) or element value (for arrays) to delete

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_JSONB_P
  - PG_GETARG_TEXT_PP
  - VARDATA_ANY
  - VARSIZE_ANY_EXHDR
  - JB_ROOT_IS_SCALAR
  - JB_ROOT_COUNT
  - [JsonbIteratorInit](../J/JsonbIteratorInit.md)
  - [JsonbIteratorNext](../J/JsonbIteratorNext.md)
  - [pushJsonbValue](../p/pushJsonbValue.md)
  - [JsonbValueToJsonb](../J/JsonbValueToJsonb.md)
  - PG_RETURN_JSONB_P
  - ereport/ERROR
- Called from (representative examples):
  - No direct callers found (exposed as SQL function)

## Notes and Other Information
- Throws an error if applied to a scalar JSONB value
- Returns the original JSONB unchanged if it is empty (count = 0)
- Uses string comparison with memcmp for key/element matching
- When deleting object keys, also skips the corresponding value
- Uses skipNested flag to efficiently traverse nested structures
- Does not modify the original JSONB; returns a new copy
- Exposed as the SQL function `jsonb_delete(jsonb, text)`

## Simplified Source

```c
Datum jsonb_delete(PG_FUNCTION_ARGS) {
    Jsonb *in = PG_GETARG_JSONB_P(0);
    text *key = PG_GETARG_TEXT_PP(1);
    char *keyptr = VARDATA_ANY(key);
    int keylen = VARSIZE_ANY_EXHDR(key);
    JsonbParseState *state = NULL;
    JsonbIterator *it;
    JsonbValue v, *res = NULL;
    bool skipNested = false;
    JsonbIteratorToken r;

    // Error check: cannot delete from scalar values
    if (JB_ROOT_IS_SCALAR(in))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                       errmsg("cannot delete from scalar")));

    // Return unchanged if empty
    if (JB_ROOT_COUNT(in) == 0)
        PG_RETURN_JSONB_P(in);

    it = JsonbIteratorInit(&in->root);

    // Iterate through JSONB structure
    while ((r = JsonbIteratorNext(&it, &v, skipNested)) != WJB_DONE) {
        skipNested = true;

        // Check if current key/element matches deletion target
        if ((r == WJB_ELEM || r == WJB_KEY) &&
            (v.type == jbvString && keylen == v.val.string.len &&
             memcmp(keyptr, v.val.string.val, keylen) == 0)) {

            // Skip the matching key and its value
            if (r == WJB_KEY)
                JsonbIteratorNext(&it, &v, true);  // Skip value too
            continue;
        }

        // Add non-matching elements to result
        res = pushJsonbValue(&state, r, r < WJB_BEGIN_ARRAY ? &v : NULL);
    }

    PG_RETURN_JSONB_P(JsonbValueToJsonb(res));
}
```