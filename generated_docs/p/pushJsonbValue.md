# pushJsonbValue

## Location
[src/backend/utils/adt/jsonb_util.c:563-636](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb_util.c#L563-L636)

## Overview
Incrementally builds JSONB structures by pushing JsonbValue elements into a JsonbParseState, handling recursive expansion of complex values and binary data unpacking.

## Definition

```c
JsonbValue *
pushJsonbValue(JsonbParseState **pstate, JsonbIteratorToken seq,
			   JsonbValue *jbval)
```
## Detailed Description
This central function orchestrates the construction of JSONB data structures through a state machine approach. It processes JsonbValues in sequential token order (WJB_BEGIN_OBJECT, WJB_KEY, WJB_VALUE, etc.) and maintains the parsing state across multiple calls.

The function handles several key scenarios:
- **Complex objects**: Recursively expands jbvObject into constituent key-value pairs
- **Complex arrays**: Recursively expands jbvArray into individual elements  
- **Binary data unpacking**: Decodes jbvBinary values using JsonbIterator to extract embedded structures
- **Scalar processing**: Delegates simple values to pushJsonbValueScalar
- **Raw scalars**: Special handling for scalar values wrapped in pseudo-arrays

For binary data (compressed JSONB structures), it initializes an iterator to traverse the embedded content and recursively pushes each encountered token. This allows seamless integration of pre-existing JSONB data into new structures.

The function maintains state through the JsonbParseState pointer, enabling incremental construction of complex nested structures across multiple function calls.

## Parameters / Member Variables  
- `**pstate`: Pointer to JsonbParseState pointer (modified to track construction state)
- `seq`: JsonbIteratorToken indicating the type of token being processed
- `*jbval`: JsonbValue to process (may be NULL for structural tokens like WJB_BEGIN_OBJECT)
## Dependencies
- Functions called/Symbols referenced:
  - [pushJsonbValueScalar](pushJsonbValueScalar.md)
  - [JsonbIteratorInit](../J/JsonbIteratorInit.md)
  - [JsonbIteratorNext](../J/JsonbIteratorNext.md)
  - WJB_BEGIN_OBJECT, WJB_END_OBJECT, WJB_BEGIN_ARRAY, WJB_END_ARRAY, WJB_KEY, WJB_VALUE, WJB_ELEM, WJB_DONE
  - jbvObject, jbvArray, jbvBinary (type enums)
  - JB_FSCALAR (header flag)
- Called from (representative examples):
  - JSON parsing functions (jsonb_in_object_start, jsonb_in_scalar, etc.)
  - JSONB construction functions (jsonb_build_object_worker, jsonb_build_array_worker)
  - JSONB modification functions (setPath, IteratorConcat)
  - [JsonbValueToJsonb](../J/JsonbValueToJsonb.md)
  - Self-recursive calls for object/array expansion

## Notes and Other Information
- **Recursive expansion**: Objects and arrays are automatically expanded into their constituent tokens
- **State management**: Maintains parsing state across calls through pstate double pointer
- **Memory allocation**: Initial JsonbParseState is allocated on first call (starts as NULL)
- **Binary unpacking**: Efficiently handles pre-compressed JSONB data through iterator-based traversal
- **Raw scalar handling**: Special logic for scalar values wrapped in pseudo-array containers
- **Token sequencing**: Expects tokens in proper JSON structural order (begin/end pairs must be balanced)
- **Return value**: Returns the final JsonbValue when structure is complete, NULL for intermediate calls

## Simplified Source

```c
JsonbValue *
pushJsonbValue(JsonbParseState **pstate, JsonbIteratorToken seq, JsonbValue *jbval)
{
    JsonbIterator *it;
    JsonbValue *res = NULL;
    JsonbValue v;
    JsonbIteratorToken tok;
    int i;

    // Handle complex objects - recursively expand into key-value pairs
    if (jbval && (seq == WJB_ELEM || seq == WJB_VALUE) && jbval->type == jbvObject)
    {
        pushJsonbValue(pstate, WJB_BEGIN_OBJECT, NULL);
        for (i = 0; i < jbval->val.object.nPairs; i++)
        {
            pushJsonbValue(pstate, WJB_KEY, &jbval->val.object.pairs[i].key);
            pushJsonbValue(pstate, WJB_VALUE, &jbval->val.object.pairs[i].value);
        }
        return pushJsonbValue(pstate, WJB_END_OBJECT, NULL);
    }

    // Handle complex arrays - recursively expand into elements
    if (jbval && (seq == WJB_ELEM || seq == WJB_VALUE) && jbval->type == jbvArray)
    {
        pushJsonbValue(pstate, WJB_BEGIN_ARRAY, NULL);
        for (i = 0; i < jbval->val.array.nElems; i++)
        {
            pushJsonbValue(pstate, WJB_ELEM, &jbval->val.array.elems[i]);
        }
        return pushJsonbValue(pstate, WJB_END_ARRAY, NULL);
    }

    // Handle simple values and structural tokens
    if (!jbval || (seq != WJB_ELEM && seq != WJB_VALUE) || jbval->type != jbvBinary)
    {
        return pushJsonbValueScalar(pstate, seq, jbval);
    }

    // Handle binary data - unpack and process each component
    it = JsonbIteratorInit(jbval->val.binary.data);

    // Special handling for scalar values wrapped in pseudo-arrays
    if ((jbval->val.binary.data->header & JB_FSCALAR) && *pstate)
    {
        tok = JsonbIteratorNext(&it, &v, true);  // WJB_BEGIN_ARRAY
        tok = JsonbIteratorNext(&it, &v, true);  // WJB_ELEM
        res = pushJsonbValueScalar(pstate, seq, &v);
        tok = JsonbIteratorNext(&it, &v, true);  // WJB_END_ARRAY
        return res;
    }

    // Process all tokens from binary data
    while ((tok = JsonbIteratorNext(&it, &v, false)) != WJB_DONE)
    {
        res = pushJsonbValueScalar(pstate, tok,
                                   tok < WJB_BEGIN_ARRAY ||
                                   (tok == WJB_BEGIN_ARRAY && v.val.array.rawScalar) ? &v : NULL);
    }

    return res;
}
```