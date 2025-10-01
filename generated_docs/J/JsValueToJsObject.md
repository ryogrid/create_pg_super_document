# JsValueToJsObject

## Location
[src/backend/utils/adt/jsonfuncs.c:2980-3026](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L2980-L3026)

## Overview
Converts a JsValue structure into a JsObject structure, handling both plain JSON strings and JsonB binary data formats for object population operations.

## Definition
```c
static bool JsValueToJsObject(JsValue *jsv, JsObject *jso, Node *escontext)
```

## Detailed Description
JsValueToJsObject performs the conversion of a generic JSON value representation (JsValue) into a structured object representation (JsObject) that can be used for composite type population. The function handles two distinct input formats:

1. **Plain JSON strings**: Converts text-based JSON into a hash table using get_json_object_as_hash()
2. **JsonB binary data**: Validates and extracts the binary container data for object operations

The function includes comprehensive error handling for invalid input types, specifically rejecting scalar values and arrays when object data is expected. It uses PostgreSQL's soft error handling mechanism to allow callers to handle errors gracefully rather than throwing exceptions.

## Parameters / Member Variables
- `jsv`: Input JsValue pointer containing either JSON string or JsonB binary data
- `jso`: Output JsObject pointer to be populated with the converted object representation
- `escontext`: Error context node for soft error handling, allowing non-exception error reporting

## Dependencies
- Functions called/Symbols referenced:
  - [get_json_object_as_hash](../g/get_json_object_as_hash.md)
  - JsonContainerIsObject
  - IsAJsonbScalar
  - JsonContainerIsScalar
  - errsave
  - SOFT_ERROR_OCCURRED
- Called from (representative examples):
  - [populate_composite](../p/populate_composite.md)
  - JsObjectFree

## Notes and Other Information
- Returns false on error when escontext points to an ErrorSaveContext, allowing callers to handle errors without exceptions
- Validates that JsonB input represents an object container, not scalar or array data
- Uses assertion checking to ensure hash table creation succeeds in non-error cases
- The function name "populate_composite" is hardcoded in error messages, suggesting this function is primarily used in composite type population contexts
- Handles both positive and negative length specifications for JSON strings, with negative values triggering strlen() calculation

## Simplified Source

```c
static bool JsValueToJsObject(JsValue *jsv, JsObject *jso, Node *escontext)
{
    jso->is_json = jsv->is_json;

    if (jsv->is_json)
    {
        // Convert plain-text JSON into hash table
        jso->val.json_hash = get_json_object_as_hash(jsv->val.json.str,
                                                    jsv->val.json.len >= 0
                                                    ? jsv->val.json.len
                                                    : strlen(jsv->val.json.str),
                                                    "populate_composite", escontext);
        Assert(jso->val.json_hash != NULL || SOFT_ERROR_OCCURRED(escontext));
    }
    else
    {
        JsonbValue *jbv = jsv->val.jsonb;

        // Validate that JsonB value is an object container
        if (jbv->type == jbvBinary && JsonContainerIsObject(jbv->val.binary.data))
        {
            jso->val.jsonb_cont = jbv->val.binary.data;
        }
        else
        {
            // Report error for invalid types (scalars or arrays)
            bool is_scalar = IsAJsonbScalar(jbv) ||
                            (jbv->type == jbvBinary && JsonContainerIsScalar(jbv->val.binary.data));

            errsave(escontext,
                   (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    is_scalar ? errmsg("cannot call %s on a scalar", "populate_composite")
                             : errmsg("cannot call %s on an array", "populate_composite")));
        }
    }

    return !SOFT_ERROR_OCCURRED(escontext);
}
```