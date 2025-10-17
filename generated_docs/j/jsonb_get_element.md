# jsonb_get_element

## Location
[src/backend/utils/adt/jsonfuncs.c:1529-1676](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L1529-L1676)

## Overview
A core function that extracts elements from JSONB data structures using a path array, supporting both object key lookup and array indexing with optional text conversion.

## Definition

```c
Datum
jsonb_get_element(Jsonb *jb, Datum *path, int npath, bool *isnull, bool as_text)
```
## Detailed Description
The  function implements the fundamental JSONB path traversal logic in PostgreSQL. It navigates through nested JSONB structures (objects, arrays, and scalars) using a sequence of path elements. The function handles object key lookups using string keys, array indexing with both positive and negative indices, and supports extraction from scalar values. It provides comprehensive error handling and null-safety checks, returning appropriate null values when paths don't exist or are invalid.

## Parameters / Member Variables
- : Input JSONB value to extract from
- : Array of Datum values representing the path elements (keys for objects, indices for arrays)
- : Number of elements in the path array
- : Output parameter set to true if the result should be NULL
- : Boolean flag determining output format
  - : Convert result to text representation
  - : Return result as JSONB

## Dependencies
- Functions called/Symbols referenced:
  - JB_ROOT_IS_OBJECT, JB_ROOT_IS_ARRAY, JB_ROOT_IS_SCALAR (JSONB type checking)
  - [getIthJsonbValueFromContainer](../g/getIthJsonbValueFromContainer.md) (array element access)
  - [getKeyJsonValueFromContainer](../g/getKeyJsonValueFromContainer.md) (object key access)
  - [JsonbToCString](../J/JsonbToCString.md), cstring_to_text (text conversion)
  - [JsonbValueAsText](../J/JsonbValueAsText.md) (value-to-text conversion)
  - [JsonbValueToJsonb](../J/JsonbValueToJsonb.md) (value-to-JSONB conversion)
  - JsonContainerIsArray, JsonContainerIsObject (container type checking)
  - [strtoint](../s/strtoint.md) (string-to-integer conversion)
- Called from (representative examples):
  - [get_jsonb_path_all](../g/get_jsonb_path_all.md)
  - [jsonb_subscript_fetch](jsonb_subscript_fetch.md)
  - [jsonb_subscript_fetch_old](jsonb_subscript_fetch_old.md)

## Notes and Other Information
- Located in src/backend/utils/adt/jsonfuncs.c:1529-1676
- Handles negative array indices by converting them to positive indices from the end
- Supports extraction from scalar values when path length is 0 (returns the scalar itself)
- Implements comprehensive error handling for invalid array indices and non-existent object keys
- Returns NULL for attempts to extract from scalars with non-empty paths
- Core building block for JSONB path-based operations in PostgreSQL
- Efficiently handles nested container traversal through iterative processing

## Simplified Source

```c
Datum jsonb_get_element(Jsonb *jb, Datum *path, int npath, bool *isnull, bool as_text) {
    JsonbContainer *container = &jb->root;
    JsonbValue *jbvp = NULL;
    int i;
    bool have_object = false, have_array = false;

    *isnull = false;

    // Determine root container type
    if (JB_ROOT_IS_OBJECT(jb))
        have_object = true;
    else if (JB_ROOT_IS_ARRAY(jb) && !JB_ROOT_IS_SCALAR(jb))
        have_array = true;
    else {
        // Root is scalar - extract it if path is empty
        if (npath <= 0)
            jbvp = getIthJsonbValueFromContainer(container, 0);
    }

    // Handle empty path - return entire object/array
    if (npath <= 0 && jbvp == NULL) {
        if (as_text)
            return PointerGetDatum(cstring_to_text(JsonbToCString(NULL, container, VARSIZE(jb))));
        else
            PG_RETURN_JSONB_P(jb);
    }

    // Traverse path elements
    for (i = 0; i < npath; i++) {
        if (have_object) {
            // Object key lookup
            text *subscr = DatumGetTextPP(path[i]);
            jbvp = getKeyJsonValueFromContainer(container,
                                              VARDATA_ANY(subscr),
                                              VARSIZE_ANY_EXHDR(subscr),
                                              NULL);
        }
        else if (have_array) {
            // Array index lookup
            char *indextext = TextDatumGetCString(path[i]);
            int lindex = strtoint(indextext, NULL, 10);

            uint32 index;
            if (lindex >= 0) {
                index = (uint32) lindex;
            } else {
                // Handle negative indices (from end)
                uint32 nelements = JsonContainerSize(container);
                if (-lindex > nelements) {
                    *isnull = true;
                    return PointerGetDatum(NULL);
                }
                index = nelements + lindex;
            }

            jbvp = getIthJsonbValueFromContainer(container, index);
        }
        else {
            // Cannot extract from scalar
            *isnull = true;
            return PointerGetDatum(NULL);
        }

        if (jbvp == NULL) {
            *isnull = true;
            return PointerGetDatum(NULL);
        }

        // If not at end of path, prepare for next iteration
        if (i < npath - 1) {
            if (jbvp->type == jbvBinary) {
                container = jbvp->val.binary.data;
                have_object = JsonContainerIsObject(container);
                have_array = JsonContainerIsArray(container);
            } else {
                // Cannot traverse further into scalar
                have_object = have_array = false;
            }
        }
    }

    // Return final result
    if (as_text) {
        if (jbvp->type == jbvNull) {
            *isnull = true;
            return PointerGetDatum(NULL);
        }
        return PointerGetDatum(JsonbValueAsText(jbvp));
    } else {
        Jsonb *res = JsonbValueToJsonb(jbvp);
        PG_RETURN_JSONB_P(res);
    }
}
```