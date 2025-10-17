# setPathArray

## Location
[src/backend/utils/adt/jsonfuncs.c:5401-5571](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L5401-L5571)

## Overview
setPathArray is a specialized array walker function that handles path-based modifications within JSON arrays, supporting index-based access, element insertion, replacement, deletion, and array expansion with gap filling.

## Definition
```c
static void
setPathArray(JsonbIterator **it, Datum *path_elems, bool *path_nulls,
             int path_len, JsonbParseState **st, int level,
             JsonbValue *newval, uint32 nelems, int op_type)
```

## Detailed Description
This static function implements array-specific logic for the setPath operation. It parses array indices from path elements, handles both positive and negative indexing, and supports various array modification operations. The function provides sophisticated index handling including:

- **Index Parsing**: Converts path elements to integers with comprehensive error checking
- **Negative Indexing**: Supports Python-style negative indices counting from array end
- **Boundary Management**: Handles out-of-bounds access based on operation flags
- **Gap Filling**: Can extend arrays beyond current bounds and fill gaps with nulls
- **Position Consistency**: Prevents index shifting when required by operation flags
- **Element Operations**: Supports insertion before/after, replacement, and deletion
- **Nested Structure Preservation**: Properly copies unmodified nested objects and arrays

The function includes special handling for edge cases like empty arrays, prepending operations, and maintaining element positions during modifications.

## Parameters / Member Variables
- `it`: Pointer to JsonbIterator for traversing the JSON array
- `path_elems`: Array of Datum values representing remaining path elements
- `path_nulls`: Boolean array indicating which path elements are null
- `path_len`: Total length of the path array
- `st`: Pointer to JsonbParseState for building the result structure
- `level`: Current recursion level in the path traversal
- `newval`: The new JsonbValue to insert/set at the target location
- `nelems`: Number of elements in the current array
- `op_type`: Bitmask of operation flags controlling modification behavior

## Dependencies
- Functions called/Symbols referenced:
  - TextDatumGetCString
  - [strtoint](strtoint.md)
  - [JsonbIteratorNext](../J/JsonbIteratorNext.md)
  - [pushJsonbValue](../p/pushJsonbValue.md)
  - [setPath](setPath.md) (recursive call)
  - [push_null_elements](../p/push_null_elements.md)
  - [push_path](../p/push_path.md)
  - WJB_ELEM, WJB_BEGIN_ARRAY, WJB_BEGIN_OBJECT, WJB_END_ARRAY, WJB_END_OBJECT
  - JB_PATH_CONSISTENT_POSITION, JB_PATH_FILL_GAPS, JB_PATH_CREATE_OR_INSERT, JB_PATH_INSERT_BEFORE, JB_PATH_INSERT_AFTER, JB_PATH_CREATE, JB_PATH_REPLACE
- Called from (representative examples):
  - [setPath](setPath.md)

## Notes and Other Information
- This is a static function internal to jsonfuncs.c
- Implements comprehensive index validation with detailed error messages for invalid integers
- Uses INT_MIN as a special marker for prepending operations when negative indices exceed array bounds
- Supports both zero-based positive indexing and negative indexing from array end
- Can fill intermediate array positions with null values when gap filling is enabled
- Prevents index shifting during insertions when consistency is required
- Handles nested structure traversal using walking_level counter for proper copying
- The function assumes the caller will close the array with WJB_END_ARRAY

## Simplified Source

```c
static void setPathArray(JsonbIterator **it, Datum *path_elems, bool *path_nulls,
                        int path_len, JsonbParseState **st, int level,
                        JsonbValue *newval, uint32 nelems, int op_type) {
    JsonbValue v;
    int idx, i;
    bool done = false;

    // Parse index from path element
    if (level < path_len && !path_nulls[level]) {
        char *c = TextDatumGetCString(path_elems[level]);
        char *badp;
        errno = 0;
        idx = strtoint(c, &badp, 10);
        if (badp == c || *badp != '\0' || errno != 0)
            ereport(ERROR, "path element at position %d is not an integer: \"%s\"",
                   level + 1, c);
    } else {
        idx = nelems;
    }

    // Handle negative indices
    if (idx < 0) {
        if (-idx > nelems) {
            if (op_type & JB_PATH_CONSISTENT_POSITION)
                ereport(ERROR, "path element at position %d is out of range: %d",
                       level + 1, idx);
            else
                idx = INT_MIN; // Special marker for prepending
        } else {
            idx = nelems + idx; // Convert to positive index
        }
    }

    // Limit index based on gap filling policy
    if (!(op_type & JB_PATH_FILL_GAPS)) {
        if (idx > 0 && idx > nelems)
            idx = nelems;
    }

    // Handle prepending and empty array creation
    if ((idx == INT_MIN || nelems == 0) && (level == path_len - 1) &&
        (op_type & JB_PATH_CREATE_OR_INSERT)) {
        if (op_type & JB_PATH_FILL_GAPS && nelems == 0 && idx > 0)
            push_null_elements(st, idx);

        pushJsonbValue(st, WJB_ELEM, newval);
        done = true;
    }

    // Iterate through array elements
    for (i = 0; i < nelems; i++) {
        JsonbIteratorToken r;

        if (i == idx && level < path_len) {
            done = true;

            if (level == path_len - 1) {
                // At target level - perform element operation
                r = JsonbIteratorNext(it, &v, true); // skip existing element

                if (op_type & (JB_PATH_INSERT_BEFORE | JB_PATH_CREATE))
                    pushJsonbValue(st, WJB_ELEM, newval);

                // Keep existing element for insert operations
                if (op_type & (JB_PATH_INSERT_AFTER | JB_PATH_INSERT_BEFORE))
                    pushJsonbValue(st, r, &v);

                if (op_type & (JB_PATH_INSERT_AFTER | JB_PATH_REPLACE))
                    pushJsonbValue(st, WJB_ELEM, newval);
            } else {
                // More path elements remain - recurse
                setPath(it, path_elems, path_nulls, path_len,
                       st, level + 1, newval, op_type);
            }
        } else {
            // Copy existing element
            r = JsonbIteratorNext(it, &v, false);
            pushJsonbValue(st, r, r < WJB_BEGIN_ARRAY ? &v : NULL);

            // Handle nested structures
            if (r == WJB_BEGIN_ARRAY || r == WJB_BEGIN_OBJECT) {
                int walking_level = 1;
                while (walking_level != 0) {
                    r = JsonbIteratorNext(it, &v, false);
                    if (r == WJB_BEGIN_ARRAY || r == WJB_BEGIN_OBJECT)
                        ++walking_level;
                    if (r == WJB_END_ARRAY || r == WJB_END_OBJECT)
                        --walking_level;
                    pushJsonbValue(st, r, r < WJB_BEGIN_ARRAY ? &v : NULL);
                }
            }
        }
    }

    // Handle appending to array end
    if ((op_type & JB_PATH_CREATE_OR_INSERT) && !done && level == path_len - 1) {
        if (op_type & JB_PATH_FILL_GAPS && idx > nelems)
            push_null_elements(st, idx - nelems);

        pushJsonbValue(st, WJB_ELEM, newval);
        done = true;
    }

    // Handle gap filling for missing intermediate paths
    if (!done && (op_type & JB_PATH_FILL_GAPS) && (level < path_len - 1)) {
        if (idx > 0)
            push_null_elements(st, idx - nelems);

        push_path(st, level, path_elems, path_nulls, path_len, newval);
    }
}
```