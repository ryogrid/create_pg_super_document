# setPathObject

## Location
[src/backend/utils/adt/jsonfuncs.c:5262-5400](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L5262-L5400)

## Overview
setPathObject is a specialized object walker function that handles path-based modifications within JSON objects, supporting key lookup, value replacement, deletion, and creation of new key-value pairs.

## Definition
```c
static void
setPathObject(JsonbIterator **it, Datum *path_elems, bool *path_nulls,
              int path_len, JsonbParseState **st, int level,
              JsonbValue *newval, uint32 npairs, int op_type)
```

## Detailed Description
This static function implements object-specific logic for the setPath operation. It iterates through all key-value pairs in a JSON object, looking for a matching key at the current path level. When a match is found, it either modifies/deletes the value (if at the target level) or recursively continues traversal (if more path elements remain).

Key behaviors include:
- **Key Matching**: Compares each object key with the current path element using exact string comparison
- **Value Operations**: Supports replacement, deletion, or recursive path traversal based on operation flags
- **Key Creation**: Can create new key-value pairs when keys don't exist (controlled by JB_PATH_CREATE_OR_INSERT)
- **Insert Protection**: Prevents overwriting existing keys when called from jsonb_insert
- **Gap Filling**: Creates entire object chains when JB_PATH_FILL_GAPS is set and intermediate path elements are missing
- **Nested Structure Copying**: Preserves unmodified nested objects and arrays through complete traversal

## Parameters / Member Variables
- `it`: Pointer to JsonbIterator for traversing the JSON object
- `path_elems`: Array of Datum values representing remaining path elements  
- `path_nulls`: Boolean array indicating which path elements are null
- `path_len`: Total length of the path array
- `st`: Pointer to JsonbParseState for building the result structure
- `level`: Current recursion level in the path traversal
- `newval`: The new JsonbValue to insert/set at the target location
- `npairs`: Number of key-value pairs in the current object
- `op_type`: Bitmask of operation flags controlling modification behavior

## Dependencies
- Functions called/Symbols referenced:
  - DatumGetTextPP
  - [JsonbIteratorNext](../J/JsonbIteratorNext.md)
  - [pushJsonbValue](../p/pushJsonbValue.md)
  - [setPath](setPath.md) (recursive call)
  - [push_path](../p/push_path.md)
  - WJB_KEY, WJB_VALUE, WJB_BEGIN_ARRAY, WJB_BEGIN_OBJECT, WJB_END_ARRAY, WJB_END_OBJECT
  - JB_PATH_CREATE_OR_INSERT, JB_PATH_INSERT_BEFORE, JB_PATH_INSERT_AFTER, JB_PATH_DELETE, JB_PATH_FILL_GAPS
- Called from (representative examples):
  - [setPath](setPath.md)

## Notes and Other Information
- This is a static function internal to jsonfuncs.c  
- Handles toasted Datum values by detoasting path elements when needed
- Implements special case handling for empty objects when creating new paths
- Uses walking_level counter to properly traverse and copy nested structures
- Prevents key redefinition when insertion operations are requested on existing keys
- Supports gap-filling by creating intermediate object chains when paths don't exist
- The function assumes the caller will close the object with WJB_END_OBJECT

## Simplified Source

```c
static void setPathObject(JsonbIterator **it, Datum *path_elems, bool *path_nulls,
                         int path_len, JsonbParseState **st, int level,
                         JsonbValue *newval, uint32 npairs, int op_type) {
    text *pathelem = NULL;
    bool done = false;

    // Check if we've reached the end of path or have null element
    if (level >= path_len || path_nulls[level])
        done = true;
    else
        pathelem = DatumGetTextPP(path_elems[level]);

    // Special case: empty object with create operation
    if ((npairs == 0) && (op_type & JB_PATH_CREATE_OR_INSERT) &&
        (level == path_len - 1)) {
        JsonbValue newkey;
        newkey.type = jbvString;
        newkey.val.string.val = VARDATA_ANY(pathelem);
        newkey.val.string.len = VARSIZE_ANY_EXHDR(pathelem);

        pushJsonbValue(st, WJB_KEY, &newkey);
        pushJsonbValue(st, WJB_VALUE, newval);
    }

    // Iterate through all key-value pairs
    for (int i = 0; i < npairs; i++) {
        JsonbValue k, v;
        JsonbIteratorToken r = JsonbIteratorNext(it, &k, true);

        // Check if current key matches path element
        if (!done &&
            k.val.string.len == VARSIZE_ANY_EXHDR(pathelem) &&
            memcmp(k.val.string.val, VARDATA_ANY(pathelem), k.val.string.len) == 0) {
            done = true;

            if (level == path_len - 1) {
                // At target level - perform operation
                if (op_type & (JB_PATH_INSERT_BEFORE | JB_PATH_INSERT_AFTER))
                    ereport(ERROR, "cannot replace existing key");

                r = JsonbIteratorNext(it, &v, true); // skip existing value
                if (!(op_type & JB_PATH_DELETE)) {
                    pushJsonbValue(st, WJB_KEY, &k);
                    pushJsonbValue(st, WJB_VALUE, newval);
                }
            } else {
                // More path elements remain - recurse
                pushJsonbValue(st, r, &k);
                setPath(it, path_elems, path_nulls, path_len,
                       st, level + 1, newval, op_type);
            }
        } else {
            // Key doesn't match - copy existing key-value pair
            if ((op_type & JB_PATH_CREATE_OR_INSERT) && !done &&
                level == path_len - 1 && i == npairs - 1) {
                // Create new key at end if needed
                JsonbValue newkey;
                newkey.type = jbvString;
                newkey.val.string.val = VARDATA_ANY(pathelem);
                newkey.val.string.len = VARSIZE_ANY_EXHDR(pathelem);

                pushJsonbValue(st, WJB_KEY, &newkey);
                pushJsonbValue(st, WJB_VALUE, newval);
            }

            // Copy existing key-value pair
            pushJsonbValue(st, r, &k);
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

    // Handle gap filling for missing intermediate paths
    if (!done && (op_type & JB_PATH_FILL_GAPS) && (level < path_len - 1)) {
        JsonbValue newkey;
        newkey.type = jbvString;
        newkey.val.string.val = VARDATA_ANY(pathelem);
        newkey.val.string.len = VARSIZE_ANY_EXHDR(pathelem);

        pushJsonbValue(st, WJB_KEY, &newkey);
        push_path(st, level, path_elems, path_nulls, path_len, newval);
    }
}
```