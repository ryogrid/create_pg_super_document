# push_path

## Location
[src/backend/utils/adt/jsonfuncs.c:1719-1802](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L1719-L1802)

## Overview
Prepares a new structure containing nested empty objects and arrays corresponding to a specified path, and assigns a new value at the end of this path.

## Definition

```c
enum jbvType *tpath = palloc0((path_len - level) * sizeof(enum jbvType));
```
## Detailed Description
The  function creates nested JSON structures (objects and arrays) based on a specified path and places a new value at the end of that path. For example, given a path [a][0][b] with value 1, it produces the structure {a: [{b: 1}]}. 

The function determines whether to create objects or arrays by attempting to parse each path element as an integer. If parsing succeeds, an array is created; otherwise, an object is created. The function handles the creation of intermediate structures, proper nesting, and ensures all opened structures are properly closed except for the outermost level.

## Parameters / Member Variables
- : Pointer to JsonbParseState used for building the JSONB structure
- : Current nesting level in the path hierarchy
- : Array of Datum values representing path elements
- : Array indicating which path elements are NULL
- : Total length of the path array
- : The JsonbValue to be inserted at the end of the path

## Dependencies
- Functions called/Symbols referenced:
  - TextDatumGetCString
  - [strtoint](../s/strtoint.md)
  - [pushJsonbValue](pushJsonbValue.md)
  - [push_null_elements](push_null_elements.md)
  - [palloc0](palloc0.md)
- Types used:
  - [JsonbParseState](../J/JsonbParseState.md)
  - jbvType
  - [JsonbValue](../J/JsonbValue.md)
  - jbvString, jbvObject, jbvArray
  - WJB_BEGIN_OBJECT, WJB_BEGIN_ARRAY, WJB_END_OBJECT, WJB_END_ARRAY
  - WJB_KEY, WJB_VALUE, WJB_ELEM
- Called from:
  - [setPathObject](../s/setPathObject.md)
  - [setPathArray](../s/setPathArray.md)

## Notes and Other Information
- This is a static function within jsonfuncs.c, not exposed externally
- The caller is responsible for ensuring the specified path does not already exist
- The function creates a temporary type path (tpath) to track expected container types at each level
- Array indices are created by pushing NULL elements up to the specified index
- The function leaves the outermost container open for the caller to close
- [Path](../P/Path.md) elements that cannot be parsed as integers are treated as object keys

## Simplified Source

```c
static void push_path(JsonbParseState **st, int level, Datum *path_elems,
                     bool *path_nulls, int path_len, JsonbValue *newval) {
    // Track expected type (object or array) for each level
    enum jbvType *tpath = palloc0((path_len - level) * sizeof(enum jbvType));
    JsonbValue newkey;

    // Create nested structures for remaining path elements
    for (int i = level + 1; i < path_len; i++) {
        char *c, *badp;
        int lindex;

        if (path_nulls[i])
            break;

        // Try to parse path element as integer to determine container type
        c = TextDatumGetCString(path_elems[i]);
        errno = 0;
        lindex = strtoint(c, &badp, 10);

        if (badp == c || *badp != '\0' || errno != 0) {
            // Text path element -> create object
            newkey.type = jbvString;
            newkey.val.string.val = c;
            newkey.val.string.len = strlen(c);

            pushJsonbValue(st, WJB_BEGIN_OBJECT, NULL);
            pushJsonbValue(st, WJB_KEY, &newkey);

            tpath[i - level] = jbvObject;
        } else {
            // Integer path element -> create array
            pushJsonbValue(st, WJB_BEGIN_ARRAY, NULL);

            // Fill array with nulls up to the target index
            push_null_elements(st, lindex);

            tpath[i - level] = jbvArray;
        }
    }

    // Insert the actual value
    if (tpath[(path_len - level) - 1] == jbvArray) {
        pushJsonbValue(st, WJB_ELEM, newval);
    } else {
        pushJsonbValue(st, WJB_VALUE, newval);
    }

    // Close all opened structures except the outermost level
    for (int i = path_len - 1; i > level; i--) {
        if (path_nulls[i])
            break;

        if (tpath[i - level] == jbvObject)
            pushJsonbValue(st, WJB_END_OBJECT, NULL);
        else
            pushJsonbValue(st, WJB_END_ARRAY, NULL);
    }
}
```