# push_path

## Location
src/backend/utils/adt/jsonfuncs.c: 1719 - 1802

## Overview
Prepares a new structure containing nested empty objects and arrays corresponding to a specified path, and assigns a new value at the end of this path.

## Definition


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
  - strtoint
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