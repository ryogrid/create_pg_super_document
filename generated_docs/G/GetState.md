# GetState

## Location
src/backend/utils/adt/jsonfuncs.c: 85 - 98

## Overview
GetState is a structure that maintains state information for the json_get* family of functions, which extract specific values from JSON documents using path-based navigation.

## Definition
```c
typedef struct GetState
{
    JsonLexContext *lex;
    text       *tresult;
    const char *result_start;
    bool        normalize_results;
    bool        next_scalar;
    int         npath;              /* length of each path-related array */
    char      **path_names;         /* field name(s) being sought */
    int        *path_indexes;       /* array index(es) being sought */
    bool       *pathok;             /* is path matched to current depth? */
    int        *array_cur_index;    /* current element index at each path
                                     * level */
} GetState;
```

## Detailed Description
The GetState structure provides comprehensive state management for JSON path-based extraction operations. It supports both field name-based navigation (for objects) and index-based navigation (for arrays), enabling complex path queries through nested JSON structures. The structure maintains arrays that correspond to each level of the path, tracking whether the path matches at each depth and the current position within arrays. This design allows for efficient traversal and extraction of deeply nested JSON values.

## Parameters / Member Variables
- `lex`: Pointer to JsonLexContext structure that provides the lexical parsing context for JSON processing
- `tresult`: Pointer to text structure that will contain the extracted result value
- `result_start`: Pointer to the start of the result string in memory for efficient text handling
- `normalize_results`: Boolean flag indicating whether the extracted results should be normalized
- `next_scalar`: Boolean flag indicating whether the next scalar value encountered should be captured
- `npath`: Integer specifying the length/depth of the path being searched
- `path_names`: Array of string pointers containing the field names to match at each path level
- `path_indexes`: Array of integers containing the array indices to match at each path level
- `pathok`: Array of boolean values indicating whether the path has been successfully matched up to each respective depth level
- `array_cur_index`: Array of integers tracking the current element index being processed at each path level for arrays

## Dependencies
- Functions called/Symbols referenced:
  - [JsonLexContext](../J/JsonLexContext.md)
  - [text](../t/text.md)
- Called from (representative examples):
  - [get_worker](../g/get_worker.md)
  - [get_object_start](../g/get_object_start.md)
  - [get_object_end](../g/get_object_end.md)
  - [get_object_field_start](../g/get_object_field_start.md)
  - [get_object_field_end](../g/get_object_field_end.md)
  - [get_array_start](../g/get_array_start.md)
  - [get_array_end](../g/get_array_end.md)
  - [get_array_element_start](../g/get_array_element_start.md)
  - [get_array_element_end](../g/get_array_element_end.md)
  - [get_scalar](../g/get_scalar.md)

## Notes and Other Information
This structure is central to PostgreSQL JSON path extraction functionality and supports complex nested path queries. The parallel arrays (path_names, path_indexes, pathok, array_cur_index) work together to maintain state at each level of the JSON hierarchy being traversed. The structure handles both object field access and array indexing, making it suitable for a wide variety of JSON query patterns. The normalize_results flag provides flexibility in how extracted values are formatted for return to the caller.