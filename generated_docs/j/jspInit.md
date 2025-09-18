# jspInit

## Location
src/backend/utils/adt/jsonpath.c: 973 - 982

## Overview
Initializes a JsonPathItem structure from a JsonPath object, serving as the entry point for JSON path expression processing by reading the root node.

## Definition


## Detailed Description
This function serves as a convenient wrapper around jspInitByBuffer that initializes a JsonPathItem structure from a JsonPath object. It first validates that the JsonPath header contains a supported version (ignoring the LAX flag), then delegates to jspInitByBuffer to perform the actual initialization from the data buffer starting at offset 0. This is typically the first function called when beginning to process a compiled JSON path expression.

## Parameters / Member Variables
- : Pointer to a JsonPathItem structure to be initialized with the root node information
- : Pointer to the JsonPath object containing the compiled JSON path expression data

## Dependencies
- Functions called/Symbols referenced:
  - JsonPath (struct type)
  - JsonPathItem (struct type)
  - JSONPATH_VERSION (version constant)
  - JSONPATH_LAX (flag constant)
  - [jspInitByBuffer](jspInitByBuffer.md) (core initialization function)
  - Assert (debugging macro)
- Called from (representative examples):
  - [extract_jsp_query](../e/extract_jsp_query.md)
  - [jsonPathToCstring](jsonPathToCstring.md)
  - [jspIsMutable](jspIsMutable.md)
  - [executeJsonPath](../e/executeJsonPath.md)
  - jspHasNext

## Notes and Other Information
- Validates JsonPath version compatibility before processing
- Acts as a convenience wrapper that starts processing from the beginning of the data buffer (offset 0)
- The JSONPATH_LAX flag is stripped during version checking but doesn't affect initialization
- Essential entry point for all JSON path expression evaluation and analysis functions
- The Assert macro ensures version compatibility and will terminate execution if an incompatible version is detected