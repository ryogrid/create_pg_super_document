# jspGetNext

## Location
src/backend/utils/adt/jsonpath.c: 1092 - 1158

## Overview
Retrieves the next item in a JSON path expression sequence and initializes a JsonPathItem structure with its data.

## Definition


## Detailed Description
The jspGetNext function is a core navigation utility in PostgreSQL's JSON path implementation that allows sequential traversal through linked JSON path items. It first checks if the current JsonPathItem (v) has a next item using jspHasNext(). If a next item exists, the function validates that the current item's type is one of the many supported JSON path item types through an extensive Assert statement covering all valid types from basic values (null, string, numeric, bool) to complex operations (arithmetic, comparison, array access, filters, and built-in functions). When a next item is found and the destination pointer (a) is provided, it initializes the destination JsonPathItem structure using jspInitByBuffer() with the base buffer and the next position offset.

## Parameters / Member Variables
- : Pointer to the current JsonPathItem from which to get the next item
- : Optional pointer to JsonPathItem structure to initialize with the next item (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - jspHasNext
  - jspInitByBuffer
  - JsonPathItem (struct type)
  - Multiple jpi* enumeration constants (jpiNull, jpiString, jpiNumeric, etc.)
- Called from (representative examples):
  - extract_jsp_path_expr_nodes
  - printJsonPathItem
  - executeItemOptUnwrapTarget
  - executeNextItem
  - executeBinaryArithmExpr

## Notes and Other Information
- Returns true if a next item exists and was successfully retrieved, false otherwise
- The extensive Assert statement serves as both validation and documentation of all supported JSON path item types
- The function is designed to work with PostgreSQL's internal JSON path buffer format
- Part of the JSON path execution engine used for JSON querying and manipulation
- The 'a' parameter can be NULL if the caller only wants to check for the existence of a next item without retrieving it