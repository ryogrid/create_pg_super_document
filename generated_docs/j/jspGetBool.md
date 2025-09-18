# jspGetBool

## Location
src/backend/utils/adt/jsonpath.c: 1203 - 1210

## Overview
Extracts and returns the boolean value from a JSON path boolean item.

## Definition


## Detailed Description
The jspGetBool function is a simple accessor function that extracts boolean values from JSON path items. It first validates that the input JsonPathItem is of type jpiBool through an Assert statement. Once validated, it dereferences the data pointer stored in the item's content.value.data field and casts it to a boolean value. This function provides type-safe access to boolean constants embedded in JSON path expressions.

## Parameters / Member Variables
- : Pointer to the JsonPathItem containing the boolean value (must be of type jpiBool)

## Dependencies
- Functions called/Symbols referenced:
  - JsonPathItem (struct type)
  - jpiBool (enumeration constant)
- Called from (representative examples):
  - printJsonPathItem
  - getJsonPathItem

## Notes and Other Information
- Returns the boolean value directly as a C bool type
- The Assert statement ensures type safety and will trigger in debug builds if called with non-boolean items
- Part of a family of type-specific accessor functions for JSON path values
- The function assumes the data pointer is properly aligned and contains valid boolean data
- Used primarily in JSON path expression evaluation and debugging output