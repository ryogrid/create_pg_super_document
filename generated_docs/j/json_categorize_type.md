# json_categorize_type

## Location
[src/backend/utils/adt/jsonfuncs.c:5975-6075](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L5975-L6075)

## Overview
Analyzes a PostgreSQL data type and determines how it should be converted to JSON, returning both the JSON type category and the appropriate output function for the conversion.

## Definition
```c
void json_categorize_type(Oid typoid, bool is_jsonb, JsonTypeCategory *tcategory, Oid *outfuncoid)
```

## Detailed Description
This function serves as the central type analysis mechanism for JSON conversion in PostgreSQL. It takes a PostgreSQL type OID and determines the most appropriate way to represent that type in JSON format. The function handles a comprehensive range of PostgreSQL types, from basic built-in types to complex user-defined types, and provides specialized handling for different categories.

The function first resolves any domain types to their base types, then categorizes them into specific JSON type categories such as boolean, numeric, date/time types, JSON types, arrays, composites, and others. For each category, it determines the appropriate output function to use for the conversion, whether that's a built-in output function, a type-specific function, or a cast function.

The function also handles special logic for JSONB types and can discover explicit cast functions for user-defined types, providing a flexible framework for extending JSON conversion to custom types.

## Parameters / Member Variables
- `typoid`: PostgreSQL type OID of the data type to be categorized
- `is_jsonb`: Boolean flag indicating whether the target JSON format is JSONB (affects JSONB type handling)
- `tcategory`: Output parameter that receives the JsonTypeCategory classification
- `outfuncoid`: Output parameter that receives the OID of the function to use for type conversion

## Dependencies
- Functions called/Symbols referenced:
  - JsonTypeCategory (enum for JSON type classification)
  - [getBaseType](../g/getBaseType.md) (function to resolve domain types to base types)
  - [getTypeOutputInfo](../g/getTypeOutputInfo.md) (function to get type output function information)
  - [get_element_type](../g/get_element_type.md) (function to check if type is an array)
  - [type_is_rowtype](../t/type_is_rowtype.md) (function to check if type is a composite/row type)
  - [find_coercion_pathway](../f/find_coercion_pathway.md) (function to find cast functions)
  - Various JSONTYPE_* constants (JSONTYPE_BOOL, JSONTYPE_NUMERIC, etc.)
  - Various function OIDs (F_BOOLOUT, F_DATE_OUT, etc.)
  - FirstNormalObjectId (constant distinguishing built-in from user types)
  - COERCION_EXPLICIT and COERCION_PATH_FUNC (coercion type constants)
- Called from (representative examples):
  - [ExecInitExprRec](../E/ExecInitExprRec.md) (expression initialization in executor)
  - array_to_json_internal (array to JSON conversion)
  - composite_to_json (composite type to JSON conversion)
  - add_json, add_jsonb (JSON value addition functions)
  - [to_json](../t/to_json.md), to_jsonb (main JSON conversion functions)
  - Various JSON aggregation functions

## Notes and Other Information
- This is a non-static function, part of PostgreSQL's public JSON API
- Handles the complete spectrum of PostgreSQL types for JSON conversion
- Provides special handling for date/time types with custom formatting
- Supports user-defined types through explicit cast function discovery
- The is_jsonb parameter affects how JSONB types are categorized when converting to different JSON formats
- Critical component in PostgreSQL's JSON conversion infrastructure
- Used extensively throughout JSON and JSONB functionality for type-appropriate conversions
- Returns appropriate output functions that can handle the specific type conversion requirements
- The function ensures that all PostgreSQL types can be represented in JSON in some form, whether through direct conversion, casting, or fallback to text representation