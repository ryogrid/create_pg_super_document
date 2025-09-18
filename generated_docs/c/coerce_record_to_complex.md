# coerce_record_to_complex

## Location
src/backend/parser/parse_coerce.c: 1012 - 1160

## Overview
This function coerces a RECORD type to a specific composite type, handling both RowExpr and whole-row variable inputs.

## Definition


## Detailed Description
The coerce_record_to_complex function converts a generic RECORD type to a specific composite type (struct). It supports two main input types: RowExpr nodes that explicitly contain field expressions, and whole-row Var nodes that represent entire table rows.

The function performs detailed type checking and coercion for each field:
1. Extracts individual field expressions from the input node
2. Looks up the target composite type's tuple descriptor
3. Iterates through each field in the target type
4. Handles dropped columns by inserting NULL constants
5. Recursively coerces each input field to match the corresponding target field type
6. Validates that input and target have matching numbers of fields
7. Constructs a new RowExpr with the coerced fields

If the target type is a domain over a composite type, it applies domain constraints after building the base composite value.

## Parameters / Member Variables
- : Parse state for error reporting and namespace resolution
- : Input node to coerce (must be RowExpr or whole-row Var)
- : OID of the target composite type
- : Coercion context (implicit, assignment, or explicit)
- : Coercion format controlling display behavior
- : Source location for error reporting

## Dependencies
- Functions called/Symbols referenced:
  - IsA (node type checking)
  - [GetNSItemByRangeTablePosn](../G/GetNSItemByRangeTablePosn.md), expandNSItemVars (namespace handling)
  - [getBaseTypeAndTypmod](../g/getBaseTypeAndTypmod.md), lookup_rowtype_tupdesc (type system)
  - [coerce_to_target_type](coerce_to_target_type.md) (recursive field coercion)
  - [coerce_to_domain](coerce_to_domain.md) (domain constraint application)
  - makeNode, makeNullConst (node construction)
  - list_head, lnext, lappend (list operations)
  - [format_type_be](../f/format_type_be.md), parser_coercion_errposition (error reporting)
- Called from:
  - [coerce_type](coerce_type.md)

## Notes and Other Information
- This is a static function, only accessible within parse_coerce.c
- Only supports RowExpr and whole-row Var inputs; other node types cause errors
- Handles dropped columns in composite types by inserting NULL placeholders
- Provides detailed error messages for field count mismatches and type conversion failures
- Supports domain types over composite types through recursive coerce_to_domain call
- Uses COERCE_IMPLICIT_CAST for individual field coercions to maintain consistent formatting
- Validates that input records have exactly the right number of fields for the target type