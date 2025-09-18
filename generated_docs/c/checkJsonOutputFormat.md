# checkJsonOutputFormat

## Location
[src/backend/parser/parse_expr.c:3450-3499](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_expr.c#L3450-L3499)

## Overview
Validates the compatibility of a specified JSON output format with the target output type, ensuring that format specifications are appropriate for the destination data type.

## Definition


## Detailed Description
This function performs validation checks on JSON format specifications to ensure they are compatible with the target output type. It enforces PostgreSQL's type system constraints for JSON formatting operations. The function validates two main aspects:

1. **Type Compatibility**: Ensures that non-default JSON formats are only applied to appropriate types (BYTEA, JSON, JSONB, or string category types) unless explicitly allowed
2. **Encoding Validation**: For JSON format types, validates that encoding specifications are only used with BYTEA types and that only UTF-8 encoding is supported

The function will raise errors with appropriate error codes and position information when validation fails.

## Parameters / Member Variables
- : ParseState pointer containing parser context and location information for error reporting
- : JsonFormat pointer specifying the JSON format configuration to validate
- : OID of the target output type that the format will be applied to
- : Boolean flag that allows format application to non-string types when true

## Dependencies
- Functions called/Symbols referenced:
  - [get_type_category_preferred](../g/get_type_category_preferred.md)
  - ereport (for error reporting)
  - [parser_errposition](../p/parser_errposition.md)
- Called from (representative examples):
  - [transformJsonOutput](../t/transformJsonOutput.md)

## Notes and Other Information
- This is a static function used internally within the parser for JSON expression validation
- Supports PostgreSQL's type safety by preventing incompatible format/type combinations
- Only UTF-8 encoding is currently supported for JSON format types
- Error messages include parser position information to help users identify problematic syntax
- The function distinguishes between BYTEA, JSON/JSONB, and string category types for format applicability