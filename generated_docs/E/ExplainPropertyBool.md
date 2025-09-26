# ExplainPropertyBool

## Location
[src/backend/commands/explain.c:4852-4866](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/explain.c#L4852-L4866)

## Overview
Explains a boolean-valued property in PostgreSQL EXPLAIN output by converting a boolean value to its string representation and passing it to the generic property explanation function.

## Definition
void ExplainPropertyBool(const char *qlabel, bool value, ExplainState *es)

## Detailed Description
This function serves as a specialized wrapper around ExplainProperty for handling boolean values. It provides a simple and consistent way to display boolean properties in explain output by converting the C boolean value to the appropriate string representation ("true" or "false"). The function directly calls ExplainProperty with NULL for the unit parameter since boolean values typically don't have associated units. This ensures boolean values are consistently formatted across all explain output formats.

## Parameters / Member Variables
- `qlabel`: The label/name of the boolean property to be displayed in the output
- `value`: The boolean value to be explained/displayed (true or false)
- `es`: Pointer to ExplainState structure containing output format and context information

## Dependencies
- Functions called/Symbols referenced:
  - [ExplainProperty](ExplainProperty.md) (generic property explanation function)
- Called from (representative examples):
  - [ExplainPrintJIT](ExplainPrintJIT.md) (for JIT compilation flags like expressions, deforming, etc.)
  - [ExplainNode](ExplainNode.md) (for boolean flags like async_capable, ordered)

## Notes and Other Information
- Simplest of the type-specific property explanation functions
- Always passes NULL as the unit parameter since boolean values don't have units
- Uses string literals "true" and "false" for consistent boolean representation
- The `true` parameter passed to ExplainProperty indicates this is a definite/literal property value
- Less frequently used compared to numeric property functions, but essential for feature flags and boolean settings
- Ensures consistent boolean formatting across TEXT, XML, JSON, and YAML output formats