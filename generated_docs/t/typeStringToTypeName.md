# typeStringToTypeName

## Location
src/backend/parser/parse_type.c: 738 - 784

## Overview
Parses a SQL-compatible type declaration string and returns a TypeName node representing the parsed type information.

## Definition
```c
TypeName *typeStringToTypeName(const char *str, Node *escontext)
```

## Detailed Description
This function takes a string representation of a SQL type (such as "int4", "integer", or "character varying(32)") and parses it using PostgreSQL's raw parser to create a TypeName node. The function handles the complete parsing process including error handling and validation.

The function sets up an error context callback to provide meaningful error messages if parsing fails. It uses the raw parser with RAW_PARSE_TYPE_NAME mode to parse the type string according to SQL grammar rules.

Key validation steps include:
- Checking for empty or whitespace-only input
- Ensuring exactly one TypeName node is returned from parsing
- Rejecting SETOF types (not allowed in this context)

The function supports soft error handling through the escontext parameter, allowing callers to handle errors gracefully rather than having them thrown immediately.

## Parameters / Member Variables
- `str`: The string containing the SQL type declaration to parse
- `escontext`: Error context node for soft error handling; if NULL, errors are thrown normally

## Dependencies
- Functions called/Symbols referenced:
  - [pts_error_callback](../p/pts_error_callback.md)
  - [raw_parser](../r/raw_parser.md)
  - RAW_PARSE_TYPE_NAME
  - linitial_node
  - unconstify
  - ereturn
  - strspn
  - strlen
- Called from (representative examples):
  - [pg_get_object_address](../p/pg_get_object_address.md) (src/backend/catalog/objectaddress.c:2147, 2203)
  - [parseTypeString](../p/parseTypeString.md) (src/backend/parser/parse_type.c:791)

## Notes and Other Information
- Returns NULL on parse failure when escontext is provided for soft error handling
- Throws ERROR on parse failure when escontext is NULL
- Rejects SETOF type constructs even if they parse successfully
- Uses PostgreSQL's standard error context callback mechanism for better error reporting
- The ErrorSaveContext option is noted as "mostly aspirational" - some grammar errors may still be thrown
- Input string is checked for being empty or containing only whitespace characters