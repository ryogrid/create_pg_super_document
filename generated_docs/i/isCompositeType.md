# isCompositeType

## Location
src/backend/parser/parse_jsontable.c: 377 - 398

## Overview
Determines whether a given type is considered "composite" for the purpose of choosing between JSON_VALUE() or JSON_QUERY() functions in JsonTable column processing.

## Definition


## Detailed Description
This static function examines a PostgreSQL type OID to determine if it should be treated as a composite type in the context of JSON table column processing. The function uses a recursive approach to handle domain types by checking their base types. A type is considered composite if it's one of the special JSON types, an array type, a composite type, or a domain over any of these types.

The determination affects whether PostgreSQL will use JSON_VALUE() (for scalar types) or JSON_QUERY() (for composite types) when processing JSON table columns, which is crucial for proper JSON data extraction and formatting.

## Parameters / Member Variables
- : The OID of the PostgreSQL type to be examined

## Dependencies
- Functions called/Symbols referenced:
  - get_typtype: Gets the type category of a given type OID
  - type_is_array: Checks if the type is an array type
  - getBaseType: Gets the base type of a domain type
  - TYPTYPE_COMPOSITE: Constant for composite type category
  - TYPTYPE_DOMAIN: Constant for domain type category
  - JSONOID: OID constant for json type
  - JSONBOID: OID constant for jsonb type
  - RECORDOID: OID constant for record type

- Called from (representative examples):
  - transformJsonTableColumns: Uses this function to determine JSON function choice
  - AlterTypeNamespaceInternal: Called during type namespace alteration operations

## Notes and Other Information
- The function is recursive when handling domain types, calling itself on the base type
- Special handling for JSON/JSONB types ensures proper JSON processing semantics
- The composite classification affects SQL/JSON standard compliance in JSON table operations
- Domain types inherit the composite classification from their base types