# map_sql_type_to_xmlschema_type

## Location
src/backend/utils/adt/xml.c: 3911 - 4085

## Overview
Maps PostgreSQL data types to complete XML Schema type definitions with restrictions and constraints according to SQL/XML:2008 sections 9.5 and 9.6.

## Definition
```c
static const char *
map_sql_type_to_xmlschema_type(Oid typeoid, int typmod)
```

## Detailed Description
This function generates complete XML Schema type definitions for PostgreSQL data types, creating detailed `<xsd:simpleType>` or `<xsd:complexType>` elements with appropriate restrictions, constraints, and validation patterns. Unlike `map_sql_type_to_xml_name` which returns simple type names, this function generates full XML Schema type definitions including base types, restrictions, and validation rules.

The function handles special cases:
- XML types: Creates complex types with mixed content and flexible element sequences
- Built-in types: Creates simple types with appropriate base restrictions and constraints
- Domain types: Creates restrictions based on the underlying base type
- Numeric types: Includes precision, scale, and range constraints
- String types: Includes length restrictions
- Date/time types: Includes detailed pattern validation using regular expressions

## Parameters / Member Variables
- `typeoid`: The PostgreSQL OID of the data type to map
- `typmod`: Type modifier containing additional type information (size, precision, scale, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - map_sql_type_to_xml_name (for generating type names and domain base types)
  - get_typtype (PostgreSQL function to determine type category)
  - getBaseTypeAndTypmod (PostgreSQL function for resolving domain base types)
  - TYPTYPE_DOMAIN (PostgreSQL constant for domain types)
  - XMLBINARY_BASE64 (XML binary encoding option)
  - Various PostgreSQL constants: INT64_FORMAT, PG_INT64_MAX, PG_INT64_MIN, etc.
- Called from (representative examples):
  - map_sql_typecoll_to_xmlschema_types

## Notes and Other Information
- Creates comprehensive XML Schema definitions with detailed validation:
  - String types: `xsd:maxLength` restrictions for VARCHAR/CHAR with length limits
  - Numeric types: `xsd:totalDigits` and `xsd:fractionDigits` for NUMERIC types
  - Integer types: `xsd:maxInclusive` and `xsd:minInclusive` range constraints
  - Binary types: Base64Binary or hexBinary encoding based on configuration
  - Date/time types: Complex regex patterns for format validation including timezone handling
- XML types receive special treatment with `mixed="true"` complex types allowing arbitrary content
- Domain types are mapped by creating restrictions on their base types
- Uses precise regular expression patterns for temporal types with optional fractional seconds
- The function is static and only used internally within the xml.c module
- Memory management relies on StringInfo for building complex XML Schema strings
- Implements both SQL/XML:2008 section 9.5 (unnamed types) and 9.6 (named types) with name attributes