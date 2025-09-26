# map_sql_type_to_xml_name

## Location
src/backend/utils/adt/xml.c: 3751 - 3855

## Overview
Maps PostgreSQL data types to XML names according to SQL/XML:2008 section 9.4 specification, creating standardized XML type names for database types.

## Definition
```c
static const char *
map_sql_type_to_xml_name(Oid typeoid, int typmod)
```

## Detailed Description
This function converts PostgreSQL data type OIDs to corresponding XML type names following the SQL/XML standard. It handles built-in PostgreSQL types with specific mapping rules and generates appropriate names for user-defined types and domains. The function takes into account type modifiers to create precise type names that include size, precision, and scale information where applicable.

For built-in types, it uses predefined XML type names. For user-defined types and domains, it constructs names using the multipart SQL identifier mapping function, incorporating database name, schema name, and type name.

## Parameters / Member Variables
- `typeoid`: The PostgreSQL OID of the data type to map
- `typmod`: Type modifier containing additional type information (size, precision, scale, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - map_multipart_sql_identifier_to_xml_name
  - get_database_name
  - get_namespace_name
  - SearchSysCache1 (PostgreSQL system cache lookup)
  - Form_pg_type (PostgreSQL type system structure)
  - TYPTYPE_DOMAIN (PostgreSQL type system constant)
- Called from (representative examples):
  - map_sql_table_to_xmlschema
  - map_sql_type_to_xmlschema_type

## Notes and Other Information
- Handles the following built-in PostgreSQL types with specific XML mappings:
  - Character types: BPCHAR → CHAR, VARCHAR → VARCHAR (with length modifiers)
  - Numeric types: NUMERIC (with precision/scale), INTEGER, SMALLINT, BIGINT, REAL, DOUBLE
  - Boolean type: BOOLEAN
  - Date/time types: TIME, TIME_WTZ, TIMESTAMP, TIMESTAMP_WTZ, DATE (with precision modifiers)
  - XML type: XML
- For NUMERIC types, extracts precision and scale from typmod using bit manipulation
- For character and time types, adjusts typmod by subtracting VARHDRSZ (variable header size)
- User-defined types and domains are mapped using hierarchical naming with database/schema qualifiers
- The function is static and only used internally within the xml.c module
- Memory management relies on StringInfo for building result strings
- Performs system catalog lookups for unknown types using the type cache