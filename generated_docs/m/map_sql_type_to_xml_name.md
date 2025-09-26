# map_sql_type_to_xml_name

## Location
[src/backend/utils/adt/xml.c:3751-3855](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L3751-L3855)

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
  - [map_multipart_sql_identifier_to_xml_name](map_multipart_sql_identifier_to_xml_name.md)
  - [get_database_name](../g/get_database_name.md)
  - [get_namespace_name](../g/get_namespace_name.md)
  - [SearchSysCache1](../S/SearchSysCache1.md) (PostgreSQL system cache lookup)
  - Form_pg_type (PostgreSQL type system structure)
  - TYPTYPE_DOMAIN (PostgreSQL type system constant)
- Called from (representative examples):
  - [map_sql_table_to_xmlschema](map_sql_table_to_xmlschema.md)
  - [map_sql_type_to_xmlschema_type](map_sql_type_to_xmlschema_type.md)

## Notes and Other Information
- Handles the following built-in PostgreSQL types with specific XML mappings:
  - Character types: BPCHAR → CHAR, VARCHAR → VARCHAR (with length modifiers)
  - [Numeric](../N/Numeric.md) types: NUMERIC (with precision/scale), INTEGER, SMALLINT, BIGINT, REAL, DOUBLE
  - [Boolean](../B/Boolean.md) type: BOOLEAN
  - Date/time types: TIME, TIME_WTZ, TIMESTAMP, TIMESTAMP_WTZ, DATE (with precision modifiers)
  - XML type: XML
- For NUMERIC types, extracts precision and scale from typmod using bit manipulation
- For character and time types, adjusts typmod by subtracting VARHDRSZ (variable header size)
- User-defined types and domains are mapped using hierarchical naming with database/schema qualifiers
- The function is static and only used internally within the xml.c module
- Memory management relies on StringInfo for building result strings
- Performs system catalog lookups for unknown types using the type cache