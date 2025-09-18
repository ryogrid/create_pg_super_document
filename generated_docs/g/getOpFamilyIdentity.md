# getOpFamilyIdentity

## Location
src/backend/catalog/objectaddress.c: 5965 - 6008

## Overview
A static helper function that generates a formatted identity string for PostgreSQL operator families, including the family name, schema qualification, and associated access method.

## Definition
```c
static void getOpFamilyIdentity(StringInfo buffer, Oid opfid, List **object, bool missing_ok)
```

## Detailed Description
This function constructs a human-readable identity string for operator families in the format "schema.family_name USING access_method". It performs lookups in both the pg_opfamily and pg_am system catalogs to retrieve the complete information needed for proper identification. The function handles schema qualification automatically and can optionally provide decomposed object components for programmatic use.

The function is designed to work with PostgreSQL's operator family system, which groups related operators and support functions for index access methods. It ensures proper identification by including both the operator family name with schema qualification and the access method that the family supports.

## Parameters / Member Variables
- `buffer`: StringInfo buffer where the operator family identity string will be appended
- `opfid`: Object ID (OID) of the operator family to identify
- `object`: Optional output parameter for a list containing the access method name, schema name, and operator family name (can be NULL)
- `missing_ok`: Boolean flag indicating whether to handle missing operator families gracefully (true) or raise an error (false)

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md) (system cache lookup for both operator family and access method)
  - HeapTupleIsValid (tuple validation)
  - elog (error logging)
  - GETSTRUCT (tuple data extraction)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md) (OID conversion)
  - [get_namespace_name_or_temp](get_namespace_name_or_temp.md) (namespace name lookup)
  - appendStringInfo (formatted string buffer operations)
  - quote_qualified_identifier (schema-qualified identifier quoting)
  - NameStr (name extraction from catalog forms)
  - list_make3 (three-element list creation)
  - [pstrdup](../p/pstrdup.md) (string duplication)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (cache cleanup)
  - Form_pg_opfamily (operator family catalog structure)
  - Form_pg_am (access method catalog structure)

- Called from (representative examples):
  - [getObjectIdentityParts](getObjectIdentityParts.md) (within operator family case handling)
  - [getObjectIdentityParts](getObjectIdentityParts.md) (within access method operator case handling)
  - [getObjectIdentityParts](getObjectIdentityParts.md) (within access method procedure case handling)
  - object_type_map (object type mapping structure)

## Notes and Other Information
- This is a static function, accessible only within the objectaddress.c compilation unit
- The function performs two system cache lookups: one for the operator family and one for its associated access method
- Output format follows the pattern "schema.family_name USING access_method_name"
- When the object parameter is provided, it returns a three-element list: access method name, schema name, and operator family name
- Proper cache management is implemented with ReleaseSysCache calls to prevent memory leaks
- The function handles missing operator families gracefully when missing_ok is true
- Access method lookup failure always raises an error since the access method should exist if the operator family exists
- Part of PostgreSQL's operator family and access method infrastructure for index support