# get_language_oid

## Location
[src/backend/commands/proclang.c:226-237](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/proclang.c#L226-L237)

## Overview
Retrieves the OID (Object Identifier) for a procedural language by its name, with optional error handling for missing languages.

## Definition

```c
Oid
get_language_oid(const char *langname, bool missing_ok)
```
## Detailed Description
This utility function performs a system catalog lookup to find the OID corresponding to a given procedural language name. It uses the system cache for efficient retrieval and provides flexible error handling based on the missing_ok parameter. The function is commonly used throughout PostgreSQL when language references need to be resolved to their internal object identifiers.

The function leverages the LANGNAME system cache for fast lookups and follows PostgreSQL's standard pattern for object name-to-OID resolution with optional error suppression.

## Parameters / Member Variables
- : C string containing the name of the procedural language to look up
- : Boolean flag controlling error behavior - if false, throws an error when language is not found; if true, returns InvalidOid for missing languages

## Dependencies
- Functions called/Symbols referenced:
  - GetSysCacheOid1
  - [CStringGetDatum](../C/CStringGetDatum.md)
  - OidIsValid
  - ereport (for error reporting)
- Called from (representative examples):
  - [objectNamesToOids](../o/objectNamesToOids.md)
  - [get_object_address](get_object_address.md)
  - [get_object_address_unqualified](get_object_address_unqualified.md)
  - [CreateTransform](../C/CreateTransform.md)
  - [convert_language_name](../c/convert_language_name.md)

## Notes and Other Information
- Uses LANGNAME system cache index for efficient lookups
- Returns InvalidOid for non-existent languages when missing_ok is true
- Throws ERRCODE_UNDEFINED_OBJECT error when language not found and missing_ok is false
- Commonly used in DDL operations and object address resolution
- Part of the standard PostgreSQL pattern for name-to-OID resolution functions
- Function is located in src/backend/commands/proclang.c:226-237