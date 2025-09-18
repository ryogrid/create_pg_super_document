# IsToastClass

## Location
[src/backend/catalog/catalog.c:195-211](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/catalog.c#L195-L211)

## Overview
Determines whether a given pg_class tuple represents a TOAST support relation by checking if it belongs to a pg_toast namespace.

## Definition
```c
bool IsToastClass(Form_pg_class reltuple)
```

## Detailed Description
IsToastClass is a variant of IsToastRelation that operates directly on a pg_class tuple (Form_pg_class) rather than an open Relation structure. This function is used when the caller wants to avoid opening the relation and instead works directly with pg_class catalog data.

The function extracts the relnamespace field from the pg_class tuple and passes it to IsToastNamespace to determine if the relation belongs to a TOAST namespace. This approach is more efficient when scanning pg_class directly, as it avoids the overhead of opening relations.

## Parameters
- `reltuple`: A pointer to a pg_class tuple (Form_pg_class) containing the relation's catalog information

## Dependencies
- Functions called/Symbols referenced:
  - [IsToastNamespace](IsToastNamespace.md) (checks if a namespace OID is a TOAST namespace)
  - Form_pg_class (typedef for FormData_pg_class pointer, representing a pg_class tuple)
- Called from:
  - [IsSystemClass](IsSystemClass.md) (src/backend/catalog/catalog.c:88 - used in system relation classification)

## Notes and Other Information
- More efficient than IsToastRelation when working directly with pg_class tuples
- Used primarily during catalog scanning operations where relations are not opened
- Part of a family of catalog utility functions that work with both open relations and pg_class tuples
- The relnamespace field is accessed directly from the tuple structure
- Located in src/backend/catalog/catalog.c at lines 195-211