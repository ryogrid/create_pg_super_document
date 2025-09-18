# typeOrDomainTypeRelid

## Location
[src/backend/parser/parse_type.c:689-718](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_type.c#L689-L718)

## Overview
Gets the associated relation OID (typrelid) for a given type OID, following through domain type chains to find the base composite type.

## Definition
```c
Oid typeOrDomainTypeRelid(Oid type_id)
```

## Detailed Description
This function is similar to typeidTypeRelid() but with an important enhancement: it follows domain type chains to find the underlying base type's relation OID. When given a domain type, it will recursively follow the typbasetype chain until it reaches a non-domain type, then return that type's typrelid.

This is equivalent to calling typeidTypeRelid(getBaseType(type_id)) but is implemented more efficiently by doing the domain traversal inline rather than making separate function calls.

The function uses a loop to traverse through domain types, checking each type's typtype field. If it's TYPTYPE_DOMAIN, it continues with the base type; otherwise, it returns the typrelid of the final non-domain type.

## Parameters / Member Variables
- `type_id`: The OID of the type (which may be a domain) to look up in the pg_type system catalog

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - HeapTupleIsValid
  - elog
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - GETSTRUCT
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - TYPTYPE_DOMAIN
- Called from (representative examples):
  - [typeInheritsFrom](typeInheritsFrom.md) (src/backend/catalog/pg_inherits.c:417)
  - [typeIsOfTypedTable](typeIsOfTypedTable.md) (src/backend/parser/parse_coerce.c:3384)
  - ISCOMPLEX macro (src/include/parser/parse_type.h:59)

## Notes and Other Information
- More efficient than calling typeidTypeRelid(getBaseType(type_id))
- Handles domain type chains transparently
- Returns InvalidOid for non-composite base types
- Throws an ERROR if any type OID in the chain is not found in the system catalog
- Uses system cache for efficient lookups during domain traversal