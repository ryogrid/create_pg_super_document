# get_object_address_type

## Location
[src/backend/catalog/objectaddress.c:1603-1641](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/objectaddress.c#L1603-L1641)

## Overview
Finds the ObjectAddress for a type or domain by resolving a TypeName specification into the corresponding catalog entry.

## Definition
static ObjectAddress get_object_address_type(ObjectType objtype, TypeName *typename, bool missing_ok)

## Detailed Description
This static function resolves a type name into an ObjectAddress structure, handling both regular types and domains. It uses LookupTypeName to find the type in the system catalogs, validates that domains are actually domain types when requested, and constructs an ObjectAddress with TypeRelationId as classId and the type's OID as objectId. The function provides appropriate error handling for missing types and type mismatches, with support for graceful handling via the missing_ok parameter.

## Parameters / Member Variables
- : The type of object being addressed - used to validate that domains are actually domain types when OBJECT_DOMAIN is specified
- : A TypeName structure containing the type specification to resolve
- : If true, returns an invalid ObjectAddress when the type doesn't exist instead of raising an error

## Dependencies
- Functions called/Symbols referenced:
  - [LookupTypeName](../L/LookupTypeName.md) (looks up type by name in system catalogs)
  - [typeTypeId](../t/typeTypeId.md) (extracts OID from Type tuple)
  - [TypeNameToString](../T/TypeNameToString.md) (converts TypeName to string for error messages)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (releases system cache tuple)
  - GETSTRUCT (extracts struct from heap tuple)
- Called from (representative examples):
  - [get_object_address](get_object_address.md) (main object address resolution function)
  - [get_object_address_opf_member](get_object_address_opf_member.md) (for resolving operator family member types)

## Notes and Other Information
- Returns ObjectAddress with classId set to TypeRelationId and objectSubId set to 0
- When objtype is OBJECT_DOMAIN, validates that the found type is actually a domain (typtype == TYPTYPE_DOMAIN)
- Uses system cache for type lookup and properly releases the cache entry
- When missing_ok is true and the type doesn't exist, returns an ObjectAddress with InvalidOid
- Handles both simple and qualified type names through the TypeName structure
- Domain validation ensures type system integrity by preventing non-domains from being treated as domains

## Simplified Source

```c
static ObjectAddress
get_object_address_type(ObjectType objtype, TypeName *typename, bool missing_ok)
{
    ObjectAddress address;
    Type tup;

    // Initialize address with TypeRelationId
    address.classId = TypeRelationId;
    address.objectId = InvalidOid;
    address.objectSubId = 0;

    // Look up the type by name
    tup = LookupTypeName(NULL, typename, NULL, missing_ok);
    if (!HeapTupleIsValid(tup)) {
        if (!missing_ok)
            ereport(ERROR, "type does not exist");
        return address;
    }

    // Get the type OID
    address.objectId = typeTypeId(tup);

    // Validate that domains are actually domain types
    if (objtype == OBJECT_DOMAIN) {
        if (((Form_pg_type) GETSTRUCT(tup))->typtype != TYPTYPE_DOMAIN)
            ereport(ERROR, "object is not a domain");
    }

    ReleaseSysCache(tup);
    return address;
}
```