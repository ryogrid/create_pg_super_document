# get_object_address_defacl

## Location
[src/backend/catalog/objectaddress.c:1958-2073](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/objectaddress.c#L1958-L2073)

## Overview
Finds and returns the ObjectAddress for a default ACL (Access Control List) by resolving the object type, username, and optional schema name to locate the corresponding pg_default_acl catalog entry.

## Definition
```c
static ObjectAddress get_object_address_defacl(List *object, bool missing_ok)
```

## Detailed Description
This function resolves a default ACL object address by parsing a list containing an object type character, username, and optional schema name. Default ACLs in PostgreSQL define the privileges that are automatically granted on newly created objects of specific types. The function performs a multi-stage lookup: decoding the object type character, resolving the username to a user ID, optionally resolving the schema name to a namespace OID, and finally searching for the corresponding entry in pg_default_acl.

The function supports five object types for default ACLs: relations (tables), sequences, functions, types, and schemas (namespaces). Each is identified by a single character code defined by the DEFACLOBJ_* constants. The function includes comprehensive error handling and provides descriptive error messages that include the resolved textual names.

## Parameters / Member Variables
- `object`: List containing 2-3 elements - object type character (first), username (second), and optional schema name (third)
- `missing_ok`: Boolean flag indicating whether to return an invalid ObjectAddress (true) or raise an error (false) when the default ACL is not found

## Dependencies
- Functions called/Symbols referenced:
  - ObjectAddressSet
  - strVal/linitial/lsecond/lthird (list manipulation and string value extraction)
  - [list_length](../l/list_length.md)
  - [SearchSysCache1](../S/SearchSysCache1.md)/SearchSysCache3 (AUTHNAME and DEFACLROLENSPOBJ cache lookups)
  - [get_namespace_oid](get_namespace_oid.md) (schema name to OID resolution)
  - [CStringGetDatum](../C/CStringGetDatum.md)/ObjectIdGetDatum/CharGetDatum (datum conversion)
  - Form_pg_authid/Form_pg_default_acl (catalog tuple access)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - DEFACLOBJ_* constants (RELATION, SEQUENCE, FUNCTION, TYPE, NAMESPACE)
- Called from (representative examples):
  - [get_object_address](get_object_address.md) (main object address resolution dispatcher)
  - object_type_map (object type mapping table)

## Notes and Other Information
- Uses DEFACLROLENSPOBJ system cache index for efficient default ACL lookup using role OID, namespace OID, and object type
- Returns an ObjectAddress with DefaultAclRelationId as the class ID and the default ACL OID as the object ID
- Supports both global default ACLs (schema is NULL/InvalidOid) and schema-specific default ACLs
- Only the first character of the object type string is considered; additional characters are ignored
- Error messages differentiate between schema-specific and global default ACLs for clarity
- Part of PostgreSQL's privilege management system for automatically granting permissions on newly created objects
- Uses a goto-based error handling pattern with a common not_found label for consistent error reporting

## Simplified Source

```c
static ObjectAddress
get_object_address_defacl(List *object, bool missing_ok)
{
    HeapTuple tp;
    Oid userid, schemaid;
    char *username, *schema;
    char objtype;
    ObjectAddress address;

    ObjectAddressSet(address, DefaultAclRelationId, InvalidOid);

    // Extract parameters from input list
    objtype = ((char *) strVal(linitial(object)))[0];  // First char of object type
    username = strVal(lsecond(object));                // Username
    schema = (list_length(object) >= 3) ? strVal(lthird(object)) : NULL;

    // Validate object type (tables, sequences, functions, types, schemas)
    switch (objtype) {
        case DEFACLOBJ_RELATION:
        case DEFACLOBJ_SEQUENCE:
        case DEFACLOBJ_FUNCTION:
        case DEFACLOBJ_TYPE:
        case DEFACLOBJ_NAMESPACE:
            break;
        default:
            ereport(ERROR, "unrecognized default ACL object type");
    }

    // Look up user ID by username
    tp = SearchSysCache1(AUTHNAME, CStringGetDatum(username));
    if (!HeapTupleIsValid(tp))
        goto not_found;
    userid = ((Form_pg_authid) GETSTRUCT(tp))->oid;
    ReleaseSysCache(tp);

    // Look up schema OID if provided
    if (schema) {
        schemaid = get_namespace_oid(schema, true);
        if (schemaid == InvalidOid)
            goto not_found;
    } else {
        schemaid = InvalidOid;
    }

    // Find the default ACL entry
    tp = SearchSysCache3(DEFACLROLENSPOBJ,
                        ObjectIdGetDatum(userid),
                        ObjectIdGetDatum(schemaid),
                        CharGetDatum(objtype));
    if (!HeapTupleIsValid(tp))
        goto not_found;

    address.objectId = ((Form_pg_default_acl) GETSTRUCT(tp))->oid;
    ReleaseSysCache(tp);
    return address;

not_found:
    if (!missing_ok)
        ereport(ERROR, "default ACL does not exist");
    return address;  // Returns invalid ObjectAddress
}
```