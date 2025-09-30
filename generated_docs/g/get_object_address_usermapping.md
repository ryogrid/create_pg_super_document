# get_object_address_usermapping

## Location
[src/backend/catalog/objectaddress.c:1792-1862](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/objectaddress.c#L1792-L1862)

## Overview
Finds and returns the ObjectAddress for a user mapping by resolving the username and server name to locate the corresponding pg_user_mapping catalog entry.

## Definition

```c
static ObjectAddress
get_object_address_usermapping(List *object, bool missing_ok)
```
## Detailed Description
This function resolves a user mapping object address by taking a list containing a username and server name, then performing lookups in the PostgreSQL system catalogs to find the corresponding user mapping. The function handles the special case of "public" user mappings (where userid is InvalidOid) and provides controlled error handling based on the missing_ok parameter.

The function performs a two-stage lookup process: first resolving the username to a user ID (or InvalidOid for "public"), then looking up the foreign server by name, and finally searching for the user mapping entry in pg_user_mapping using both the user ID and server ID as keys.

## Parameters / Member Variables
- : List containing exactly two string values - the username and server name for the user mapping
- : Boolean flag indicating whether to return an invalid ObjectAddress (true) or raise an error (false) when the user mapping is not found

## Dependencies
- Functions called/Symbols referenced:
  - ObjectAddressSet
  - strVal/linitial/lsecond (list manipulation)
  - [SearchSysCache1](../S/SearchSysCache1.md)/SearchSysCache2 (system catalog lookups)
  - [GetForeignServerByName](../G/GetForeignServerByName.md)
  - [CStringGetDatum](../C/CStringGetDatum.md)/ObjectIdGetDatum (datum conversion)
  - Form_pg_authid/Form_pg_user_mapping (catalog tuple access)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
- Called from (representative examples):
  - [get_object_address](get_object_address.md) (main object address resolution dispatcher)
  - object_type_map (object type mapping table)

## Notes and Other Information
- Handles the special "public" user case by setting userid to InvalidOid rather than looking up an actual user
- Uses USERMAPPINGUSERSERVER system cache index for efficient user mapping lookup
- Returns an ObjectAddress with UserMappingRelationId as the class ID and the user mapping OID as the object ID
- Error messages provide both username and server name context for better diagnostics
- Part of PostgreSQL's object address resolution system used for dependency tracking and privilege management

## Simplified Source

```c
static ObjectAddress
get_object_address_usermapping(List *object, bool missing_ok)
{
    ObjectAddress address;
    Oid userid;
    char *username, *servername;
    ForeignServer *server;
    HeapTuple tp;

    ObjectAddressSet(address, UserMappingRelationId, InvalidOid);

    // Extract username and server name from input list
    username = strVal(linitial(object));
    servername = strVal(lsecond(object));

    // Handle special "public" user case or look up user ID
    if (strcmp(username, "public") == 0) {
        userid = InvalidOid;
    } else {
        tp = SearchSysCache1(AUTHNAME, CStringGetDatum(username));
        if (!HeapTupleIsValid(tp)) {
            if (!missing_ok)
                ereport(ERROR, "user mapping does not exist");
            return address;
        }
        userid = ((Form_pg_authid) GETSTRUCT(tp))->oid;
        ReleaseSysCache(tp);
    }

    // Look up foreign server by name
    server = GetForeignServerByName(servername, true);
    if (!server) {
        if (!missing_ok)
            ereport(ERROR, "server does not exist");
        return address;
    }

    // Find the user mapping entry
    tp = SearchSysCache2(USERMAPPINGUSERSERVER,
                        ObjectIdGetDatum(userid),
                        ObjectIdGetDatum(server->serverid));
    if (!HeapTupleIsValid(tp)) {
        if (!missing_ok)
            ereport(ERROR, "user mapping does not exist");
        return address;
    }

    address.objectId = ((Form_pg_user_mapping) GETSTRUCT(tp))->oid;
    ReleaseSysCache(tp);

    return address;
}
```