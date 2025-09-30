# get_object_address_publication_schema

## Location
[src/backend/catalog/objectaddress.c:1916-1957](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/objectaddress.c#L1916-L1957)

## Overview
Finds and returns the ObjectAddress for a publication schema mapping by resolving both the schema name and publication name to locate the corresponding pg_publication_namespace catalog entry.

## Definition
```c
static ObjectAddress get_object_address_publication_schema(List *object, bool missing_ok)
```

## Detailed Description
This function resolves a publication schema object address by taking a list containing a schema name and publication name, then performing lookups to find the corresponding publication-schema mapping. It first resolves the schema name to a namespace OID using get_namespace_oid, then looks up the publication by name, and finally searches for the mapping entry in pg_publication_namespace that connects the specific schema to the publication.

This function is part of PostgreSQL's logical replication system, which allows publications to include entire schemas (all tables within a schema) rather than individual tables. The publication schema mapping tracks which schemas are included in each publication.

## Parameters / Member Variables
- `object`: List containing exactly two string elements - the schema name and the publication name
- `missing_ok`: Boolean flag indicating whether to return an invalid ObjectAddress (true) or raise an error (false) when the publication schema mapping is not found

## Dependencies
- Functions called/Symbols referenced:
  - ObjectAddressSet
  - strVal/linitial/lsecond (list manipulation and string value extraction)
  - [get_namespace_oid](get_namespace_oid.md) (schema name to OID resolution)
  - [GetPublicationByName](../G/GetPublicationByName.md)
  - GetSysCacheOid2 (PUBLICATIONNAMESPACEMAP cache lookup)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
- Called from (representative examples):
  - [get_object_address](get_object_address.md) (main object address resolution dispatcher)
  - object_type_map (object type mapping table)

## Notes and Other Information
- Uses PUBLICATIONNAMESPACEMAP system cache index for efficient publication-schema mapping lookup
- Returns an ObjectAddress with PublicationNamespaceRelationId as the class ID and the publication namespace mapping OID as the object ID
- Error messages include both schema name and publication name for better diagnostics
- Part of PostgreSQL's logical replication system for tracking which schemas are included in publications
- Supports the "FOR ALL TABLES IN SCHEMA" feature of logical replication publications
- Unlike the publication relation variant, this function does not need to open or return any additional objects

## Simplified Source

```c
static ObjectAddress
get_object_address_publication_schema(List *object, bool missing_ok)
{
    ObjectAddress address;
    Publication *pub;
    char *pubname, *schemaname;
    Oid schemaid;

    ObjectAddressSet(address, PublicationNamespaceRelationId, InvalidOid);

    // Extract schema and publication names from input list
    schemaname = strVal(linitial(object));
    pubname = strVal(lsecond(object));

    // Look up schema OID by name
    schemaid = get_namespace_oid(schemaname, missing_ok);
    if (!OidIsValid(schemaid))
        return address;

    // Look up publication by name
    pub = GetPublicationByName(pubname, missing_ok);
    if (!pub)
        return address;

    // Find the publication-schema mapping
    address.objectId = GetSysCacheOid2(PUBLICATIONNAMESPACEMAP,
                                      Anum_pg_publication_namespace_oid,
                                      ObjectIdGetDatum(schemaid),
                                      ObjectIdGetDatum(pub->oid));

    if (!OidIsValid(address.objectId) && !missing_ok)
        ereport(ERROR, "publication schema does not exist");

    return address;
}
```