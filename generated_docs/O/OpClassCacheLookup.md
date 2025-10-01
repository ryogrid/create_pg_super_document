# OpClassCacheLookup

## Location
[src/backend/commands/opclasscmds.c:162-219](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/opclasscmds.c#L162-L219)

## Overview
OpClassCacheLookup is a static function that looks up an existing operator class by name in the PostgreSQL system catalog, returning a syscache tuple reference for further processing.

## Definition
```c
static HeapTuple OpClassCacheLookup(Oid amID, List *opclassname, bool missing_ok)
```

## Detailed Description
This function searches for an operator class within the system catalog using either a qualified or unqualified name. Similar to OpFamilyCacheLookup, it handles two distinct lookup strategies:

1. **Qualified names**: When a schema is specified, it performs a direct lookup in the specified namespace using the CLAAMNAMENSP syscache.
2. **Unqualified names**: When no schema is specified, it searches through the current search path using OpclassnameGetOpcid() followed by a lookup in the CLAOID syscache.

The function includes comprehensive error handling, generating detailed error messages when the operator class is not found and missing_ok is false. It validates both the operator class existence and the access method validity, providing context-specific error messages.

## Parameters
- `amID`: The OID of the access method that the operator class belongs to
- `opclassname`: A list representing the qualified or unqualified name of the operator class to look up
- `missing_ok`: If true, the function returns NULL when the operator class is not found; if false, it raises an error

## Dependencies
- Functions called/Symbols referenced:
  - [DeconstructQualifiedName](../D/DeconstructQualifiedName.md)
  - [LookupExplicitNamespace](../L/LookupExplicitNamespace.md)
  - [SearchSysCache3](../S/SearchSysCache3.md)
  - [OpclassnameGetOpcid](OpclassnameGetOpcid.md)
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - [NameListToString](../N/NameListToString.md)
  - Form_pg_am
- Called from:
  - [get_opclass_oid](../g/get_opclass_oid.md)

## Notes and Other Information
- This is a static function, meaning it is only accessible within the same source file (opclasscmds.c)
- The function follows the same pattern as OpFamilyCacheLookup but operates on operator classes instead of operator families
- Uses different syscache keys (CLAAMNAMENSP and CLAOID) compared to the operator family lookup functions
- Error reporting includes both the operator class name and access method name for better diagnostics
- Supports PostgreSQL's namespace resolution mechanism for both qualified and unqualified names

## Simplified Source

```c
static HeapTuple OpClassCacheLookup(Oid amID, List *opclassname, bool missing_ok)
{
    char *schemaname;
    char *opcname;
    HeapTuple htup;

    // Parse the qualified name
    DeconstructQualifiedName(opclassname, &schemaname, &opcname);

    if (schemaname)
    {
        // Look in specific schema
        Oid namespaceId = LookupExplicitNamespace(schemaname, missing_ok);
        if (!OidIsValid(namespaceId))
            htup = NULL;
        else
            htup = SearchSysCache3(CLAAMNAMENSP,
                                   ObjectIdGetDatum(amID),
                                   PointerGetDatum(opcname),
                                   ObjectIdGetDatum(namespaceId));
    }
    else
    {
        // Search through search path
        Oid opcID = OpclassnameGetOpcid(amID, opcname);
        if (!OidIsValid(opcID))
            htup = NULL;
        else
            htup = SearchSysCache1(CLAOID, ObjectIdGetDatum(opcID));
    }

    // Handle not found case
    if (!HeapTupleIsValid(htup) && !missing_ok)
    {
        HeapTuple amtup = SearchSysCache1(AMOID, ObjectIdGetDatum(amID));
        if (!HeapTupleIsValid(amtup))
            elog(ERROR, "cache lookup failed for access method %u", amID);

        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                 errmsg("operator class \"%s\" does not exist for access method \"%s\"",
                        NameListToString(opclassname),
                        NameStr(((Form_pg_am) GETSTRUCT(amtup))->amname))));
    }

    return htup;
}
```