# OpFamilyCacheLookup

## Location
[src/backend/commands/opclasscmds.c:81-138](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/opclasscmds.c#L81-L138)

## Overview
OpFamilyCacheLookup is a static function that looks up an existing operator family by name in the PostgreSQL system catalog, returning a syscache tuple reference for further processing.

## Definition
```c
static HeapTuple OpFamilyCacheLookup(Oid amID, List *opfamilyname, bool missing_ok)
```

## Detailed Description
This function searches for an operator family within the system catalog using either a qualified or unqualified name. It handles two distinct lookup strategies:

1. **Qualified names**: When a schema is specified, it performs a direct lookup in the specified namespace using the OPFAMILYAMNAMENSP syscache.
2. **Unqualified names**: When no schema is specified, it searches through the current search path using OpfamilynameGetOpfid() followed by a lookup in the OPFAMILYOID syscache.

The function includes comprehensive error handling, generating detailed error messages when the operator family is not found and missing_ok is false. It validates both the operator family existence and the access method validity.

## Parameters
- `amID`: The OID of the access method that the operator family belongs to
- `opfamilyname`: A list representing the qualified or unqualified name of the operator family to look up
- `missing_ok`: If true, the function returns NULL when the operator family is not found; if false, it raises an error

## Dependencies
- Functions called/Symbols referenced:
  - [DeconstructQualifiedName](../D/DeconstructQualifiedName.md)
  - [LookupExplicitNamespace](../L/LookupExplicitNamespace.md)
  - [SearchSysCache3](../S/SearchSysCache3.md)
  - [OpfamilynameGetOpfid](OpfamilynameGetOpfid.md)
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - [NameListToString](../N/NameListToString.md)
  - Form_pg_am
- Called from:
  - [get_opfamily_oid](../g/get_opfamily_oid.md)

## Notes and Other Information
- This is a static function, meaning it is only accessible within the same source file (opclasscmds.c)
- The function carefully handles memory management by working with syscache tuples
- Error reporting includes both the operator family name and access method name for better diagnostics
- The function supports PostgreSQL's namespace resolution mechanism through qualified/unqualified name handling

## Simplified Source

```c
static HeapTuple OpFamilyCacheLookup(Oid amID, List *opfamilyname, bool missing_ok) {
    char *schemaname;
    char *opfname;
    HeapTuple htup;

    // Parse the qualified/unqualified name
    DeconstructQualifiedName(opfamilyname, &schemaname, &opfname);

    if (schemaname) {
        // Schema-qualified lookup: search in specific namespace
        Oid namespaceId = LookupExplicitNamespace(schemaname, missing_ok);
        if (!OidIsValid(namespaceId))
            htup = NULL;
        else
            htup = SearchSysCache3(OPFAMILYAMNAMENSP,
                                  ObjectIdGetDatum(amID),
                                  PointerGetDatum(opfname),
                                  ObjectIdGetDatum(namespaceId));
    } else {
        // Unqualified lookup: search through search path
        Oid opfID = OpfamilynameGetOpfid(amID, opfname);
        if (!OidIsValid(opfID))
            htup = NULL;
        else
            htup = SearchSysCache1(OPFAMILYOID, ObjectIdGetDatum(opfID));
    }

    // Handle not found case
    if (!HeapTupleIsValid(htup) && !missing_ok) {
        HeapTuple amtup = SearchSysCache1(AMOID, ObjectIdGetDatum(amID));
        if (!HeapTupleIsValid(amtup))
            elog(ERROR, "cache lookup failed for access method %u", amID);
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                 errmsg("operator family \"%s\" does not exist for access method \"%s\"",
                        NameListToString(opfamilyname),
                        NameStr(((Form_pg_am) GETSTRUCT(amtup))->amname))));
    }

    return htup;
}
```