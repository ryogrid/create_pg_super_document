# LookupTypeNameExtended

## Location
[src/backend/parser/parse_type.c:73-231](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_type.c#L73-L231)

## Overview
LookupTypeNameExtended is the core function for looking up PostgreSQL types by name, providing comprehensive type resolution with full control over lookup behavior including temporary types and missing type handling.

## Definition
```c
Type LookupTypeNameExtended(ParseState *pstate, const TypeName *typeName, int32 *typmod_p, bool temp_ok, bool missing_ok)
```

## Detailed Description
LookupTypeNameExtended performs comprehensive type name resolution in PostgreSQL's parser system. It handles three distinct cases: types specified by OID (internally generated TypeName objects), %TYPE references to existing table columns, and normal type name lookups. The function supports schema-qualified names, array type references, and provides fine-grained control over whether temporary types should be considered and how missing types should be handled. The function validates type modifiers and returns a system cache entry for the resolved type.

## Parameters / Member Variables
- `pstate`: ParseState pointer for error location reporting (may be NULL)
- `typeName`: TypeName structure containing the type specification to resolve
- `typmod_p`: Pointer to int32 where resolved type modifier will be stored (may be NULL)
- `temp_ok`: Boolean indicating whether temporary namespace types should be considered
- `missing_ok`: Boolean controlling error behavior when type is not found (true = return NULL, false = raise error)

## Dependencies
- Functions called/Symbols referenced:
  - [makeRangeVar](../m/makeRangeVar.md)
  - [NameListToString](../N/NameListToString.md)
  - RangeVarGetRelid
  - [get_attnum](../g/get_attnum.md)
  - [get_atttype](../g/get_atttype.md)
  - [DeconstructQualifiedName](../D/DeconstructQualifiedName.md)
  - [setup_parser_errposition_callback](../s/setup_parser_errposition_callback.md)
  - [LookupExplicitNamespace](LookupExplicitNamespace.md)
  - GetSysCacheOid2
  - [cancel_parser_errposition_callback](../c/cancel_parser_errposition_callback.md)
  - [TypenameGetTypidExtended](../T/TypenameGetTypidExtended.md)
  - [get_array_type](../g/get_array_type.md)
  - [typenameTypeMod](../t/typenameTypeMod.md)
- Called from (representative examples):
  - [LookupTypeName](LookupTypeName.md)
  - [FuncNameAsType](../F/FuncNameAsType.md)

## Notes and Other Information
Located in src/backend/parser/parse_type.c:73-231. This function requires callers to check typisdefined before assuming the type is fully valid, and successful calls must ReleaseSysCache the returned tuple when done. The function handles complex %TYPE syntax for referencing column types and supports array type decoration. Most code should use the higher-level typenameType or typenameTypeId functions instead of calling this directly.

## Simplified Source

```c
Type
LookupTypeNameExtended(ParseState *pstate,
                       const TypeName *typeName, int32 *typmod_p,
                       bool temp_ok, bool missing_ok)
{
    Oid typoid;
    HeapTuple tup;
    int32 typmod;

    if (typeName->names == NIL)
    {
        // Internal TypeName with OID already set
        typoid = typeName->typeOid;
    }
    else if (typeName->pct_type)
    {
        // Handle %TYPE reference to existing field
        RangeVar *rel = makeRangeVar(NULL, NULL, typeName->location);
        char *field = NULL;
        Oid relid;
        AttrNumber attnum;

        // Parse dotted name (relation.column, schema.relation.column, etc.)
        switch (list_length(typeName->names))
        {
            case 2:
                rel->relname = strVal(linitial(typeName->names));
                field = strVal(lsecond(typeName->names));
                break;
            case 3:
                rel->schemaname = strVal(linitial(typeName->names));
                rel->relname = strVal(lsecond(typeName->names));
                field = strVal(lthird(typeName->names));
                break;
            // Additional cases for 4-part names...
            default:
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                               errmsg("improper %%TYPE reference")));
        }

        // Look up the field and get its type
        relid = RangeVarGetRelid(rel, NoLock, missing_ok);
        attnum = get_attnum(relid, field);
        if (attnum == InvalidAttrNumber)
        {
            if (missing_ok)
                typoid = InvalidOid;
            else
                ereport(ERROR, (errcode(ERRCODE_UNDEFINED_COLUMN),
                               errmsg("column \"%s\" does not exist", field)));
        }
        else
        {
            typoid = get_atttype(relid, attnum);
        }
    }
    else
    {
        // Normal type name lookup
        char *schemaname;
        char *typname;

        DeconstructQualifiedName(typeName->names, &schemaname, &typname);

        if (schemaname)
        {
            // Schema-qualified lookup
            Oid namespaceId = LookupExplicitNamespace(schemaname, missing_ok);
            if (OidIsValid(namespaceId))
                typoid = GetSysCacheOid2(TYPENAMENSP, Anum_pg_type_oid,
                                        PointerGetDatum(typname),
                                        ObjectIdGetDatum(namespaceId));
            else
                typoid = InvalidOid;
        }
        else
        {
            // Search path lookup
            typoid = TypenameGetTypidExtended(typname, temp_ok);
        }

        // Handle array type decoration
        if (typeName->arrayBounds != NIL)
            typoid = get_array_type(typoid);
    }

    if (!OidIsValid(typoid))
    {
        if (typmod_p)
            *typmod_p = -1;
        return NULL;
    }

    // Get type tuple and compute typmod
    tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typoid));
    if (!HeapTupleIsValid(tup))
        elog(ERROR, "cache lookup failed for type %u", typoid);

    typmod = typenameTypeMod(pstate, typeName, (Type) tup);

    if (typmod_p)
        *typmod_p = typmod;

    return (Type) tup;
}
```