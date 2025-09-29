# OpernameGetOprid

## Location
[src/backend/catalog/namespace.c:1785-1887](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L1785-L1887)

## Overview
OpernameGetOprid looks up an operator by its name and exact input data types, returning the operator's OID or InvalidOid if not found.

## Definition

```c
struct the name list */
	DeconstructQualifiedName(names, &schemaname, &opername);
```
## Detailed Description
This function performs operator resolution in the PostgreSQL catalog system. It takes a possibly schema-qualified operator name and exact input data types to locate the corresponding operator. The function handles both schema-qualified and unqualified operator names. For schema-qualified names, it searches only within the specified schema. For unqualified names, it searches through the current namespace search path to find the first matching operator. The function supports prefix operators by accepting InvalidOid for the left operand type.

## Parameters / Member Variables
- : List containing the operator name, possibly schema-qualified
- : OID of the left operand data type (use InvalidOid for prefix operators)
- : OID of the right operand data type

## Dependencies
- Functions called/Symbols referenced:
  - [DeconstructQualifiedName](../D/DeconstructQualifiedName.md)
  - [LookupExplicitNamespace](../L/LookupExplicitNamespace.md)
  - [SearchSysCache4](../S/SearchSysCache4.md)
  - SearchSysCacheList3
  - [recomputeNamespacePath](../r/recomputeNamespacePath.md)
  - [CStringGetDatum](../C/CStringGetDatum.md)
  - Form_pg_operator
  - ReleaseSysCacheList
- Called from (representative examples):
  - [OperatorIsVisibleExt](OperatorIsVisibleExt.md)
  - [LookupOperName](../L/LookupOperName.md)
  - [binary_oper_exact](../b/binary_oper_exact.md)
  - [left_oper](../l/left_oper.md)
  - [regoperatorin](../r/regoperatorin.md)

## Notes and Other Information
- Returns InvalidOid if the operator is not found or if a specified schema does not exist
- For unqualified names, searches through the active search path and returns the first match
- Skips the temporary namespace during search path traversal
- Uses system cache lookups for efficient operator resolution
- Critical for operator resolution in SQL parsing and execution phases

## Simplified Source

```c
Oid
OpernameGetOprid(List *names, Oid oprleft, Oid oprright)
{
    char *schemaname;
    char *opername;
    CatCList *catlist;
    ListCell *l;

    // Parse the qualified name into schema and operator parts
    DeconstructQualifiedName(names, &schemaname, &opername);

    if (schemaname) {
        // Schema-qualified: search only in specified schema
        Oid namespaceId = LookupExplicitNamespace(schemaname, true);

        if (OidIsValid(namespaceId)) {
            HeapTuple opertup = SearchSysCache4(OPERNAMENSP,
                                               CStringGetDatum(opername),
                                               ObjectIdGetDatum(oprleft),
                                               ObjectIdGetDatum(oprright),
                                               ObjectIdGetDatum(namespaceId));
            if (HeapTupleIsValid(opertup)) {
                Form_pg_operator operclass = (Form_pg_operator) GETSTRUCT(opertup);
                Oid result = operclass->oid;
                ReleaseSysCache(opertup);
                return result;
            }
        }
        return InvalidOid;
    }

    // Unqualified: search by name and argument types across search path
    catlist = SearchSysCacheList3(OPERNAMENSP,
                                  CStringGetDatum(opername),
                                  ObjectIdGetDatum(oprleft),
                                  ObjectIdGetDatum(oprright));

    if (catlist->n_members == 0) {
        ReleaseSysCacheList(catlist);
        return InvalidOid;
    }

    // Find the first match in the namespace search path
    recomputeNamespacePath();

    foreach(l, activeSearchPath) {
        Oid namespaceId = lfirst_oid(l);

        if (namespaceId == myTempNamespace)
            continue;  // Skip temp namespace

        for (int i = 0; i < catlist->n_members; i++) {
            HeapTuple opertup = &catlist->members[i]->tuple;
            Form_pg_operator operform = (Form_pg_operator) GETSTRUCT(opertup);

            if (operform->oprnamespace == namespaceId) {
                Oid result = operform->oid;
                ReleaseSysCacheList(catlist);
                return result;
            }
        }
    }

    ReleaseSysCacheList(catlist);
    return InvalidOid;
}
```