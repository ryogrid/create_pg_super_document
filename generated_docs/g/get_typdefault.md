# get_typdefault

## Location
[src/backend/utils/cache/lsyscache.c:2448-2520](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L2448-L2520)

## Overview
Retrieves the default value expression for a specified PostgreSQL data type, returning it as a parsed expression node tree that can be used in query planning and execution.

## Definition

```c
struct fields. Must do it the hard way with
	 * SysCacheGetAttr.
	 */
	datum = SysCacheGetAttr(TYPEOID,
							typeTuple,
							Anum_pg_type_typdefaultbin,
							&isNull);
```
## Detailed Description
This function performs a system catalog lookup to retrieve the default value for a given data type from the pg_type system catalog. It handles two forms of default values: binary expression defaults (stored in typdefaultbin) and plain text literal defaults (stored in typdefault). For binary defaults, it deserializes the stored expression tree using stringToNode(). For text defaults, it converts the text to the appropriate type value using the type's input function and creates a Const node. The function returns NULL if no default value is defined for the type. The caller is responsible for ensuring type coercion if needed, as the returned expression might not exactly match the expected datatype.

## Parameters / Member Variables
- : The OID of the data type whose default value is to be retrieved

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - HeapTupleIsValid
  - GETSTRUCT
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md)
  - [stringToNode](../s/stringToNode.md)
  - TextDatumGetCString
  - [OidInputFunctionCall](../O/OidInputFunctionCall.md)
  - [getTypeIOParam](getTypeIOParam.md)
  - [makeConst](../m/makeConst.md)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - Form_pg_type
- Called from (representative examples):
  - [build_column_default](../b/build_column_default.md)

## Notes and Other Information
- Returns a palloc'd expression node tree that must be freed by the caller when no longer needed
- Handles both typdefaultbin (expression tree) and typdefault (literal text) columns from pg_type
- The function gives preference to typdefaultbin over typdefault when both are present
- For literal defaults, the function performs input parsing using the type's own input function
- The returned Const node includes proper type information (OID, length, collation, pass-by-value flag)
- Part of the lsyscache.c module which provides cached access to system catalog information
- Used primarily in DDL operations and query rewriting where default values need to be materialized
- The caller should be prepared to perform type coercion as the default might be of a related but not identical type

## Simplified Source

```c
Node *get_typdefault(Oid typid) {
    // Look up the type in system catalog
    HeapTuple typeTuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
    if (!HeapTupleIsValid(typeTuple))
        elog(ERROR, "cache lookup failed for type %u", typid);

    Form_pg_type type = (Form_pg_type) GETSTRUCT(typeTuple);
    Datum datum;
    bool isNull;
    Node *expr;

    // First try to get binary expression default (typdefaultbin)
    datum = SysCacheGetAttr(TYPEOID, typeTuple,
                           Anum_pg_type_typdefaultbin, &isNull);

    if (!isNull) {
        // We have a binary expression default - deserialize it
        expr = stringToNode(TextDatumGetCString(datum));
    } else {
        // Try plain text literal default (typdefault)
        datum = SysCacheGetAttr(TYPEOID, typeTuple,
                               Anum_pg_type_typdefault, &isNull);

        if (!isNull) {
            // Convert text default to proper type value and create Const node
            char *strDefaultVal = TextDatumGetCString(datum);

            // Parse the text using type's input function
            datum = OidInputFunctionCall(type->typinput, strDefaultVal,
                                        getTypeIOParam(typeTuple), -1);

            // Create Const node with the parsed value
            expr = (Node *) makeConst(typid, -1, type->typcollation,
                                     type->typlen, datum, false, type->typbyval);
            pfree(strDefaultVal);
        } else {
            // No default value defined
            expr = NULL;
        }
    }

    ReleaseSysCache(typeTuple);
    return expr;
}
```