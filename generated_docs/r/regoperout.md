# regoperout

## Location
[src/backend/utils/adt/regproc.c:545-612](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regproc.c#L545-L612)

## Overview
Converts an operator OID to its string representation, including proper namespace qualification when necessary.

## Definition
```c
Datum regoperout(PG_FUNCTION_ARGS)
```

## Detailed Description
The `regoperout` function is a PostgreSQL output function that converts an operator OID (Object Identifier) back to its string representation. This function is the inverse of `regoperin` and is used internally by PostgreSQL when displaying regoper values to users.

The function performs several sophisticated operations:
1. Handles invalid OIDs by returning "0"
2. Looks up the operator in the system catalog (`pg_operator`)
3. In bootstrap mode, returns just the operator name
4. In normal mode, determines whether namespace qualification is needed by checking if the operator name would be unique without qualification
5. If qualification is needed, prepends the schema name
6. For non-existent operators, returns the OID as a numeric string

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro
  - Argument 0: OID of the operator to convert to string

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_OID`: Extract OID argument from function call
  - [pstrdup](../p/pstrdup.md): Duplicate a C string with palloc
  - [SearchSysCache1](../S/SearchSysCache1.md): Search system cache for operator tuple
  - `HeapTupleIsValid`: Check if heap tuple is valid
  - `Form_pg_operator`: Cast to pg_operator structure
  - `GETSTRUCT`: Extract structure from heap tuple
  - `NameStr`: Extract name from Name structure
  - `IsBootstrapProcessingMode`: Check if in bootstrap mode
  - [OpernameGetCandidates](../O/OpernameGetCandidates.md): Find operator candidates by name
  - `list_make1`: Create single-element list
  - [makeString](../m/makeString.md): Create String node
  - [get_namespace_name](../g/get_namespace_name.md): Get namespace name by OID
  - [quote_identifier](../q/quote_identifier.md): Quote SQL identifier if needed
  - [ReleaseSysCache](../R/ReleaseSysCache.md): Release system cache tuple
  - [palloc](../p/palloc.md): PostgreSQL memory allocator
  - `sprintf`: Format string
  - `snprintf`: Safe string formatting
  - `PG_RETURN_CSTRING`: Return C string from function

- Called from (representative examples):
  - No direct references found (typically called via PostgreSQL's type system)

## Notes and Other Information
- This is an output function for the regoper data type
- The function intelligently handles namespace qualification to ensure the output can be parsed back unambiguously
- In bootstrap mode, namespace resolution is skipped for simplicity
- Returns numeric representation for operators that no longer exist in the catalog
- Part of PostgreSQL's regtype family for displaying object references in a user-friendly format

## Simplified Source

```c
Datum
regoperout(PG_FUNCTION_ARGS)
{
    Oid oprid = PG_GETARG_OID(0);
    char *result;
    HeapTuple opertup;

    // Handle invalid OID case
    if (oprid == InvalidOid) {
        result = pstrdup("0");
        PG_RETURN_CSTRING(result);
    }

    // Look up operator in system catalog
    opertup = SearchSysCache1(OPEROID, ObjectIdGetDatum(oprid));

    if (HeapTupleIsValid(opertup)) {
        Form_pg_operator operform = (Form_pg_operator) GETSTRUCT(opertup);
        char *oprname = NameStr(operform->oprname);

        // In bootstrap mode, just return the operator name
        if (IsBootstrapProcessingMode()) {
            result = pstrdup(oprname);
        } else {
            // Check if operator name needs namespace qualification
            FuncCandidateList clist = OpernameGetCandidates(
                list_make1(makeString(oprname)), '\0', false);

            if (clist != NULL && clist->next == NULL && clist->oid == oprid) {
                // Name is unique, no qualification needed
                result = pstrdup(oprname);
            } else {
                // Need to qualify with namespace
                const char *nspname = get_namespace_name(operform->oprnamespace);
                nspname = quote_identifier(nspname);
                result = (char *) palloc(strlen(nspname) + strlen(oprname) + 2);
                sprintf(result, "%s.%s", nspname, oprname);
            }
        }
        ReleaseSysCache(opertup);
    } else {
        // Operator not found, return numeric OID
        result = (char *) palloc(NAMEDATALEN);
        snprintf(result, NAMEDATALEN, "%u", oprid);
    }

    PG_RETURN_CSTRING(result);
}
```