# regoperin

## Location
[src/backend/utils/adt/regproc.c:478-526](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regproc.c#L478-L526)

## Overview
Converts operator name strings to operator OIDs, serving as the input function for PostgreSQL's regoper data type.

## Definition
```c
Datum regoperin(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the input function for PostgreSQL's regoper data type, responsible for converting string representations of operators into their corresponding OIDs. The function accepts both operator names (which may be schema-qualified) and numeric OIDs as input. When given a name, it performs operator name resolution using PostgreSQL's search path mechanism to find matching operators in pg_operator.

The function handles several important cases: numeric OID input for symmetry with output functions, the special value '0' for unknown operators, proper error handling for ambiguous or non-existent operators, and bootstrap mode restrictions where only numeric OIDs are accepted.

## Parameters / Member Variables
- Input: C-string containing operator name or numeric OID (retrieved via PG_GETARG_CSTRING(0))
- Returns: OID of the matched operator

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CSTRING
  - [parseNumericOid](../p/parseNumericOid.md)
  - PG_RETURN_OID
  - IsBootstrapProcessingMode
  - elog
  - [stringToQualifiedNameList](../s/stringToQualifiedNameList.md)
  - [OpernameGetCandidates](../O/OpernameGetCandidates.md)
  - ereturn
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
  - FuncCandidateList (type)
- Called from (representative examples):
  - [to_regoper](../t/to_regoper.md) (src/backend/utils/adt/regproc.c:533)

## Notes and Other Information
- This is a standard PostgreSQL type input function following the naming convention of [typename]in
- Accepts both operator names and numeric OIDs for flexibility
- Uses PostgreSQL's standard name resolution mechanism with search path support
- Handles schema-qualified operator names (e.g., "pg_catalog.+")
- Provides proper error handling for undefined operators and ambiguous operator names
- Bootstrap mode restriction ensures system stability during initial database setup
- The function is part of PostgreSQL's operator registration and identification system
- Special handling for '0' input representing InvalidOid/unknown operator
- Uses the same candidate list mechanism as other operator lookup functions for consistency

## Simplified Source

```c
Datum regoperin(PG_FUNCTION_ARGS) {
    char *opr_name_or_oid = PG_GETARG_CSTRING(0);
    Node *escontext = fcinfo->context;
    Oid result;

    // Handle numeric OID input
    if (parseNumericOid(opr_name_or_oid, &result, escontext))
        PG_RETURN_OID(result);

    // Bootstrap mode only accepts OIDs
    if (IsBootstrapProcessingMode())
        elog(ERROR, "regoper values must be OIDs in bootstrap mode");

    // Parse operator name (possibly schema-qualified)
    List *names = stringToQualifiedNameList(opr_name_or_oid, escontext);
    if (names == NIL)
        PG_RETURN_NULL();

    // Find candidate operators with matching name
    FuncCandidateList clist = OpernameGetCandidates(names, '\0', true);

    // Check for no matches
    if (clist == NULL)
        ereturn(escontext, (Datum) 0,
                (errcode(ERRCODE_UNDEFINED_FUNCTION),
                 errmsg("operator does not exist: %s", opr_name_or_oid)));

    // Check for ambiguous matches
    else if (clist->next != NULL)
        ereturn(escontext, (Datum) 0,
                (errcode(ERRCODE_AMBIGUOUS_FUNCTION),
                 errmsg("more than one operator named %s", opr_name_or_oid)));

    result = clist->oid;
    PG_RETURN_OID(result);
}
```