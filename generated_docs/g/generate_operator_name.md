# generate_operator_name

## Location
[src/backend/utils/adt/ruleutils.c:13032-13108](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L13032-L13108)

## Overview
Computes the name to display for an operator specified by OID, given that it is being called with the specified actual argument types.

## Definition
```c
static char *generate_operator_name(Oid operid, Oid arg1, Oid arg2)
```

## Detailed Description
This function generates a properly formatted operator name for display purposes, handling operator resolution ambiguity by considering argument types. The function determines whether schema-qualification is necessary based on whether the parser would be able to resolve the correct operator given just the unqualified operator name with the specified argument types.

The function supports both binary operators (oprkind = `b`) and left unary operators (oprkind = `l`). If schema-qualification is needed, it wraps the operator name in the `OPERATOR(schema.name)` syntax required for qualified operator usage in expressions.

The result includes all necessary quoting and schema-prefixing, plus the OPERATOR() decoration needed to use a qualified operator name in an expression.

## Parameters / Member Variables
- `operid`: The OID of the operator to generate a name for
- `arg1`: The OID of the first (left) argument type; pass InvalidOid for unused arg of a unary operator
- `arg2`: The OID of the second (right) argument type; pass InvalidOid for unused arg of a unary operator

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md) (system cache lookup)
  - [oper](../o/oper.md) (binary operator lookup)
  - [left_oper](../l/left_oper.md) (left unary operator lookup)  
  - [makeString](../m/makeString.md) (string construction utility)
  - [oprid](../o/oprid.md) (get operator OID from Operator)
  - [get_namespace_name_or_temp](get_namespace_name_or_temp.md) (namespace name resolution)
  - [quote_identifier](../q/quote_identifier.md) (identifier quoting)
- Called from (representative examples):
  - [get_oper_expr](get_oper_expr.md) (operator expression formatting)
  - [get_rule_expr](get_rule_expr.md) (rule expression decompilation)
  - [get_sublink_expr](get_sublink_expr.md) (sublink expression formatting)
  - [pg_get_indexdef_worker](../p/pg_get_indexdef_worker.md) (index definition formatting)

## Notes and Other Information
- The function uses operator resolution logic to determine if schema-qualification is necessary
- Only handles binary (b) and left unary (l) operators; right unary operators would cause an error
- Memory management is handled through StringInfo buffer that caller must free
- Part of the rule decompilation system used for displaying stored rules, views, and constraints

## Simplified Source

```c
static char *generate_operator_name(Oid operid, Oid arg1, Oid arg2) {
    StringInfoData buf;
    HeapTuple opertup;
    Form_pg_operator operform;
    char *oprname;
    char *nspname;
    Operator p_result;

    initStringInfo(&buf);

    // Look up operator information in system cache
    opertup = SearchSysCache1(OPEROID, ObjectIdGetDatum(operid));
    if (!HeapTupleIsValid(opertup))
        elog(ERROR, "cache lookup failed for operator %u", operid);

    operform = (Form_pg_operator) GETSTRUCT(opertup);
    oprname = NameStr(operform->oprname);

    // Test if parser can resolve unqualified operator with these arg types
    switch (operform->oprkind) {
        case 'b':  // Binary operator
            p_result = oper(NULL, list_make1(makeString(oprname)), arg1, arg2, true, -1);
            break;
        case 'l':  // Left unary operator
            p_result = left_oper(NULL, list_make1(makeString(oprname)), arg2, true, -1);
            break;
        default:
            elog(ERROR, "unrecognized oprkind: %d", operform->oprkind);
            p_result = NULL;
            break;
    }

    // If parser resolves to the same operator, no schema qualification needed
    if (p_result != NULL && oprid(p_result) == operid) {
        nspname = NULL;
    } else {
        // Schema qualification needed - use OPERATOR() syntax
        nspname = get_namespace_name_or_temp(operform->oprnamespace);
        appendStringInfo(&buf, "OPERATOR(%s.", quote_identifier(nspname));
    }

    // Add operator name
    appendStringInfoString(&buf, oprname);

    // Close OPERATOR() if schema-qualified
    if (nspname)
        appendStringInfoChar(&buf, ')');

    // Clean up cache references
    if (p_result != NULL)
        ReleaseSysCache(p_result);
    ReleaseSysCache(opertup);

    return buf.data;
}
```