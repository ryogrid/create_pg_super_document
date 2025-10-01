# generate_operator_clause

## Location
[src/backend/utils/adt/ruleutils.c:13109-13148](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L13109-L13148)

## Overview
Generates a binary-operator WHERE clause for internally-generated SQL queries, ensuring precise operator resolution and type casting.

## Definition
```c
void generate_operator_clause(StringInfo buf,
                            const char *leftop, Oid leftoptype,
                            Oid opoid,
                            const char *rightop, Oid rightoptype)
```

## Detailed Description
This function constructs a binary operator expression of the form "leftop op rightop" for use in internally-generated SQL queries where precision is essential and readability is secondary. The function ensures that the parser will select the desired operator when the query is parsed by always using the OPERATOR(schema.op) syntax to avoid search-path uncertainties.

The function automatically inserts type casts when either input type does not match the expected input type of the operator, preventing ambiguous-operator resolution issues. This is crucial for maintaining correctness in automatically generated SQL queries.

The output format is: `leftop [::cast] OPERATOR(schema.op) rightop [::cast]` where casts are added only when necessary.

## Parameters / Member Variables
- `buf`: StringInfo buffer to append the generated operator clause to
- `leftop`: String representation of the left operand expression
- `leftoptype`: OID of the actual type of the left operand
- `opoid`: OID of the operator to use
- `rightop`: String representation of the right operand expression  
- `rightoptype`: OID of the actual type of the right operand

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md) (system cache lookup for operator)
  - [get_namespace_name](get_namespace_name.md) (namespace name resolution)
  - [quote_identifier](../q/quote_identifier.md) (identifier quoting)
  - [add_cast_to](../a/add_cast_to.md) (type casting utility)
  - [appendStringInfo](../a/appendStringInfo.md)/appendStringInfoString (string buffer operations)
- Called from (representative examples):
  - [refresh_by_match_merge](../r/refresh_by_match_merge.md) (materialized view refresh operations)
  - [ri_GenerateQual](../r/ri_GenerateQual.md) (referential integrity constraint generation)

## Notes and Other Information
- Only supports binary operators (asserts oprkind == `b`)
- Always uses fully-qualified OPERATOR() syntax for reliability
- Automatically handles type casting to ensure operator resolution correctness
- Designed for internal SQL generation where precision trumps readability
- The caller must ensure leftop and rightop are suitable for casting (preferably parenthesized if complex expressions)

## Simplified Source

```c
void generate_operator_clause(StringInfo buf,
                            const char *leftop, Oid leftoptype,
                            Oid opoid,
                            const char *rightop, Oid rightoptype)
{
    HeapTuple opertup;
    Form_pg_operator operform;
    char *oprname;
    char *nspname;

    // Look up the operator in the system catalog
    opertup = SearchSysCache1(OPEROID, ObjectIdGetDatum(opoid));
    if (!HeapTupleIsValid(opertup))
        elog(ERROR, "cache lookup failed for operator %u", opoid);

    operform = (Form_pg_operator) GETSTRUCT(opertup);
    Assert(operform->oprkind == 'b');  // Only binary operators supported
    oprname = NameStr(operform->oprname);

    // Get the operator's namespace name for qualification
    nspname = get_namespace_name(operform->oprnamespace);

    // Build the clause: leftop [::cast] OPERATOR(schema.op) rightop [::cast]
    appendStringInfoString(buf, leftop);
    if (leftoptype != operform->oprleft)
        add_cast_to(buf, operform->oprleft);

    appendStringInfo(buf, " OPERATOR(%s.", quote_identifier(nspname));
    appendStringInfoString(buf, oprname);
    appendStringInfo(buf, ") %s", rightop);

    if (rightoptype != operform->oprright)
        add_cast_to(buf, operform->oprright);

    ReleaseSysCache(opertup);
}
```