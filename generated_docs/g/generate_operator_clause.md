# generate_operator_clause

## Location
src/backend/utils/adt/ruleutils.c: 13109 - 13148

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
  - SearchSysCache1 (system cache lookup for operator)
  - get_namespace_name (namespace name resolution)
  - quote_identifier (identifier quoting)
  - add_cast_to (type casting utility)
  - appendStringInfo/appendStringInfoString (string buffer operations)
- Called from (representative examples):
  - refresh_by_match_merge (materialized view refresh operations)
  - ri_GenerateQual (referential integrity constraint generation)

## Notes and Other Information
- Only supports binary operators (asserts oprkind == `b`)
- Always uses fully-qualified OPERATOR() syntax for reliability
- Automatically handles type casting to ensure operator resolution correctness
- Designed for internal SQL generation where precision trumps readability
- The caller must ensure leftop and rightop are suitable for casting (preferably parenthesized if complex expressions)