# pg_get_partconstrdef_string

## Location
[src/backend/utils/adt/ruleutils.c:2108-2125](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L2108-L2125)

## Overview
Returns the partition constraint as a plain C-string for the specified partition relation, formatted with a given table alias and without pretty-printing.

## Definition

```c
char *
pg_get_partconstrdef_string(Oid partitionId, char *aliasname)
```
## Detailed Description
This function provides a simplified interface to retrieve partition constraint definitions as unformatted C-strings. Unlike , this function is designed for internal use and returns a plain string without pretty-printing or indentation. It allows specifying a custom alias name for the relation, which is useful when the constraint needs to be referenced in a different context (such as in subqueries or joins). The function is commonly used in scenarios where the constraint definition needs to be embedded in larger SQL constructs or when performance is more important than readability.

## Parameters / Member Variables
- `partitionId`: Object identifier (OID) of the partition relation whose constraint definition should be retrieved
- `*aliasname`: Custom alias name to use for the relation in the generated constraint expression (can be different from the actual table name)
## Dependencies
- Functions called/Symbols referenced:
  - [get_partition_qual_relid](../g/get_partition_qual_relid.md) (retrieves the partition constraint expression from system catalogs)
  - [deparse_context_for](../d/deparse_context_for.md) (creates deparsing context with the specified alias name)
  - [deparse_expression](../d/deparse_expression.md) (converts expression tree to SQL string without pretty-printing)
- Called from (representative examples):
  - [RI_PartitionRemove_Check](../R/RI_PartitionRemove_Check.md) (in referential integrity triggers for partition constraint validation)
  - RULE_INDEXDEF_KEYS_ONLY (referenced in ruleutils.h header file)

## Notes and Other Information
- This is an internal function returning a C-string (char*) rather than a PostgreSQL Datum/text type
- No pretty-printing is applied - the output is compact and suitable for machine processing
- The alias parameter allows flexibility in how the relation is referenced in the generated constraint
- Commonly used in referential integrity checks and constraint validation scenarios
- The returned string should be freed by the caller when no longer needed
- Used when constraint expressions need to be embedded in larger SQL constructs where formatting is not important

## Simplified Source

```c
char *
pg_get_partconstrdef_string(Oid partitionId, char *aliasname)
{
    // Get the partition constraint expression
    Expr *constr_expr = get_partition_qual_relid(partitionId);

    // Create deparsing context with the specified alias
    List *context = deparse_context_for(aliasname, partitionId);

    // Convert expression to string (no pretty-printing)
    return deparse_expression((Node *) constr_expr, context, true, false);
}
```