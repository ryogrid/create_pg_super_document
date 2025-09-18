# match_boolean_partition_clause

## Location
src/backend/partitioning/partprune.c: 3673 - 3759

## Overview
Matches boolean clauses against a partition key to determine if they can be used for partition pruning, handling various forms of boolean tests and expressions.

## Definition
```c
static PartClauseMatchStatus
match_boolean_partition_clause(Oid partopfamily, Expr *clause, Expr *partkey,
                              Expr **outconst, bool *notclause)
```

## Detailed Description
This function analyzes boolean clauses to determine if they can be matched against a partition key for pruning purposes. It handles two main categories of boolean expressions:

1. **BooleanTest nodes**: Handles explicit boolean tests like "IS TRUE", "IS FALSE", "IS UNKNOWN" and their negated forms ("IS NOT TRUE", etc.)

2. **Direct boolean expressions**: Handles direct boolean expressions and their negations

The function validates that the partition operator family is a built-in boolean operator family before proceeding with matching. It sets output parameters to indicate the equivalent constant value and whether the clause was in negated form.

## Parameters / Member Variables
- `partopfamily`: OID of the partition operator family (must be a built-in boolean opfamily)
- `clause`: The boolean expression clause to be matched
- `partkey`: The partition key expression to match against
- `outconst`: Output parameter set to a Const node containing the equivalent boolean value
- `notclause`: Output parameter indicating if the clause was in negated form

## Dependencies
- Functions called/Symbols referenced:
  - IsBuiltinBooleanOpfamily
  - IsA
  - [equal](../e/equal.md)
  - [makeBoolConst](makeBoolConst.md)
  - [is_notclause](../i/is_notclause.md)
  - [get_notclausearg](../g/get_notclausearg.md)
  - [negate_clause](../n/negate_clause.md)
- Called from (representative examples):
  - [match_clause_to_partition_key](match_clause_to_partition_key.md)

## Notes and Other Information
- Returns PARTCLAUSE_MATCH_CLAUSE for "IS [NOT] (TRUE|FALSE)" clauses
- Returns PARTCLAUSE_MATCH_NULLNESS for "IS [NOT] UNKNOWN" clauses  
- Returns PARTCLAUSE_UNSUPPORTED for unsupported clause types
- Returns PARTCLAUSE_NOMATCH for supported clauses that don't match the partition key
- Handles RelabelType nodes by unwrapping them to access the underlying expression
- Only works with built-in boolean operator families since partitioning currently only supports built-in access methods
- Located in src/backend/partitioning/partprune.c:3673-3759