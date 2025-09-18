# OperatorUpd

## Location
src/backend/catalog/pg_operator.c: 684 - 852

## Overview
Updates the commutator and negator back-reference fields in related operators to maintain bidirectional consistency when creating or dropping operators with mutual relationships.

## Definition
```c
void OperatorUpd(Oid baseId, Oid commId, Oid negId, bool isDelete)
```

## Detailed Description
This function maintains the integrity of bidirectional operator relationships in PostgreSQL's catalog. When an operator declares another operator as its commutator or negator, the referenced operator must have its corresponding field updated to point back to the first operator.

The function handles two main scenarios:

1. **Creation Mode (isDelete = false)**: Sets the oprcom field of the commutator operator and the oprnegate field of the negator operator to point back to the base operator, establishing bidirectional relationships.

2. **Deletion Mode (isDelete = true)**: Clears the back-reference fields in related operators to prevent dangling OID references when the base operator is being dropped.

The function performs thorough validation to detect and prevent inconsistent operator relationships, such as when an operator is already the commutator/negator of a different operator. It uses CommandCounterIncrement to ensure visibility of updates between operations, which is crucial when an operator is its own commutator or when updating multiple related operators.

## Parameters / Member Variables
- `baseId`: OID of the base operator being created or deleted
- `commId`: OID of the commutator operator to update (InvalidOid if none)
- `negId`: OID of the negator operator to update (InvalidOid if none)
- `isDelete`: Boolean indicating whether this is for operator creation (false) or deletion (true)

## Dependencies
- Functions called/Symbols referenced:
  - CommandCounterIncrement (ensures transaction visibility)
  - table_open/table_close (catalog relation access)
  - SearchSysCacheCopy1 (retrieves operator tuples)
  - CatalogTupleUpdate (updates catalog tuples)
  - get_opname (gets operator name for error messages)
  - Form_pg_operator (operator tuple structure)
- Called from (representative examples):
  - OperatorCreate (at src/backend/catalog/pg_operator.c:534)
  - RemoveOperatorById (at src/backend/commands/operatorcmds.c:434)
  - AlterOperator (at src/backend/commands/operatorcmds.c:694)

## Notes and Other Information
- Uses RowExclusiveLock when opening the pg_operator catalog to ensure exclusive update access
- Performs careful validation to prevent corruption of operator relationships
- Handles edge cases like self-commutative operators and self-negating operators
- CommandCounterIncrement calls are strategically placed to handle cases where operators reference themselves or each other
- Essential for maintaining referential integrity in the operator system, preventing orphaned references after operator deletion