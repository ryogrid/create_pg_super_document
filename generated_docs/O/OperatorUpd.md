# OperatorUpd

## Location
[src/backend/catalog/pg_operator.c:684-852](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_operator.c#L684-L852)

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
  - [CommandCounterIncrement](../C/CommandCounterIncrement.md) (ensures transaction visibility)
  - [table_open](../t/table_open.md)/table_close (catalog relation access)
  - SearchSysCacheCopy1 (retrieves operator tuples)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md) (updates catalog tuples)
  - [get_opname](../g/get_opname.md) (gets operator name for error messages)
  - Form_pg_operator (operator tuple structure)
- Called from (representative examples):
  - [OperatorCreate](OperatorCreate.md) (at src/backend/catalog/pg_operator.c:534)
  - [RemoveOperatorById](../R/RemoveOperatorById.md) (at src/backend/commands/operatorcmds.c:434)
  - [AlterOperator](../A/AlterOperator.md) (at src/backend/commands/operatorcmds.c:694)

## Notes and Other Information
- Uses RowExclusiveLock when opening the pg_operator catalog to ensure exclusive update access
- Performs careful validation to prevent corruption of operator relationships
- Handles edge cases like self-commutative operators and self-negating operators
- [CommandCounterIncrement](../C/CommandCounterIncrement.md) calls are strategically placed to handle cases where operators reference themselves or each other
- Essential for maintaining referential integrity in the operator system, preventing orphaned references after operator deletion

## Simplified Source

```c
void OperatorUpd(Oid baseId, Oid commId, Oid negId, bool isDelete) {
    Relation pg_operator_desc;
    HeapTuple tup;

    // Increment command counter for visibility when creating operators
    if (!isDelete)
        CommandCounterIncrement();

    // Open operator catalog for updates
    pg_operator_desc = table_open(OperatorRelationId, RowExclusiveLock);

    // Update commutator operator if specified
    if (OidIsValid(commId)) {
        tup = SearchSysCacheCopy1(OPEROID, ObjectIdGetDatum(commId));

        if (HeapTupleIsValid(tup)) {
            Form_pg_operator t = (Form_pg_operator) GETSTRUCT(tup);
            bool update_commutator = false;

            if (isDelete && OidIsValid(t->oprcom)) {
                // Clear back-reference for deletion
                t->oprcom = InvalidOid;
                update_commutator = true;
            }
            else if (!isDelete && t->oprcom != baseId) {
                // Check for conflicts with existing references
                if (OidIsValid(t->oprcom)) {
                    ereport(ERROR,
                           (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                            errmsg("commutator operator %s is already the commutator of another operator",
                                   NameStr(t->oprname))));
                }

                // Set back-reference to base operator
                t->oprcom = baseId;
                update_commutator = true;
            }

            // Apply updates and ensure visibility
            if (update_commutator) {
                CatalogTupleUpdate(pg_operator_desc, &tup->t_self, tup);
                CommandCounterIncrement();
            }
        }
    }

    // Update negator operator if specified (similar logic)
    if (OidIsValid(negId)) {
        tup = SearchSysCacheCopy1(OPEROID, ObjectIdGetDatum(negId));

        if (HeapTupleIsValid(tup)) {
            Form_pg_operator t = (Form_pg_operator) GETSTRUCT(tup);
            bool update_negator = false;

            if (isDelete && OidIsValid(t->oprnegate)) {
                // Clear back-reference for deletion
                t->oprnegate = InvalidOid;
                update_negator = true;
            }
            else if (!isDelete && t->oprnegate != baseId) {
                // Check for conflicts with existing references
                if (OidIsValid(t->oprnegate)) {
                    ereport(ERROR,
                           (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                            errmsg("negator operator %s is already the negator of another operator",
                                   NameStr(t->oprname))));
                }

                // Set back-reference to base operator
                t->oprnegate = baseId;
                update_negator = true;
            }

            // Apply updates and ensure visibility for deletion case
            if (update_negator) {
                CatalogTupleUpdate(pg_operator_desc, &tup->t_self, tup);
                if (isDelete)
                    CommandCounterIncrement();
            }
        }
    }

    // Close catalog relation
    table_close(pg_operator_desc, RowExclusiveLock);
}
```