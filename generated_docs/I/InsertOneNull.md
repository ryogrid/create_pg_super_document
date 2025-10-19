# InsertOneNull

## Location
[src/backend/bootstrap/bootstrap.c:664-681](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/bootstrap/bootstrap.c#L664-L681)

## Overview
InsertOneNull marks a specific column position as NULL during bootstrap tuple construction, with validation against NOT NULL constraints.

## Definition
```c
void InsertOneNull(int i)
```

## Detailed Description
InsertOneNull is a bootstrap function that handles NULL value insertion during PostgreSQL system initialization. The function sets the appropriate NULL marker for a specified column position while enforcing NOT NULL constraints defined for the column.

The function performs several validation and setup steps:
1. Validates that the column index is within acceptable bounds (0 to MAXATTR-1)
2. Checks if the target column has a NOT NULL constraint defined
3. If a NOT NULL constraint exists, raises an ERROR with details about the violation
4. If the NULL is allowed, sets the column value to a NULL pointer datum
5. Marks the corresponding position in the Nulls array as true to indicate NULL value

This function works alongside InsertOneValue and InsertOneTuple to construct complete rows during bootstrap, ensuring data integrity by respecting column constraints even during system initialization.

## Parameters / Member Variables
- `i`: Zero-based column index where the NULL value should be set (must be 0 <= i < MAXATTR)

## Dependencies
- Functions called/Symbols referenced:
  - TupleDescAttr (accesses attribute information from tuple descriptor)
  - NameStr (extracts string from Name structure)
  - RelationGetRelationName (gets relation name for error reporting)
  - [PointerGetDatum](../P/PointerGetDatum.md) (converts NULL pointer to Datum)
  - elog (error and debug logging)
  - Assert (bounds checking)
  - MAXATTR (maximum number of attributes constant)
  - DEBUG4 (debug logging level)
  - ERROR (error logging level)
- Called from (representative examples):
  - Bootstrap parser when processing NULL column values

## Notes and Other Information
- This function is part of the bootstrap process and only used during PostgreSQL system initialization
- Enforces NOT NULL constraints even during bootstrap, maintaining data integrity from the start
- Sets both the values array entry to a NULL datum and the Nulls array entry to true
- Error messages include both column name and relation name for clear identification of constraint violations
- Uses Assert for bounds checking to catch programming errors during development
- The NULL marker set here is later used by InsertOneTuple when constructing the final heap tuple
- Operates on the assumption that a relation is currently open (boot_reldesc is valid)
- Debug logging reports which column is being set to NULL for troubleshooting purposes

## Simplified Source

```c
void InsertOneNull(int i)
{
    // Validate column index bounds
    Assert(i >= 0 && i < MAXATTR);

    // Check NOT NULL constraint
    if (TupleDescAttr(boot_reldesc->rd_att, i)->attnotnull)
        elog(ERROR, "NULL value specified for not-null column \"%s\" of relation \"%s\"",
             NameStr(TupleDescAttr(boot_reldesc->rd_att, i)->attname),
             RelationGetRelationName(boot_reldesc));

    // Set NULL value and marker
    values[i] = PointerGetDatum(NULL);
    Nulls[i] = true;
}
```