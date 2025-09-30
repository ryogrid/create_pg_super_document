# CheckVarSlotCompatibility

## Location
[src/backend/executor/execExprInterp.c:1986-2036](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExprInterp.c#L1986-L2036)

## Overview
Validates that a specific attribute in a tuple slot is compatible with a variable reference from a compiled expression, checking for schema changes like dropped columns or type mismatches.

## Definition
```c
static void CheckVarSlotCompatibility(TupleTableSlot *slot, int attnum, Oid vartype)
```

## Detailed Description
This function performs detailed schema compatibility validation for individual variable references in PostgreSQL expressions. It serves as a defensive mechanism against schema changes that could occur between expression compilation and execution.

The function performs three critical validations:

1. **Attribute Existence**: Verifies the requested attribute number doesn't exceed the tuple descriptor's column count
2. **Dropped Column Detection**: Checks if the attribute has been dropped using the `attisdropped` flag
3. **Type Compatibility**: Ensures the attribute's current type matches the expected type from the compiled expression

Key design considerations:
- **System attributes exemption**: Attributes with attnum ≤ 0 (system attributes like ctid, xmin, etc.) are skipped because their types never change
- **Typmod limitation**: The function intentionally doesn't check typmod (type modifiers) because many expression nodes don't carry accurate typmod information, and PostgreSQL's design minimizes critical dependencies on typmod values
- **Error reporting**: Provides detailed error messages with both expected and actual types to aid in debugging schema mismatches

The function is essential for handling scenarios where:
- DDL operations modify table schemas after plan caching
- Concurrent schema changes occur during long-running transactions
- Views or rules reference altered underlying tables
- Prepared statements are reused after schema modifications

## Parameters / Member Variables
- `slot`: Pointer to TupleTableSlot containing the tuple descriptor to validate against
- `attnum`: Attribute number to check (1-based indexing, ≤0 for system attributes)
- `vartype`: Expected OID of the attribute's data type from the compiled expression

## Dependencies
- Functions called/Symbols referenced:
  - TupleDescAttr (macro to access attribute metadata)
  - elog/ereport (error reporting functions)
  - [format_type_be](../f/format_type_be.md) (formats type OIDs for error messages)
- Called from:
  - [CheckExprStillValid](CheckExprStillValid.md) (for each variable operation in expressions)

## Notes and Other Information
- Declared static as it's an internal implementation detail of the expression interpreter
- Only validates user attributes (attnum > 0), system attributes are assumed stable
- Intentionally doesn't validate typmod due to expression tree limitations and minimal impact
- Error codes used: ERRCODE_UNDEFINED_COLUMN for dropped attributes, ERRCODE_DATATYPE_MISMATCH for type changes
- Part of PostgreSQL's defensive programming approach to handle schema evolution gracefully
- The validation occurs only once per expression evaluation, not on every tuple access
- Critical for maintaining data integrity and preventing crashes when schemas change unexpectedly

## Simplified Source

```c
static void
CheckVarSlotCompatibility(TupleTableSlot *slot, int attnum, Oid vartype)
{
    // System attributes (attnum <= 0) never change, so skip validation
    if (attnum <= 0)
        return;

    TupleDesc slot_tupdesc = slot->tts_tupleDescriptor;
    Form_pg_attribute attr;

    // Check that attribute number is within valid range
    if (attnum > slot_tupdesc->natts)
        elog(ERROR, "attribute number %d exceeds number of columns %d",
             attnum, slot_tupdesc->natts);

    attr = TupleDescAttr(slot_tupdesc, attnum - 1);

    // Check if the attribute has been dropped
    if (attr->attisdropped)
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_COLUMN),
                 errmsg("attribute %d of type %s has been dropped",
                        attnum, format_type_be(slot_tupdesc->tdtypeid))));

    // Check for type mismatch between expected and actual types
    if (vartype != attr->atttypid)
        ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                 errmsg("attribute %d of type %s has wrong type",
                        attnum, format_type_be(slot_tupdesc->tdtypeid)),
                 errdetail("Table has type %s, but query expects %s.",
                           format_type_be(attr->atttypid),
                           format_type_be(vartype))));
}
```