# check_safe_enum_use

## Location
[src/backend/utils/adt/enum.c:63-108](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/enum.c#L63-L108)

## Overview
Ensures that uncommitted enum values are not used in SQL operations to prevent index corruption during transaction rollbacks.

## Definition

```c
static void
check_safe_enum_use(HeapTuple enumval_tup)
```
## Detailed Description
This function implements a safety mechanism to prevent the use of uncommitted enum values in SQL operations. The primary concern is preventing index corruption that could occur if an uncommitted enum value gets into an index and the transaction is later rolled back. Since enum value comparisons rely on the underlying pg_enum catalog entry, removing the heap entry alone is insufficient to guarantee index integrity.

The function allows several exceptions:
- Values that are already committed (fast path via hint bits)
- Values belonging to enum types created in the same transaction
- Values created during CREATE TYPE AS ENUM operations
- Values added via ALTER TYPE ADD VALUE when the enum type was created in the current transaction

The implementation checks transaction states and maintains a list of uncommitted enum values to enforce these rules centrally.

## Parameters / Member Variables
- `enumval_tup`: HeapTuple representing the pg_enum catalog entry to validate for safe usage
## Dependencies
- Functions called/Symbols referenced:
  - Form_pg_enum
  - HeapTupleHeaderXminCommitted
  - HeapTupleHeaderGetXmin
  - [TransactionIdIsInProgress](../T/TransactionIdIsInProgress.md)
  - [TransactionIdDidCommit](../T/TransactionIdDidCommit.md)
  - [EnumUncommitted](../E/EnumUncommitted.md)
- Called from (representative examples):
  - [enum_in](../e/enum_in.md)
  - [enum_recv](../e/enum_recv.md)
  - [enum_endpoint](../e/enum_endpoint.md)
  - [enum_range_internal](../e/enum_range_internal.md)

## Notes and Other Information
- This is a static function internal to enum.c, serving as a central validation point
- The function takes a conservative approach, being "stronger than necessary" to ensure safety
- Currently only handles ALTER TYPE ADD VALUE at the outermost transaction level
- Provides specific error messages with hints when unsafe usage is detected
- Critical for maintaining database integrity during concurrent enum modifications

## Simplified Source

```c
static void check_safe_enum_use(HeapTuple enumval_tup) {
    Form_pg_enum enum_data = (Form_pg_enum) GETSTRUCT(enumval_tup);

    // Fast path: if tuple is marked as committed, it's safe to use
    if (HeapTupleHeaderXminCommitted(enumval_tup->t_data))
        return;

    // Check if the creating transaction has committed
    TransactionId xmin = HeapTupleHeaderGetXmin(enumval_tup->t_data);
    if (!TransactionIdIsInProgress(xmin) && TransactionIdDidCommit(xmin))
        return;

    // Check if enum value is in the uncommitted list
    if (!EnumUncommitted(enum_data->oid))
        return;  // Not uncommitted, so it's safe

    // Enum value is uncommitted and unsafe to use
    ereport(ERROR,
            (errcode(ERRCODE_UNSAFE_NEW_ENUM_VALUE_USAGE),
             errmsg("unsafe use of new value \"%s\" of enum type %s",
                    NameStr(enum_data->enumlabel),
                    format_type_be(enum_data->enumtypid)),
             errhint("New enum values must be committed before they can be used.")));
}
```