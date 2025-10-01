# ScanKeyEntryInitialize

## Location
[src/backend/access/common/scankey.c:32-75](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/scankey.c#L32-L75)

## Overview
Initializes a scan key entry with all field values, allowing for comprehensive configuration of scan parameters including flags, strategy, and operator procedures.

## Definition

```c
void
ScanKeyEntryInitialize(ScanKey entry,
					   int flags,
					   AttrNumber attributeNumber,
					   StrategyNumber strategy,
					   Oid subtype,
					   Oid collation,
					   RegProcedure procedure,
					   Datum argument)
```
## Detailed Description
ScanKeyEntryInitialize is the most comprehensive function for initializing ScanKey entries in PostgreSQL. It provides full control over all scan key parameters, making it suitable for complex scanning scenarios where specific flags, subtypes, collations, or custom operators are required. The function handles both valid procedures and special null-search cases (SK_SEARCHNULL/SK_SEARCHNOTNULL), automatically setting up the appropriate function manager information. This function is particularly important for index scanning operations where precise control over search conditions is necessary.

## Parameters / Member Variables
- : Pointer to the ScanKey structure to be initialized
- : Control flags (e.g., SK_SEARCHNULL, SK_SEARCHNOTNULL) that modify scanning behavior
- : The column number (1-based) of the attribute being scanned
- : Strategy number indicating the type of comparison operation (e.g., equality, less-than)
- : OID of the subtype for polymorphic operators, or InvalidOid if not applicable
- : OID of the collation to use for string comparisons
- : OID of the comparison function/operator procedure to use
- : The value to compare against during scanning

## Dependencies
- Functions called/Symbols referenced:
  - RegProcedureIsValid
  - [fmgr_info](../f/fmgr_info.md)
  - MemSet
  - SK_SEARCHNULL
  - SK_SEARCHNOTNULL
- Called from (representative examples):
  - [_bt_first](../b/_bt_first.md) (B-tree index scanning)
  - [check_exclusion_or_unique_constraint](../c/check_exclusion_or_unique_constraint.md) (constraint checking)
  - [ExecIndexBuildScanKeys](../E/ExecIndexBuildScanKeys.md) (executor index scanning)
  - [get_actual_variable_range](../g/get_actual_variable_range.md) (statistics estimation)

## Notes and Other Information
- The CurrentMemoryContext at call time should be as long-lived as the ScanKey itself, as it will be used for any subsidiary info attached to the ScanKey's FmgrInfo record
- When SK_SEARCHNULL or SK_SEARCHNOTNULL flags are set, the procedure parameter can be invalid (InvalidOid)
- This is the most flexible initialization function, suitable for cases requiring full control over scan parameters
- Located at src/backend/access/common/scankey.c:32-75

## Simplified Source

```c
void
ScanKeyEntryInitialize(ScanKey entry,
                       int flags,
                       AttrNumber attributeNumber,
                       StrategyNumber strategy,
                       Oid subtype,
                       Oid collation,
                       RegProcedure procedure,
                       Datum argument)
{
    // Set basic scan key fields
    entry->sk_flags = flags;
    entry->sk_attno = attributeNumber;
    entry->sk_strategy = strategy;
    entry->sk_subtype = subtype;
    entry->sk_collation = collation;
    entry->sk_argument = argument;

    // Set up function info for comparison procedure
    if (RegProcedureIsValid(procedure)) {
        // Normal case: initialize function manager info
        fmgr_info(procedure, &entry->sk_func);
    } else {
        // Special case: null search operations don't need a procedure
        Assert(flags & (SK_SEARCHNULL | SK_SEARCHNOTNULL));
        MemSet(&entry->sk_func, 0, sizeof(entry->sk_func));
    }
}
```