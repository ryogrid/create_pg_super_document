# ScanKeyInit

## Location
[src/backend/access/common/scankey.c:76-100](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/scankey.c#L76-L100)

## Overview
A simplified version of ScanKeyEntryInitialize that provides default values for common scan key initialization scenarios, particularly optimized for hardwired system catalog lookups.

## Definition

```c
void
ScanKeyInit(ScanKey entry,
			AttrNumber attributeNumber,
			StrategyNumber strategy,
			RegProcedure procedure,
			Datum argument)
```
## Detailed Description
ScanKeyInit is a streamlined function designed for common scan key initialization needs, especially for hardwired lookups in system catalogs. It assumes sensible defaults: flags are set to zero, subtype is InvalidOid, and collation is set to C_COLLATION_OID. This function cannot handle NULL arguments, unary operators, or non-default operators, but these features are rarely needed for most system catalog searches. The use of C_COLLATION_OID as the default collation is appropriate for all collation-aware columns in system catalogs and is safely ignored for non-collatable column types.

## Parameters / Member Variables
- : Pointer to the ScanKey structure to be initialized
- : The column number (1-based) of the attribute being scanned
- : Strategy number indicating the type of comparison operation
- : OID of the comparison function/operator procedure to use
- : The value to compare against during scanning

## Dependencies
- Functions called/Symbols referenced:
  - [fmgr_info](../f/fmgr_info.md)
  - C_COLLATION_OID (constant)
  - InvalidOid (constant)
- Called from (representative examples):
  - Currently no direct references found in the analyzed codebase

## Notes and Other Information
- This is the recommended version for hardwired lookups in system catalogs due to its simplicity
- Automatically sets flags to 0, subtype to InvalidOid, and collation to C_COLLATION_OID
- Cannot handle NULL search conditions (SK_SEARCHNULL/SK_SEARCHNOTNULL)
- CurrentMemoryContext at call time should be as long-lived as the ScanKey itself
- The C_COLLATION_OID default is correct for system catalog columns and safely ignored for non-collatable types
- Located at src/backend/access/common/scankey.c:76-100

## Simplified Source

```c
void ScanKeyInit(ScanKey entry, AttrNumber attributeNumber,
                 StrategyNumber strategy, RegProcedure procedure,
                 Datum argument) {
    // Initialize scan key with sensible defaults for system catalog lookups
    entry->sk_flags = 0;
    entry->sk_attno = attributeNumber;
    entry->sk_strategy = strategy;
    entry->sk_subtype = InvalidOid;
    entry->sk_collation = C_COLLATION_OID;  // Safe default for all system catalogs
    entry->sk_argument = argument;

    // Setup function manager info for the comparison procedure
    fmgr_info(procedure, &entry->sk_func);
}
```