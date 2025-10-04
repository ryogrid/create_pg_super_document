# spgadjustmembers

## Location
[src/backend/access/spgist/spgvalidate.c:332-392](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgvalidate.c#L332-L392)

## Overview
The `spgadjustmembers` function serves as a prechecking function for adding operators and support functions to an SP-GiST operator family, setting appropriate dependency relationships for catalog management.

## Definition
```c
void spgadjustmembers(Oid opfamilyoid, Oid opclassoid, List *operators, List *functions)
```

## Detailed Description
The `spgadjustmembers` function configures dependency relationships when operators and functions are added to an SP-GiST operator family. It establishes whether each member should have hard or soft dependencies and determines the appropriate reference object (operator class vs operator family).

For operators, the function sets all dependencies as soft dependencies pointing to the operator family, since operator membership depends entirely on what the support functions determine and can be altered dynamically.

For support functions, the function distinguishes between required and optional functions:
- Required functions (config, choose, picksplit, inner_consistent, leaf_consistent) get hard dependencies
- Optional functions (compress, options) get soft family dependencies

The dependency management ensures proper catalog behavior during DROP operations and maintains referential integrity in the PostgreSQL system catalogs.

## Parameters / Member Variables
- `opfamilyoid`: The OID of the SP-GiST operator family being modified
- `opclassoid`: The OID of the operator class (may be used for dependency targeting)
- `operators`: List of OpFamilyMember structures representing operators to be added
- `functions`: List of OpFamilyMember structures representing support functions to be added

## Dependencies
- Functions called/Symbols referenced:
  - [OpFamilyMember](../O/OpFamilyMember.md) (structure access)
  - SPGIST_CONFIG_PROC
  - SPGIST_CHOOSE_PROC  
  - SPGIST_PICKSPLIT_PROC
  - SPGIST_INNER_CONSISTENT_PROC
  - SPGIST_LEAF_CONSISTENT_PROC
  - SPGIST_COMPRESS_PROC
  - SPGIST_OPTIONS_PROC
  - ereport
- Called from (representative examples):
  - [spghandler](spghandler.md) (in spgutils.c:83)

## Notes and Other Information
- This function does not return a value (void return type)
- Sets `ref_is_hard = false` for all operators, making them soft dependencies on the operator family
- Required support functions (numbers 1-5) get hard dependencies, while optional functions (6-7) get soft family dependencies
- The function handles both CREATE OPERATOR CLASS and ALTER OPERATOR FAMILY scenarios
- Invalid support function numbers trigger an ERROR with ERRCODE_INVALID_OBJECT_DEFINITION
- SP-GiST operator classes generally do not share operator families, simplifying dependency management
- The dependency structure affects what happens when objects are dropped from the system

## Simplified Source

```c
void
spgadjustmembers(Oid opfamilyoid, Oid opclassoid, List *operators, List *functions)
{
    ListCell *lc;

    // Configure operator dependencies
    // All operators get soft dependencies on the operator family
    foreach(lc, operators) {
        OpFamilyMember *op = (OpFamilyMember *) lfirst(lc);

        op->ref_is_hard = false;      // Soft dependency
        op->ref_is_family = true;     // Points to family
        op->refobjid = opfamilyoid;   // Target is operator family
    }

    // Configure support function dependencies
    foreach(lc, functions) {
        OpFamilyMember *op = (OpFamilyMember *) lfirst(lc);

        switch (op->number) {
            case SPGIST_CONFIG_PROC:
            case SPGIST_CHOOSE_PROC:
            case SPGIST_PICKSPLIT_PROC:
            case SPGIST_INNER_CONSISTENT_PROC:
            case SPGIST_LEAF_CONSISTENT_PROC:
                // Required support functions get hard dependencies
                op->ref_is_hard = true;
                break;

            case SPGIST_COMPRESS_PROC:
            case SPGIST_OPTIONS_PROC:
                // Optional functions get soft family dependencies
                op->ref_is_hard = false;
                op->ref_is_family = true;
                op->refobjid = opfamilyoid;
                break;

            default:
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                         errmsg("support function number %d is invalid for access method %s",
                               op->number, "spgist")));
                break;
        }
    }
}
```