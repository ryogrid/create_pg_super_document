# CompareOpclassOptions

## Location
src/backend/commands/indexcmds.c: 360 - 432

## Overview
Compares per-column opclass options which are represented by arrays of text[] datums, handling cases where both elements and arrays can be NULL.

## Definition
```c
static bool CompareOpclassOptions(const Datum *opts1, const Datum *opts2, int natts)
```

## Detailed Description
CompareOpclassOptions is a utility function that performs deep comparison of operator class options between two index definitions. The function handles the complexity of comparing arrays of text[] datums where both individual elements and the arrays themselves can be NULL.

The comparison is performed using binary equivalence with C collation to ensure strict matching, since the function does not make assumptions about the semantics of opclass options. This conservative approach ensures that any difference in options is detected and treated as incompatible.

The function iterates through each attribute position and compares the corresponding options:
- If both options are NULL, they are considered equal
- If one is NULL and the other is not, they are considered different
- For non-NULL options, it uses the array equality function with C collation for binary comparison

## Parameters / Member Variables
- `opts1`: First array of Datum representing opclass options for each attribute
- `opts2`: Second array of Datum representing opclass options for each attribute  
- `natts`: Number of attributes (length of the option arrays)

## Dependencies
- Functions called/Symbols referenced:
  - [fmgr_info](../f/fmgr_info.md)
  - [FunctionCall2Coll](../F/FunctionCall2Coll.md)
- Called from (representative examples):
  - [CheckIndexCompatible](CheckIndexCompatible.md)

## Notes and Other Information
- Uses C collation (C_COLLATION_OID) to enforce binary equivalence of text values
- Handles NULL values gracefully at both array and element levels
- Conservative comparison approach - any difference results in incompatibility
- Static function only used within indexcmds.c
- Essential for determining index compatibility during ALTER TABLE operations
- Located in src/backend/commands/indexcmds.c:360-432