# BuildDescFromLists

## Location
src/backend/access/common/tupdesc.c: 858 - 898

## Overview
Builds a TupleDesc from separate lists of column names, data types, type modifiers, and collations, primarily for functions returning RECORD type.

## Definition
```c
TupleDesc BuildDescFromLists(const List *names, 
                            const List *types, 
                            const List *typmods, 
                            const List *collations)
```

## Detailed Description
BuildDescFromLists constructs a complete tuple descriptor by combining four parallel lists containing the essential information for each column: names, data types, type modifiers, and collations. This function is specifically designed for scenarios where functions return RECORD types and the tuple structure needs to be dynamically determined at runtime.

The function creates a new tuple descriptor using CreateTemplateTupleDesc and then iterates through all four lists simultaneously using the forfour macro. For each attribute, it calls TupleDescInitEntry to set up the basic type information, followed by TupleDescInitEntryCollation to assign the appropriate collation.

No constraints are generated for the resulting tuple descriptor, making it suitable for temporary or dynamic record structures where constraints are not needed or will be applied elsewhere.

## Parameters / Member Variables
- `names`: List of String nodes containing the column names
- `types`: List of Oid values representing the PostgreSQL data type OIDs for each column
- `typmods`: List of int32 values containing type modifier information for each column
- `collations`: List of Oid values representing the collation OIDs for each column

## Dependencies
- Functions called/Symbols referenced:
  - [CreateTemplateTupleDesc](../C/CreateTemplateTupleDesc.md) (creates empty tuple descriptor)
  - forfour (macro for parallel iteration over four lists)
  - [TupleDescInitEntry](../T/TupleDescInitEntry.md) (initializes individual attribute entries)
  - [TupleDescInitEntryCollation](../T/TupleDescInitEntryCollation.md) (sets collation for attributes)
  - list_length (gets list length)
  - strVal, lfirst_oid, lfirst_int (list access macros)
- Called from (representative examples):
  - [ExecInitFunctionScan](../E/ExecInitFunctionScan.md) (function scan initialization)
  - [ExecInitTableFuncScan](../E/ExecInitTableFuncScan.md) (table function scan initialization)
  - [inline_set_returning_function](../i/inline_set_returning_function.md) (optimizer inlining)

## Notes and Other Information
- All four input lists must have the same length - the function asserts this condition
- No constraints are generated in the resulting tuple descriptor
- Primarily designed for RECORD-returning functions where the tuple structure is determined dynamically
- The function assumes all input lists contain valid, properly typed elements
- Uses parallel list iteration (forfour) to process all attributes efficiently
- Each attribute is numbered starting from 1 (PostgreSQL convention for attribute numbers)