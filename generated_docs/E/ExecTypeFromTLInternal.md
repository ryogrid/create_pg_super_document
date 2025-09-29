# ExecTypeFromTLInternal

## Location
[src/backend/executor/execTuples.c:2043-2083](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execTuples.c#L2043-L2083)

## Overview
Internal implementation function that creates a tuple descriptor from a target list, with optional filtering of resjunk columns.

## Definition
```c
static TupleDesc ExecTypeFromTLInternal(List *targetList, bool skipjunk)
```

## Detailed Description
This static function serves as the core implementation for both ExecTypeFromTL and ExecCleanTypeFromTL, providing the actual logic for creating tuple descriptors from target lists. The function performs the following steps:

1. **Length calculation**: Determines the appropriate length for the tuple descriptor based on whether resjunk columns should be included or skipped
2. **Template creation**: Creates an empty template tuple descriptor with the calculated length
3. **Attribute population**: Iterates through the target list, populating each attribute in the descriptor with:
   - Column name (from TargetEntry.resname)
   - Data type (from expression type analysis)
   - Type modifier (from expression type modifier analysis)  
   - Collation information (from expression collation analysis)

The skipjunk parameter controls whether resjunk columns are included in the final descriptor. When true, resjunk entries are skipped and not included in the result. When false, all target list entries are processed.

## Parameters / Member Variables
- `targetList`: A List of TargetEntry nodes representing the columns to include in the tuple descriptor
- `skipjunk`: Boolean flag indicating whether to skip resjunk columns (true) or include them (false)

## Dependencies
- Functions called/Symbols referenced:
  - [ExecCleanTargetListLength](ExecCleanTargetListLength.md) (for counting non-junk entries)
  - [ExecTargetListLength](ExecTargetListLength.md) (for counting all entries)
  - [CreateTemplateTupleDesc](../C/CreateTemplateTupleDesc.md) (for creating the empty descriptor template)
  - [TupleDescInitEntry](../T/TupleDescInitEntry.md) (for initializing each attribute entry)
  - [TupleDescInitEntryCollation](../T/TupleDescInitEntryCollation.md) (for setting collation information)
  - [exprType](../e/exprType.md), exprTypmod, exprCollation (for extracting type information from expressions)
- Called from (representative examples):
  - [ExecTypeFromTL](ExecTypeFromTL.md) (with skipjunk=false)
  - [ExecCleanTypeFromTL](ExecCleanTypeFromTL.md) (with skipjunk=true)

## Notes and Other Information
- This is a static function, not exposed outside of execTuples.c
- The function properly handles both scenarios: including and excluding resjunk columns
- Uses PostgreSQL's expression analysis functions to extract accurate type information
- Maintains proper attribute numbering (cur_resno) even when skipping junk columns
- The resulting tuple descriptor includes complete type and collation information for each column
- Essential for creating accurate schema descriptions for query results and intermediate tuple formats

## Simplified Source

```c
// Simplified version of ExecTypeFromTLInternal
static TupleDesc ExecTypeFromTLInternal(List *targetList, bool skipjunk) {
    // Calculate descriptor length based on junk filtering preference
    int len = skipjunk ? ExecCleanTargetListLength(targetList)
                      : ExecTargetListLength(targetList);

    // Create empty tuple descriptor template
    TupleDesc typeInfo = CreateTemplateTupleDesc(len);

    int cur_resno = 1;

    // Populate descriptor with each target list entry
    foreach(l, targetList) {
        TargetEntry *tle = lfirst(l);

        // Skip junk columns if requested
        if (skipjunk && tle->resjunk)
            continue;

        // Initialize attribute with type information from expression
        TupleDescInitEntry(typeInfo, cur_resno, tle->resname,
                          exprType((Node *) tle->expr),
                          exprTypmod((Node *) tle->expr), 0);

        // Set collation information
        TupleDescInitEntryCollation(typeInfo, cur_resno,
                                   exprCollation((Node *) tle->expr));

        cur_resno++;
    }

    return typeInfo;
}
```

Key simplifications made:
- Combined length calculation logic into a single conditional expression
- Added descriptive comments for each major step
- Preserved the essential algorithm: calculate length, create template, populate attributes
- Maintained the skipjunk filtering logic and proper attribute numbering
- Kept all core function calls that perform the actual work