# identify_join_columns

## Location
[src/backend/utils/adt/ruleutils.c:4878-4945](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L4878-L4945)

## Overview
Analyzes a join expression to determine the source columns and their mappings, populating the join-specific fields of a deparse_columns structure used for SQL rule decompilation.

## Definition
```c
static void identify_join_columns(JoinExpr *j, RangeTblEntry *jrte, deparse_columns *colinfo)
```

## Detailed Description
This function is a crucial component of PostgreSQL's rule decompilation system that analyzes join expressions to understand column mappings between the joined relations. It extracts the range table indexes of the left and right child relations from the join expression, then processes the join's column mapping information to populate arrays that track which output columns come from which input relations.

The function handles the complex mapping of columns in joins, particularly dealing with merged columns from USING clauses. It processes the joinleftcols and joinrightcols lists from the RangeTblEntry to create leftattnos and rightattnos arrays that map each output column position to the corresponding attribute numbers in the left and right input relations.

The function distinguishes between merged columns (those combined due to USING clauses) and unmerged columns, ensuring proper column attribution during rule decompilation. It validates that child relations will be processed before the join in the decompilation pass through assertion checks.

## Parameters / Member Variables
- `j`: Pointer to the JoinExpr node representing the join operation being analyzed
- `jrte`: Pointer to the RangeTblEntry for this join, containing join column mapping information
- `colinfo`: Pointer to deparse_columns structure to be populated with join column mapping data

## Dependencies
- Functions called/Symbols referenced:
  - [JoinExpr](../J/JoinExpr.md) (parse node structure)
  - [RangeTblEntry](../R/RangeTblEntry.md) (range table entry structure)  
  - deparse_columns (column information structure)
  - [RangeTblRef](../R/RangeTblRef.md) (range table reference node)
  - nodeTag (node type identification macro)
  - lfirst_int (list cell integer extraction macro)
- Called from (representative examples):
  - [set_using_names](../s/set_using_names.md) (at line 4172)

## Notes and Other Information
- This is a static function, only accessible within ruleutils.c
- The function assumes that child relations in a join tree are processed before parent joins (enforced by assertions)
- Handles both simple range table references and nested join expressions as join arguments
- Allocates memory for leftattnos and rightattnos arrays using palloc0 for zero-initialization
- Critical for proper column name resolution in complex join expressions during rule decompilation
- The function works in conjunction with set_using_names to complete the join column name resolution process
- Merged columns from USING clauses are treated specially and appear first in the output column list

## Simplified Source

```c
static void identify_join_columns(JoinExpr *j, RangeTblEntry *jrte,
                                 deparse_columns *colinfo) {
    int numjoincols;
    int jcolno;
    int rcolno;
    ListCell *lc;

    // Extract left child RT index
    if (IsA(j->larg, RangeTblRef))
        colinfo->leftrti = ((RangeTblRef *) j->larg)->rtindex;
    else if (IsA(j->larg, JoinExpr))
        colinfo->leftrti = ((JoinExpr *) j->larg)->rtindex;
    else
        elog(ERROR, "unrecognized node type in jointree: %d", (int) nodeTag(j->larg));

    // Extract right child RT index
    if (IsA(j->rarg, RangeTblRef))
        colinfo->rightrti = ((RangeTblRef *) j->rarg)->rtindex;
    else if (IsA(j->rarg, JoinExpr))
        colinfo->rightrti = ((JoinExpr *) j->rarg)->rtindex;
    else
        elog(ERROR, "unrecognized node type in jointree: %d", (int) nodeTag(j->rarg));

    // Validate processing order
    Assert(colinfo->leftrti < j->rtindex);
    Assert(colinfo->rightrti < j->rtindex);

    // Initialize column mapping arrays
    numjoincols = list_length(jrte->joinaliasvars);
    Assert(numjoincols == list_length(jrte->eref->colnames));
    colinfo->leftattnos = (int *) palloc0(numjoincols * sizeof(int));
    colinfo->rightattnos = (int *) palloc0(numjoincols * sizeof(int));

    // Process left column mappings
    jcolno = 0;
    foreach(lc, jrte->joinleftcols) {
        int leftattno = lfirst_int(lc);
        colinfo->leftattnos[jcolno++] = leftattno;
    }

    // Process right column mappings (handling merged columns specially)
    rcolno = 0;
    foreach(lc, jrte->joinrightcols) {
        int rightattno = lfirst_int(lc);

        if (rcolno < jrte->joinmergedcols) {
            // Merged column from USING clause
            colinfo->rightattnos[rcolno] = rightattno;
        } else {
            // Unmerged column
            colinfo->rightattnos[jcolno++] = rightattno;
        }
        rcolno++;
    }
    Assert(jcolno == numjoincols);
}
```