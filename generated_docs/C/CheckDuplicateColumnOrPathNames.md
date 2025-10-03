# CheckDuplicateColumnOrPathNames

## Location
[src/backend/parser/parse_jsontable.c:173-215](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_jsontable.c#L173-L215)

## Overview
Recursively validates that column and path names within a JSON_TABLE specification are unique, preventing naming conflicts in the JSON table structure.

## Definition

```c
static void
CheckDuplicateColumnOrPathNames(JsonTableParseContext *cxt,
								List *columns)
```
## Detailed Description
This function performs a comprehensive check for duplicate column and path names within a JSON_TABLE specification. It recursively traverses the column list, examining both regular columns and nested path specifications. For each column or path encountered, it:

1. Checks if the name already exists in the context's pathNames list using LookupPathOrColumnName
2. Reports a DUPLICATE_ALIAS error if a duplicate is found, including the specific location in the source
3. Adds valid names to the pathNames list to track them for subsequent duplicate detection
4. Recursively processes nested columns when encountering JTC_NESTED column types

The function ensures that all column and path names within a JSON_TABLE are unique, which is required for proper SQL execution and disambiguation.

## Parameters / Member Variables
- `*cxt`: JsonTableParseContext containing parsing state and the pathNames list for duplicate detection
- `*columns`: List of JsonTableColumn nodes to check for duplicate names
## Dependencies
- Functions called/Symbols referenced:
  - [LookupPathOrColumnName](../L/LookupPathOrColumnName.md) (called twice - for path names and column names)
  - [CheckDuplicateColumnOrPathNames](CheckDuplicateColumnOrPathNames.md) (recursive call for nested columns)
  - castNode, lfirst, lappend (list manipulation functions)
  - ereport, errcode, errmsg, parser_errposition (error reporting)
- Called from (representative examples):
  - [transformJsonTable](../t/transformJsonTable.md)
  - [CheckDuplicateColumnOrPathNames](CheckDuplicateColumnOrPathNames.md) (recursively)

## Notes and Other Information
- This is a static function, only accessible within the parse_jsontable.c module
- The function handles both regular columns and nested path specifications (JTC_NESTED)
- Error reporting includes precise source location information for better user diagnostics
- The recursive nature allows handling arbitrarily nested JSON_TABLE structures
- Names are added to cxt->pathNames as they are validated, building up the complete set of used names

## Simplified Source

```c
static void
CheckDuplicateColumnOrPathNames(JsonTableParseContext *cxt,
                                List *columns)
{
    ListCell *lc1;

    foreach(lc1, columns)
    {
        JsonTableColumn *jtc = castNode(JsonTableColumn, lfirst(lc1));

        if (jtc->coltype == JTC_NESTED)
        {
            // Check nested path name for duplicates
            if (jtc->pathspec->name)
            {
                if (LookupPathOrColumnName(cxt, jtc->pathspec->name))
                    ereport(ERROR, "duplicate JSON_TABLE path name: %s");
                cxt->pathNames = lappend(cxt->pathNames, jtc->pathspec->name);
            }

            // Recursively check nested columns
            CheckDuplicateColumnOrPathNames(cxt, jtc->columns);
        }
        else
        {
            // Check regular column name for duplicates
            if (LookupPathOrColumnName(cxt, jtc->name))
                ereport(ERROR, "duplicate JSON_TABLE column name: %s");
            cxt->pathNames = lappend(cxt->pathNames, jtc->name);
        }
    }
}
```