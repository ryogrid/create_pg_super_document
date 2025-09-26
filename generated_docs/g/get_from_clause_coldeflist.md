# get_from_clause_coldeflist

## Location
[src/backend/utils/adt/ruleutils.c:12436-12486](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L12436-L12486)

## Overview
Reconstructs a column definition list for function range table entries, including column names, types, and collations in SQL format.

## Definition
```c
static void get_from_clause_coldeflist(RangeTblFunction *rtfunc, deparse_columns *colinfo, deparse_context *context)
```

## Detailed Description
This function generates a parenthesized list of column definitions for function calls in FROM clauses. It handles the complex task of reconstructing the original column definition syntax, including column names, data types with modifiers, and collation specifications.

The function operates in two modes based on the colinfo parameter:
- **With colinfo**: Uses column names from the deparse_columns structure (for top-level column alias lists)
- **Without colinfo (NULL)**: Uses original column names from rtfunc->funccolnames (for embedded ROWS FROM() syntax)

For each column, the function:
1. Extracts type information (OID, type modifier, collation)
2. Determines the appropriate column name based on the mode
3. Formats the column as "name type" with proper identifier quoting
4. Adds COLLATE clause if the collation differs from the type's default
5. Separates multiple columns with commas

The output format is: `(column1 type1, column2 type2 COLLATE collation, ...)`

## Parameters / Member Variables
- `rtfunc`: Range table function containing column type and name information
- `colinfo`: Deparse columns structure (NULL to use original function column names)
- `context`: Deparse context containing the output buffer

## Dependencies
- Functions called/Symbols referenced:
  - forfour (macro for iterating over four lists simultaneously)
  - lfirst_oid, lfirst_int (list access macros)
  - [quote_identifier](../q/quote_identifier.md)
  - [format_type_with_typemod](../f/format_type_with_typemod.md)
  - [get_typcollation](get_typcollation.md)
  - [generate_collation_name](generate_collation_name.md)
  - [appendStringInfo](../a/appendStringInfo.md), appendStringInfoChar, appendStringInfoString
- Called from (representative examples):
  - [get_from_clause_item](get_from_clause_item.md) (for function RTEs with column definitions)

## Notes and Other Information
- Critical for reconstructing ROWS FROM() syntax and function column definitions
- Handles both simple function calls and complex multi-function scenarios
- Properly manages collation specifications that differ from type defaults
- Uses forfour macro to efficiently iterate over parallel lists of column attributes
- Assumes no dropped columns exist in the function column lists
- Essential for maintaining SQL standard compliance in function call deparsing