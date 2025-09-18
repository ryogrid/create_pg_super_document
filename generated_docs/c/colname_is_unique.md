# colname_is_unique

## Location
[src/backend/utils/adt/ruleutils.c:4766-4819](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L4766-L4819)

## Overview
Checks whether a proposed column name is distinct from all already-chosen column names in the current RTE and global naming context.

## Definition
```c
static bool colname_is_unique(const char *colname, deparse_namespace *dpns, deparse_columns *colinfo)
```

## Detailed Description
This function performs a comprehensive uniqueness check for a proposed column name by examining multiple sources of potential naming conflicts:

**Local RTE Column Name Checks:**
- Searches through colinfo->colnames[] array for already-assigned column aliases within the current RTE
- Also checks colinfo->new_colnames[] array when it's being built (partially redundant but ensures completeness)

**Global USING Column Name Checks:**
- Examines dpns->using_names list containing USING column names that must be globally unique across the query level
- Checks colinfo->parentUsing list for USING column names assigned in parent joins

The function implements a systematic approach to avoid naming conflicts at multiple scopes:
1. **RTE-local scope**: Prevents duplicate names within the same range table entry
2. **Global USING scope**: Enforces uniqueness requirements for merged USING columns  
3. **Parent join scope**: Respects USING column names from higher levels in the join tree

This multi-level checking ensures that the generated column aliases will not create ambiguous references when the rule or view is later parsed and executed.

## Parameters / Member Variables
- `colname`: The proposed column name to check for uniqueness
- `dpns`: Query-wide deparse namespace containing global naming information
- `colinfo`: Column information structure for the current RTE containing local naming state

## Dependencies
- Functions called/Symbols referenced:
  - strcmp (standard C library function)
- Called from (representative examples):
  - [make_colname_unique](../m/make_colname_unique.md)

## Notes and Other Information
- Part of PostgreSQL's rule decompilation system for ensuring unique column names
- Returns true if the name is unique and safe to use, false if conflicts exist
- The function performs string comparisons using strcmp() for exact matches
- Critical for preventing naming conflicts that could cause parsing errors when rules/views are reloaded  
- The multi-scope checking approach handles complex scenarios with nested joins and USING clauses
- Used as a building block by make_colname_unique() to find suitable unique names