# ChooseForeignKeyConstraintNameAddition

## Location
[src/backend/commands/tablecmds.c:9428-9469](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L9428-L9469)

## Overview
ChooseForeignKeyConstraintNameAddition generates the column-name portion of foreign key constraint names by concatenating the referencing column names with underscores.

## Definition


## Detailed Description
This utility function creates a string representation of column names for use in automatically generated foreign key constraint names. It takes a list of column names that reference the foreign table and concatenates them with underscore separators. The resulting string is designed to be used with ChooseConstraintName along with the table name and "fkey" suffix to create a complete, descriptive constraint name.

The function implements careful length management to respect PostgreSQL's NAMEDATALEN limit (typically 64 characters). It builds the column name portion incrementally, stopping when the accumulated length reaches NAMEDATALEN to prevent buffer overflow. This ensures constraint names remain within PostgreSQL's identifier length limits while being as descriptive as possible.

## Parameters / Member Variables
- : List of column names (as String nodes) that participate in the foreign key reference

## Dependencies
- Functions called/Symbols referenced:
  - strlcpy (safe string copying)
  - strVal (extract string value from Value node)
  - lfirst (list iteration macro)
  - [pstrdup](../p/pstrdup.md) (PostgreSQL string duplication)
  - NAMEDATALEN (maximum identifier length constant)
- Called from (representative examples):
  - [ATExecAddConstraint](../A/ATExecAddConstraint.md)
  - [addFkConstraint](../a/addFkConstraint.md)

## Notes and Other Information
- Part of a family of similar functions including ChooseExtendedStatisticNameAddition and ChooseIndexNameAddition
- Uses a conservative buffer size of NAMEDATALEN * 2 but truncates output to NAMEDATALEN
- Employs paranoid programming practices with strlcpy for safety even when buffer sizes should be sufficient
- The underscore separator convention matches PostgreSQL's standard naming patterns for multi-column constraints
- Essential for automatic constraint naming when users don't specify explicit constraint names
- Helps ensure generated constraint names are both meaningful and unique within the namespace