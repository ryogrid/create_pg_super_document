# ChooseIndexNameAddition

## Location
[src/backend/commands/indexcmds.c:2598-2631](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/indexcmds.c#L2598-L2631)

## Overview
ChooseIndexNameAddition generates a column-based name component for PostgreSQL indexes by concatenating column names with underscores, truncating as needed to fit within PostgreSQL's naming constraints.

## Definition


## Detailed Description
ChooseIndexNameAddition creates a "name2" component that will be used by ChooseRelationName when generating index names. It concatenates column names from the provided list, separating them with underscores, and ensures the result fits within PostgreSQL's NAMEDATALEN limit. The function builds the name incrementally, checking length constraints to avoid buffer overflow.

The generated string serves as the column-specific portion of index names, helping to make index names descriptive and unique based on the columns they cover.

## Parameters / Member Variables
- : A List of column names (as char* strings) that the index covers

## Dependencies
- Functions called/Symbols referenced:
  - NAMEDATALEN (PostgreSQL's maximum name length constant)
  - strlcpy (safe string copy function)
  - [pstrdup](../p/pstrdup.md) (PostgreSQL's string duplication function)
  - lfirst (list iteration macro)
  - foreach (list iteration macro)
- Called from:
  - [ChooseIndexName](ChooseIndexName.md) (three times for different index types in src/backend/commands/indexcmds.c)

## Notes and Other Information
- This is a static function internal to indexcmds.c
- Uses a buffer of size NAMEDATALEN * 2 for building the name, but truncates output to NAMEDATALEN
- The function is paranoid about buffer overflow protection using strlcpy
- Similar functions exist for other constraint types (ChooseForeignKeyConstraintNameAddition, ChooseExtendedStatisticNameAddition)
- Stops concatenating column names once NAMEDATALEN limit is reached
- Each column name is separated by an underscore character
- Returns a palloc'd string that must be freed by the caller