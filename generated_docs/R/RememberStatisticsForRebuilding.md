# RememberStatisticsForRebuilding

## Location
src/backend/commands/tablecmds.c: 13811 - 13839

## Overview
RememberStatisticsForRebuilding records extended statistics objects that need to be rebuilt during ALTER TABLE operations, ensuring deduplication and proper definition capture before column type changes.

## Definition
```c
static void RememberStatisticsForRebuilding(Oid stxoid, AlteredTableInfo *tab)
```

## Detailed Description
This function is a subroutine for ATExecAlterColumnType that manages extended statistics objects during column type alterations. It implements the same deduplication logic as RememberIndexForRebuilding to prevent recreating the same statistics object multiple times. The function is particularly important when a statistics object depends on multiple columns whose types are being altered, as it must capture the statistics definition before any type changes are applied to avoid confusion in ruleutils.c when regenerating the definition later.

## Parameters / Member Variables
- `stxoid`: The OID of the extended statistics object that needs to be remembered for rebuilding
- `tab`: Pointer to AlteredTableInfo structure that tracks all changes for the table being altered

## Dependencies
- Functions called/Symbols referenced:
  - list_member_oid
  - pg_get_statisticsobjdef_string
  - lappend_oid
  - AlteredTableInfo (struct)
- Called from (representative examples):
  - child_dependency_type
  - RememberAllDependentForRebuilding

## Notes and Other Information
- The deduplication check prevents double recreation and ensures definition strings are captured before any column type changes
- Similar in structure and purpose to RememberIndexForRebuilding but specifically handles extended statistics objects
- Part of PostgreSQL's extended statistics infrastructure that supports multivariate statistics like n-distinct, dependencies, and MCV lists
- Critical for maintaining statistics during ALTER TABLE operations that change column types