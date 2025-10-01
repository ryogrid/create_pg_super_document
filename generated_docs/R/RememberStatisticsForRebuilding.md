# RememberStatisticsForRebuilding

## Location
[src/backend/commands/tablecmds.c:13811-13839](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L13811-L13839)

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
  - [list_member_oid](../l/list_member_oid.md)
  - [pg_get_statisticsobjdef_string](../p/pg_get_statisticsobjdef_string.md)
  - [lappend_oid](../l/lappend_oid.md)
  - [AlteredTableInfo](../A/AlteredTableInfo.md) (struct)
- Called from (representative examples):
  - child_dependency_type
  - [RememberAllDependentForRebuilding](RememberAllDependentForRebuilding.md)

## Notes and Other Information
- The deduplication check prevents double recreation and ensures definition strings are captured before any column type changes
- Similar in structure and purpose to RememberIndexForRebuilding but specifically handles extended statistics objects
- Part of PostgreSQL's extended statistics infrastructure that supports multivariate statistics like n-distinct, dependencies, and MCV lists
- Critical for maintaining statistics during ALTER TABLE operations that change column types

## Simplified Source

```c
static void
RememberStatisticsForRebuilding(Oid stxoid, AlteredTableInfo *tab)
{
    // Prevent duplicate entries - critical for multiple column dependencies
    if (!list_member_oid(tab->changedStatisticsOids, stxoid))
    {
        // Capture statistics definition before any changes
        char *defstring = pg_get_statisticsobjdef_string(stxoid);

        // Add to tracking lists
        tab->changedStatisticsOids = lappend_oid(tab->changedStatisticsOids, stxoid);
        tab->changedStatisticsDefs = lappend(tab->changedStatisticsDefs, defstring);
    }
}
```