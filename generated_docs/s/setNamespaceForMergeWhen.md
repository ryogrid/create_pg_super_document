# setNamespaceForMergeWhen

## Location
[src/backend/parser/parse_merge.c:52-106](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_merge.c#L52-L106)

## Overview
A static function that manages namespace visibility for different types of MERGE statement action clauses by controlling which relations (source and/or target) are visible during expression transformation.

## Definition

```c
static void
setNamespaceForMergeWhen(ParseState *pstate, MergeWhenClause *mergeWhenClause,
						 Index targetRTI, Index sourceRTI)
```
## Detailed Description
This function adjusts the namespace visibility in the parser state to ensure that the correct relations are accessible when transforming individual MERGE action's qualification expressions and target lists. The visibility rules depend on the type of MERGE action:

- **MATCHED actions** (UPDATE/DELETE): Can see both source and target relations since they operate on rows that exist in both relations
- **NOT MATCHED BY SOURCE actions** (UPDATE/DELETE): Can only see the target relation since there's no corresponding source row
- **NOT MATCHED BY TARGET actions** (INSERT): Can only see the source relation since there's no corresponding target row

The function explicitly makes the appropriate relations visible in the namespace to allow unqualified column references, overriding any hiding that might occur due to internal join nodes.

## Parameters / Member Variables
- `*pstate`: Parser state containing the current parsing context and namespace information
- `*mergeWhenClause`: The specific MERGE WHEN clause being processed, containing match type and command information
- `targetRTI`: Range table index of the target relation in the MERGE statement
- `sourceRTI`: Range table index of the source relation in the MERGE statement
## Dependencies
- Functions called/Symbols referenced:
  - rt_fetch
  - [setNamespaceVisibilityForRTE](setNamespaceVisibilityForRTE.md)
  - MERGE_WHEN_MATCHED (enum value)
  - MERGE_WHEN_NOT_MATCHED_BY_SOURCE (enum value)
  - CMD_UPDATE, CMD_DELETE, CMD_INSERT, CMD_NOTHING (command type constants)
- Called from (representative examples):
  - [transformMergeStmt](../t/transformMergeStmt.md)

## Notes and Other Information
- This is a static helper function specific to MERGE statement parsing
- The function includes assertions to validate that command types are appropriate for each match kind
- The visibility settings are crucial for proper scoping of column references in MERGE actions
- Different match types have different visibility requirements based on SQL MERGE semantics
- The function handles the three main MERGE match scenarios defined by the SQL standard

## Simplified Source

```c
static void
setNamespaceForMergeWhen(ParseState *pstate, MergeWhenClause *mergeWhenClause,
                         Index targetRTI, Index sourceRTI)
{
    RangeTblEntry *targetRelRTE, *sourceRelRTE;

    targetRelRTE = rt_fetch(targetRTI, pstate->p_rtable);
    sourceRelRTE = rt_fetch(sourceRTI, pstate->p_rtable);

    if (mergeWhenClause->matchKind == MERGE_WHEN_MATCHED) {
        // MATCHED actions can see both target and source relations
        Assert(mergeWhenClause->commandType == CMD_UPDATE ||
               mergeWhenClause->commandType == CMD_DELETE ||
               mergeWhenClause->commandType == CMD_NOTHING);

        setNamespaceVisibilityForRTE(pstate->p_namespace, targetRelRTE, true, true);
        setNamespaceVisibilityForRTE(pstate->p_namespace, sourceRelRTE, true, true);
    }
    else if (mergeWhenClause->matchKind == MERGE_WHEN_NOT_MATCHED_BY_SOURCE) {
        // NOT MATCHED BY SOURCE actions can see target but not source
        Assert(mergeWhenClause->commandType == CMD_UPDATE ||
               mergeWhenClause->commandType == CMD_DELETE ||
               mergeWhenClause->commandType == CMD_NOTHING);

        setNamespaceVisibilityForRTE(pstate->p_namespace, targetRelRTE, true, true);
        setNamespaceVisibilityForRTE(pstate->p_namespace, sourceRelRTE, false, false);
    }
    else {
        // NOT MATCHED BY TARGET actions can see source but not target
        Assert(mergeWhenClause->commandType == CMD_INSERT ||
               mergeWhenClause->commandType == CMD_NOTHING);

        setNamespaceVisibilityForRTE(pstate->p_namespace, targetRelRTE, false, false);
        setNamespaceVisibilityForRTE(pstate->p_namespace, sourceRelRTE, true, true);
    }
}
```