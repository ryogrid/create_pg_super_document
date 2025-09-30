# setTargetTable

## Location
[src/backend/parser/parse_clause.c:180-254](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_clause.c#L180-L254)

## Overview
Adds the target relation of INSERT/UPDATE/DELETE/MERGE statements to the range table and establishes special links to it in the ParseState, while acquiring necessary write locks.

## Definition

```c
int
setTargetTable(ParseState *pstate, RangeVar *relation,
			   bool inh, bool alsoSource, AclMode requiredPerms)
```
## Detailed Description
The `setTargetTable` function is essential for processing data modification statements (DML) in PostgreSQL. It performs several critical tasks:

1. **ENR conflict checking**: Ensures that Ephemeral Named Relations (ENRs) don't conflict with the target table name
2. **Lock acquisition**: Opens the target relation and acquires a write lock (RowExclusiveLock) that will be held until transaction end
3. **Range table entry creation**: Creates an RTE and ParseNamespaceItem for the target relation
4. **Permission management**: Sets the required permissions on the target relation
5. **Conditional namespace addition**: Optionally adds the target to the query's joinlist and namespace based on the statement type

The function handles different DML statement types appropriately:
- **INSERT**: Target is not added to joinlist/namespace (destination only)
- **UPDATE/DELETE**: Target is added to joinlist/namespace (needs to be scanned/joined)
- **MERGE**: Target is handled like INSERT initially (added separately later)

Lock acquisition occurs before processing the FROM list to ensure write locks are obtained before any read locks if the target appears in both contexts.

## Parameters / Member Variables
- `pstate`: The current parse state containing parsing context and target relation information
- `relation`: RangeVar specifying the target relation (table name, schema, alias)
- `inh`: Boolean indicating whether to include inheritance children in the operation
- `alsoSource`: Boolean indicating whether the target should also be treated as a source (true for UPDATE/DELETE, false for INSERT/MERGE)
- `requiredPerms`: AclMode specifying the permissions required on the target relation

## Dependencies
- Functions called/Symbols referenced:
  - [RangeVar](../R/RangeVar.md)
  - [ParseNamespaceItem](../P/ParseNamespaceItem.md)
  - [scanNameSpaceForENR](scanNameSpaceForENR.md)
  - [parserOpenTable](../p/parserOpenTable.md)
  - [addRangeTableEntryForRelation](../a/addRangeTableEntryForRelation.md)
  - [addNSItemToQuery](../a/addNSItemToQuery.md)
- Called from (representative examples):
  - [transformDeleteStmt](../t/transformDeleteStmt.md)
  - [transformInsertStmt](../t/transformInsertStmt.md)
  - [transformUpdateStmt](../t/transformUpdateStmt.md)
  - [transformMergeStmt](../t/transformMergeStmt.md)

## Notes and Other Information
- Must be called before processing the FROM list to ensure proper lock ordering
- Handles cleanup of previous target relations for multi-action rules
- The write lock is held until transaction end and is not released by free_parsestate()
- ENRs (Ephemeral Named Relations) take precedence over regular tables with the same name
- Permission checking is customized based on the specific DML operation requirements
- Returns the range table index of the target relation for later reference

## Simplified Source

```c
int setTargetTable(ParseState *pstate, RangeVar *relation,
                   bool inh, bool alsoSource, AclMode requiredPerms) {
    ParseNamespaceItem *nsitem;

    // Check for ENR conflicts (ENRs hide tables of same name)
    if (relation->schemaname == NULL &&
        scanNameSpaceForENR(pstate, relation->relname))
        ereport(ERROR, "relation cannot be target of modifying statement");

    // Close any previous target relation (for multi-action rules)
    if (pstate->p_target_relation != NULL)
        table_close(pstate->p_target_relation, NoLock);

    // Open target relation and acquire write lock (held till transaction end)
    pstate->p_target_relation = parserOpenTable(pstate, relation,
                                                RowExclusiveLock);

    // Create range table entry and namespace item
    nsitem = addRangeTableEntryForRelation(pstate, pstate->p_target_relation,
                                          RowExclusiveLock,
                                          relation->alias, inh, false);

    // Remember this as the query target
    pstate->p_target_nsitem = nsitem;

    // Set required permissions (overrides default ACL_SELECT)
    nsitem->p_perminfo->requiredPerms = requiredPerms;

    // For UPDATE/DELETE, add table to joinlist and namespace
    if (alsoSource)
        addNSItemToQuery(pstate, nsitem, true, true, true);

    return nsitem->p_rtindex;
}
```

**Key Points:**
- Sets up target relation for DML statements (INSERT/UPDATE/DELETE/MERGE)
- Checks for ENR conflicts and closes any previous target relation
- Acquires RowExclusiveLock on target (held until transaction end)
- Creates range table entry and namespace item for target relation
- Sets appropriate permissions based on operation requirements
- Conditionally adds to joinlist/namespace: true for UPDATE/DELETE, false for INSERT/MERGE
- Returns range table index for later reference during parsing