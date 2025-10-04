# _outForeignKeyOptInfo

## Location
[src/backend/nodes/outfuncs.c:429-454](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/outfuncs.c#L429-L454)

## Overview
Serializes a ForeignKeyOptInfo node to its string representation, outputting foreign key constraint metadata and optimization statistics used by PostgreSQL's query planner for join optimization.

## Definition

```c
static void
_outForeignKeyOptInfo(StringInfo str, const ForeignKeyOptInfo *node)
```
## Detailed Description
The  function serializes ForeignKeyOptInfo nodes, which contain metadata about foreign key constraints that PostgreSQL's query planner uses for join optimization and constraint inference.

The function outputs comprehensive foreign key information including the referencing and referenced table OIDs (con_relid, ref_relid), the number of key columns (nkeys), and the actual column mappings (conkey, confkey arrays). It also includes operator information (conpfeqop) and various optimization statistics like the number of matched equivalence classes (nmatched_ec), constant equivalence classes (nconst_ec), and restriction info matches.

For compactness, the equivalence class and restriction info arrays are summarized as counts rather than full serialization, making the output more readable while preserving essential optimization information.

## Parameters / Member Variables
- `str`: StringInfo buffer where the serialized output is appended
- `*node`: Pointer to the ForeignKeyOptInfo node to be serialized
## Dependencies
- Functions called/Symbols referenced:
  - WRITE_NODE_TYPE
  - WRITE_UINT_FIELD
  - WRITE_INT_FIELD
  - WRITE_ATTRNUMBER_ARRAY
  - WRITE_OID_ARRAY
  - [appendStringInfoString](../a/appendStringInfoString.md)
  - [appendStringInfo](../a/appendStringInfo.md)
  - [list_length](../l/list_length.md)
- Called from (representative examples):
  - (Part of node output dispatch system - called indirectly through nodeToString mechanisms)

## Notes and Other Information
This function is part of PostgreSQL's query optimization infrastructure and provides visibility into how foreign key constraints are analyzed and utilized during query planning. The compact representation of equivalence classes and restriction info arrays (showing counts rather than full contents) reflects the practical need to balance information completeness with output readability. ForeignKeyOptInfo nodes are typically created during query planning when the optimizer analyzes foreign key relationships to enable optimizations like join elimination and constraint propagation. The function is essential for debugging foreign key-related optimization decisions in complex queries involving multiple tables with referential integrity constraints.

## Simplified Source

```c
static void _outForeignKeyOptInfo(StringInfo str, const ForeignKeyOptInfo *node) {
    WRITE_NODE_TYPE("FOREIGNKEYOPTINFO");

    // Output basic foreign key information
    WRITE_UINT_FIELD(con_relid);      // Referencing table OID
    WRITE_UINT_FIELD(ref_relid);      // Referenced table OID
    WRITE_INT_FIELD(nkeys);           // Number of key columns

    // Output column mappings and operators
    WRITE_ATTRNUMBER_ARRAY(conkey, node->nkeys);    // Referencing columns
    WRITE_ATTRNUMBER_ARRAY(confkey, node->nkeys);   // Referenced columns
    WRITE_OID_ARRAY(conpfeqop, node->nkeys);        // Equality operators

    // Output optimization statistics
    WRITE_INT_FIELD(nmatched_ec);     // Matched equivalence classes
    WRITE_INT_FIELD(nconst_ec);       // Constant equivalence classes
    WRITE_INT_FIELD(nmatched_rcols);  // Matched result columns
    WRITE_INT_FIELD(nmatched_ri);     // Matched restriction infos

    // Output compact equivalence class and restriction info counts
    appendStringInfoString(str, " :eclass");
    for (int i = 0; i < node->nkeys; i++)
        appendStringInfo(str, " %d", (node->eclass[i] != NULL));

    appendStringInfoString(str, " :rinfos");
    for (int i = 0; i < node->nkeys; i++)
        appendStringInfo(str, " %d", list_length(node->rinfos[i]));
}
```