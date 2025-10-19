# _outRangeTblEntry

## Location
[src/backend/nodes/outfuncs.c:496-575](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/outfuncs.c#L496-L575)

## Overview
Serializes a RangeTblEntry structure to string format, handling the complex union-like structure with different fields based on the range table entry type (relation, subquery, join, function, etc.).

## Definition

```c
static void
_outRangeTblEntry(StringInfo str, const RangeTblEntry *node)
```
## Detailed Description
The  function is responsible for serializing RangeTblEntry structures, which are fundamental components of PostgreSQL's query representation. Range table entries represent the various sources of tuples in a query, including base relations, subqueries, joins, functions, VALUES clauses, CTEs, and more.

The function uses a switch statement based on the  field to determine which specific fields to serialize, as RangeTblEntry uses a union-like approach where different fields are meaningful depending on the entry type. Each RTE kind has its own set of relevant fields that need to be serialized appropriately.

The function handles eight different range table entry types:
- RTE_RELATION: Base relations/tables
- RTE_SUBQUERY: Subqueries in FROM clause
- RTE_JOIN: Join expressions  
- RTE_FUNCTION: Function calls in FROM clause
- RTE_TABLEFUNC: Table functions
- RTE_VALUES: VALUES clauses
- RTE_CTE: Common Table Expressions
- RTE_NAMEDTUPLESTORE: Named tuple stores
- RTE_RESULT: Result relations

## Parameters / Member Variables
- `str`: StringInfo buffer where the serialized RangeTblEntry representation will be written
- `*node`: Pointer to the RangeTblEntry structure to be serialized
## Dependencies
- Functions called/Symbols referenced:
  - WRITE_NODE_TYPE
  - WRITE_NODE_FIELD
  - WRITE_ENUM_FIELD
  - WRITE_OID_FIELD
  - WRITE_BOOL_FIELD
  - WRITE_CHAR_FIELD
  - WRITE_INT_FIELD
  - WRITE_UINT_FIELD
  - WRITE_STRING_FIELD
  - WRITE_FLOAT_FIELD
- Types/Constants referenced:
  - [RangeTblEntry](../R/RangeTblEntry.md)
  - [RTEKind](../R/RTEKind.md) enumeration values
  - JoinType enumeration
- Called from (representative examples):
  - No direct callers found (likely called through function pointer dispatch in the node output system)

## Notes and Other Information
- This is a static function, used only within the outfuncs.c compilation unit
- The function demonstrates PostgreSQL's polymorphic approach to range table entries, where a single structure type represents multiple conceptually different data sources
- Some fields are reused across different RTE kinds (e.g., RELATION fields are reused in SUBQUERY and NAMEDTUPLESTORE)
- The function includes error handling for unrecognized RTE kinds
- Common fields like , , and  are serialized for all RTE types
- Part of PostgreSQL's plan/parse tree serialization system used for plan caching, parallel query execution, and debugging

## Simplified Source

```c
static void
_outRangeTblEntry(StringInfo str, const RangeTblEntry *node)
{
    // Write the node type identifier
    WRITE_NODE_TYPE("RANGETBLENTRY");

    // Write common fields for all RTE types
    WRITE_NODE_FIELD(alias);
    WRITE_NODE_FIELD(eref);
    WRITE_ENUM_FIELD(rtekind, RTEKind);

    // Write type-specific fields based on RTE kind
    switch (node->rtekind) {
        case RTE_RELATION:
            // Base table/relation fields
            WRITE_OID_FIELD(relid);
            WRITE_BOOL_FIELD(inh);
            WRITE_CHAR_FIELD(relkind);
            WRITE_INT_FIELD(rellockmode);
            WRITE_UINT_FIELD(perminfoindex);
            WRITE_NODE_FIELD(tablesample);
            break;

        case RTE_SUBQUERY:
            // Subquery fields (reuses some relation fields)
            WRITE_NODE_FIELD(subquery);
            WRITE_BOOL_FIELD(security_barrier);
            WRITE_OID_FIELD(relid);
            WRITE_BOOL_FIELD(inh);
            // ... other relation fields reused
            break;

        case RTE_JOIN:
            // Join-specific fields
            WRITE_ENUM_FIELD(jointype, JoinType);
            WRITE_INT_FIELD(joinmergedcols);
            WRITE_NODE_FIELD(joinaliasvars);
            WRITE_NODE_FIELD(joinleftcols);
            WRITE_NODE_FIELD(joinrightcols);
            WRITE_NODE_FIELD(join_using_alias);
            break;

        case RTE_FUNCTION:
            // Function call fields
            WRITE_NODE_FIELD(functions);
            WRITE_BOOL_FIELD(funcordinality);
            break;

        case RTE_VALUES:
            // VALUES clause fields
            WRITE_NODE_FIELD(values_lists);
            WRITE_NODE_FIELD(coltypes);
            WRITE_NODE_FIELD(coltypmods);
            WRITE_NODE_FIELD(colcollations);
            break;

        case RTE_CTE:
            // Common Table Expression fields
            WRITE_STRING_FIELD(ctename);
            WRITE_UINT_FIELD(ctelevelsup);
            WRITE_BOOL_FIELD(self_reference);
            WRITE_NODE_FIELD(coltypes);
            // ... other column metadata
            break;

        // Additional cases: RTE_TABLEFUNC, RTE_NAMEDTUPLESTORE, RTE_RESULT
        default:
            elog(ERROR, "unrecognized RTE kind: %d", (int) node->rtekind);
    }

    // Write final common fields
    WRITE_BOOL_FIELD(lateral);
    WRITE_BOOL_FIELD(inFromCl);
    WRITE_NODE_FIELD(securityQuals);
}
```

**Key Simplifications:**
- Condensed repetitive cases while showing the pattern
- Added descriptive comments for each RTE type
- Grouped logical sections (common fields, type-specific fields, final fields)
- Preserved the essential switch-based dispatch logic
- Maintained error handling for unknown types
- Reduced from ~78 lines to ~50 lines while preserving all essential logic