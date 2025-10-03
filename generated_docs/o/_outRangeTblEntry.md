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