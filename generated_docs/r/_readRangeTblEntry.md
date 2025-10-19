# _readRangeTblEntry

## Location
[src/backend/nodes/readfuncs.c:347-438](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/readfuncs.c#L347-L438)

## Overview
A static function that deserializes a RangeTblEntry node from its string representation, handling all types of range table entries used in PostgreSQL query planning.

## Definition

```c
static RangeTblEntry *
_readRangeTblEntry(void)
```
## Detailed Description
The  function reconstructs RangeTblEntry nodes from their serialized format during PostgreSQL's node deserialization process. A RangeTblEntry represents a single item in the FROM clause of a SQL query and can take various forms depending on the type of data source.

The function uses a switch statement based on the  field to handle different types of range table entries:
- **RTE_RELATION**: Regular tables or views
- **RTE_SUBQUERY**: Subqueries in the FROM clause
- **RTE_JOIN**: JOIN operations
- **RTE_FUNCTION**: Functions in the FROM clause
- **RTE_TABLEFUNC**: Table functions (like JSON_TABLE)
- **RTE_VALUES**: VALUES clauses
- **RTE_CTE**: Common Table Expressions (WITH clauses)
- **RTE_NAMEDTUPLESTORE**: Named tuple stores (for ephemeral relations)
- **RTE_RESULT**: Result relations

Each RTE type has its own specific fields that need to be deserialized. The function also handles special cases like copying column type information from TableFunc nodes and reusing certain RELATION fields for SUBQUERY and NAMEDTUPLESTORE types.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - READ_LOCALS (macro for local variable setup)
  - READ_NODE_FIELD (macro to read node fields)
  - READ_ENUM_FIELD (macro to read enum fields)
  - READ_OID_FIELD (macro to read OID fields)
  - READ_BOOL_FIELD (macro to read boolean fields)
  - READ_CHAR_FIELD (macro to read character fields)
  - READ_INT_FIELD (macro to read integer fields)
  - READ_UINT_FIELD (macro to read unsigned integer fields)
  - READ_STRING_FIELD (macro to read string fields)
  - READ_FLOAT_FIELD (macro to read float fields)
  - READ_DONE (macro for cleanup)
  - elog (error logging function)
- Called from (representative examples):
  - No direct references found (likely called via function pointer table)

## Notes and Other Information
- This is a static function, accessible only within readfuncs.c
- Handles all nine different types of range table entries with specialized field reading for each
- Implements special logic for RTE_TABLEFUNC to copy column type information from the TableFunc node
- Some RTE types (SUBQUERY, NAMEDTUPLESTORE) reuse fields originally designed for RELATION entries
- RTE_RESULT entries have no extra fields beyond the common ones
- Uses PostgreSQL's standard READ_* macro pattern for consistent field deserialization
- Part of the broader query plan serialization/deserialization system used for prepared statements and parallel query execution

## Simplified Source

```c
static RangeTblEntry *
_readRangeTblEntry(void)
{
    // Initialize local variables for reading
    READ_LOCALS(RangeTblEntry);

    // Read common fields for all RTE types
    READ_NODE_FIELD(alias);
    READ_NODE_FIELD(eref);
    READ_ENUM_FIELD(rtekind, RTEKind);

    // Read type-specific fields based on RTE kind
    switch (local_node->rtekind) {
        case RTE_RELATION:
            // Base table/relation fields
            READ_OID_FIELD(relid);
            READ_BOOL_FIELD(inh);
            READ_CHAR_FIELD(relkind);
            READ_INT_FIELD(rellockmode);
            READ_UINT_FIELD(perminfoindex);
            READ_NODE_FIELD(tablesample);
            break;

        case RTE_SUBQUERY:
            // Subquery fields (reuses some relation fields)
            READ_NODE_FIELD(subquery);
            READ_BOOL_FIELD(security_barrier);
            READ_OID_FIELD(relid);
            READ_BOOL_FIELD(inh);
            // ... other relation fields reused
            break;

        case RTE_JOIN:
            // Join-specific fields
            READ_ENUM_FIELD(jointype, JoinType);
            READ_INT_FIELD(joinmergedcols);
            READ_NODE_FIELD(joinaliasvars);
            READ_NODE_FIELD(joinleftcols);
            READ_NODE_FIELD(joinrightcols);
            READ_NODE_FIELD(join_using_alias);
            break;

        case RTE_TABLEFUNC:
            // Table function with special column type handling
            READ_NODE_FIELD(tablefunc);
            if (local_node->tablefunc) {
                TableFunc *tf = local_node->tablefunc;
                // Copy column type info from TableFunc to RTE
                local_node->coltypes = tf->coltypes;
                local_node->coltypmods = tf->coltypmods;
                local_node->colcollations = tf->colcollations;
            }
            break;

        case RTE_VALUES:
        case RTE_CTE:
            // VALUES and CTE have similar column metadata fields
            // ... field reading logic
            break;

        case RTE_RESULT:
            // No extra fields
            break;

        default:
            elog(ERROR, "unrecognized RTE kind: %d", (int) local_node->rtekind);
    }

    // Read final common fields
    READ_BOOL_FIELD(lateral);
    READ_BOOL_FIELD(inFromCl);
    READ_NODE_FIELD(securityQuals);

    READ_DONE();
}
```

**Key Simplifications:**
- Condensed repetitive cases while showing the pattern
- Added descriptive comments for each RTE type
- Highlighted the special TableFunc column type copying logic
- Grouped logical sections (common fields, type-specific fields, final fields)
- Preserved the essential switch-based dispatch and error handling
- Reduced from ~90 lines to ~50 lines while preserving all essential logic