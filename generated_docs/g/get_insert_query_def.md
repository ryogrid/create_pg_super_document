# get_insert_query_def

## Location
src/backend/utils/adt/ruleutils.c: 6647 - 6862

## Overview
Converts an internal INSERT Query structure into its textual SQL representation for rule deparsing, handling all INSERT variants including VALUES, SELECT, and ON CONFLICT clauses.

## Definition
```c
static void get_insert_query_def(Query *query, deparse_context *context)
```

## Detailed Description
This comprehensive function reconstructs INSERT statements from their internal parse tree representation. It handles the full spectrum of PostgreSQL INSERT syntax:

**Core INSERT components:**
- WITH clauses for common table expressions
- Target relation name with proper aliasing
- Column name lists with indirection (array subscripts, field access)
- OVERRIDING SYSTEM/USER VALUE clauses

**Data source variants:**
- Single-row VALUES with expression list
- Multi-row VALUES from VALUES RTEs  
- INSERT ... SELECT from subquery RTEs
- DEFAULT VALUES when no data is specified

**Advanced features:**
- ON CONFLICT clauses with arbiter specifications
- ON CONFLICT DO NOTHING/UPDATE actions
- Constraint-based and index-based conflict detection
- WHERE clauses for partial unique indexes
- RETURNING clauses for output

The function intelligently determines the INSERT type by examining the range table entries (RTEs) and reconstructs the appropriate syntax while preserving all semantic information.

## Parameters / Member Variables
- `query`: Query structure containing the complete INSERT parse tree
- `context`: Deparse context with output buffer, indentation, and formatting state

## Dependencies
- Functions called/Symbols referenced:
  - [get_with_clause](get_with_clause.md) (for WITH clause processing)
  - generate_relation_name (for target table name)
  - get_rte_alias (for table aliasing)
  - [get_attname](get_attname.md)/quote_identifier (for column names)
  - processIndirection (for complex column references)
  - [get_query_def](get_query_def.md) (for INSERT ... SELECT subqueries)
  - [get_values_def](get_values_def.md) (for multi-row VALUES)
  - get_rule_expr (for expressions and conflict specifications)
  - [get_target_list](get_target_list.md) (for RETURNING clauses)
- Called from (representative examples):
  - [get_query_def](get_query_def.md) (main query deparsing entry point)

## Notes and Other Information
- Static function accessible only within ruleutils.c
- Handles PostgreSQL's INSERT extensions like ON CONFLICT ("UPSERT" functionality)
- Properly manages indentation and formatting through the context system
- Critical for view definition reconstruction and rule display
- Located at src/backend/utils/adt/ruleutils.c:6647-6862
- One of the largest and most complex query reconstruction functions due to INSERT's syntactic variety