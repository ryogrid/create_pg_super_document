# select_rtable_names_for_explain

## Location
src/backend/utils/adt/ruleutils.c: 3799 - 3827

## Overview
Determines relation aliases used during an EXPLAIN operation by serving as a frontend to set_rtable_names.

## Definition
List *select_rtable_names_for_explain(List *rtable, Bitmapset *rels_used)

## Detailed Description
This function is specifically designed to support the EXPLAIN command by determining appropriate relation aliases that will be displayed in the query execution plan output. It creates a deparse_namespace structure, initializes it with the provided rtable, and calls set_rtable_names to compute the aliases. The function exposes these aliases to EXPLAIN so that the command knows the correct alias names to print in its output.

## Parameters / Member Variables
- rtable: List of range table entries (RTEs) representing the relations in the query
- rels_used: Bitmapset indicating which relations from the rtable are actually used in the query

## Dependencies
- Functions called/Symbols referenced:
  - deparse_namespace
  - set_rtable_names
- Called from (representative examples):
  - ExplainPrintPlan
  - RULE_INDEXDEF_KEYS_ONLY

## Notes and Other Information
- This is essentially a wrapper function around set_rtable_names tailored for EXPLAIN operations
- The function only computes relation aliases, not column aliases (as noted in the comment)
- Returns the rtable_names list from the deparse_namespace structure
- The function initializes several fields of deparse_namespace to appropriate default values (NIL, NULL) before processing