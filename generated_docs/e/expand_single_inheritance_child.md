# expand_single_inheritance_child

## Location
src/backend/optimizer/util/inherit.c: 461 - 655

## Overview
Builds a RangeTblEntry and AppendRelInfo for a single child relation in an inheritance hierarchy, along with optional PlanRowMark for row locking support.

## Definition


## Detailed Description
This static function creates the necessary planner data structures for a single child relation in an inheritance or partitioning hierarchy. Key operations include:

1. **RTE Creation**: Creates a new RangeTblEntry by copying most fields from the parent RTE but updating relation-specific fields (OID, relkind, inh flag). Sets securityQuals to empty to ensure parent RLS conditions apply uniformly.

2. **AppendRelInfo Construction**: Creates an AppendRelInfo structure using make_append_rel_info to establish the parent-child relationship and column mapping.

3. **Column Alias Management**: Constructs proper column aliases for the child relation by mapping parent column names through the AppendRelInfo, ensuring EXPLAIN output shows correct column names.

4. **PlanRowMark Setup**: If row locking is required, creates a PlanRowMark for the child with appropriate mark type based on the child's relation kind.

5. **Result Relation Handling**: For UPDATE/DELETE/MERGE operations, adds the child to result relation sets and generates necessary row identity columns including tableoid.

The function supports both traditional inheritance and modern partitioning, with partitioned children marked for further expansion.

## Parameters / Member Variables
- : PlannerInfo structure containing global planner state
- : RangeTblEntry of the immediate parent relation
- : Index of parent RTE in the range table
- : Open Relation structure for the parent
- : PlanRowMark from the top-level parent for row locking
- : Open Relation structure for the child being processed
- : Output parameter for the created child RangeTblEntry
- : Output parameter for the child's range table index

## Dependencies
- Functions called/Symbols referenced:
  - make_append_rel_info
  - makeAlias, makeString, makeVar
  - select_rowmark_type
  - add_row_identity_var
  - add_row_identity_columns
  - copyObject
  - bms_is_member, bms_add_member
- Called from (representative examples):
  - expand_inherited_rtentry
  - expand_partitioned_rtentry

## Notes and Other Information
- Creates a hierarchical structure where each partitioned descendant acts as parent of its immediate partitions (differs from older flattened approach)
- PlanRowMarks retain the top-parent's RTI while accumulating mark types from all descendants
- Child permissions are handled through the parent - no separate permission checking for child RTEs
- For partitioned children, the inh flag is set to true to trigger further expansion
- Table aliases are duplicated from parent; ruleutils.c handles uniqueness during plan printing
- Row identity columns (tableoid) are automatically added for child target relations in DML operations