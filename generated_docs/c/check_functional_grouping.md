# check_functional_grouping

## Location
src/backend/catalog/pg_constraint.c: 1367 - 1403

## Overview
Determines whether a relation can be proven functionally dependent on a set of grouping columns by checking if the relation's primary key is a subset of the grouping columns.

## Definition


## Detailed Description
This function implements functional dependency checking for SQL GROUP BY clause validation. It verifies whether a relation can be considered functionally dependent on a set of grouping expressions, which is essential for determining if non-grouped columns can be legally selected in aggregate queries.

The function works by:
1. Retrieving the primary key column attributes for the specified relation
2. Identifying which relation columns appear in the grouping expressions
3. Checking if the primary key columns are a subset of the grouped columns
4. If so, adding the primary key constraint OID to the dependency list

This is used in SQL standard compliance for GROUP BY queries, where columns not in the GROUP BY clause can still be selected if they are functionally dependent on the grouped columns (typically through a primary key relationship).

## Parameters / Member Variables
- : OID of the relation to check for functional dependency
- : Variable number identifying the relation in the query's range table
- : Nesting level for the variable reference (0 for current query level)
- : List of grouping expressions from the GROUP BY clause
- : Output parameter - list of constraint OIDs that prove the functional dependency

## Dependencies
- Functions called/Symbols referenced:
  - : Retrieves primary key column bitmap and constraint OID
  - : Adds column attribute number to bitmap set
  - : Tests if one bitmap is subset of another
  - : Appends OID to list
  - : Constant for attribute number offset calculation

- Called from (representative examples):
  - : Used during parse analysis of aggregate queries

## Notes and Other Information
- Currently only supports primary key constraints for functional dependency proofs
- Could theoretically support unique constraints with NOT NULL columns, but this is not implemented due to limitations in representing NOT NULL constraints in pg_constraint
- The function performs attribute number adjustment using FirstLowInvalidHeapAttributeNumber to handle PostgreSQL's internal attribute numbering scheme
- Returns false if the relation has no primary key or if the primary key is not fully covered by the grouping columns
- Part of PostgreSQL's SQL standard compliance for GROUP BY functionality
- Located in src/backend/catalog/pg_constraint.c:1367-1403