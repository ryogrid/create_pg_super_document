# tlist_same_collations

## Location
src/backend/optimizer/util/tlist.c: 282 - 317

## Overview
Compares a target list's exposed collations against a specified list of expected collations, with optional handling of junk columns.

## Definition


## Detailed Description
This function verifies whether a target list produces the same collations as specified in a given list of expected collations. It follows identical logic to tlist_same_datatypes but focuses on collation compatibility rather than datatype compatibility. This is crucial for ensuring correct string comparison and sorting behavior in set operations and other query planning scenarios.

Collations determine how text data is compared and sorted, including case sensitivity, accent sensitivity, and locale-specific rules. The function ensures that different parts of a query plan will handle text data consistently according to the expected collation rules.

Like its datatype counterpart, this function handles resjunk columns based on the junkOK parameter, allowing callers to either ignore or reject the presence of auxiliary columns.

## Parameters / Member Variables
- : The target list whose collations are to be checked
- : List of Oid values representing the expected collations
- : Whether to ignore resjunk columns (true) or reject them (false)

## Dependencies
- Functions called/Symbols referenced:
  - list_head (to get the first element of colCollations list)
  - exprCollation (to get the collation of an expression)
  - lnext (to advance through colCollations list)
  - lfirst_oid (to extract Oid from list cell)
  - TargetEntry (struct type)
- Called from (representative examples):
  - recurse_set_operations

## Notes and Other Information
- Returns false if the target list is longer or shorter than the expected collations list
- Only non-junk columns are counted when junkOK is true
- Used primarily in set operations (UNION, INTERSECT, EXCEPT) to ensure collation compatibility
- Collation compatibility is essential for correct text handling in international applications
- The function assumes colCollations contains Oid values representing PostgreSQL collation OIDs
- Works in conjunction with tlist_same_datatypes to ensure complete type and collation compatibility