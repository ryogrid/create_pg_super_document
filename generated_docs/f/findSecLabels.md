# findSecLabels

## Location
src/bin/pg_dump/pg_dump.c: 15552 - 15630

## Overview
Performs a binary search to find all security labels associated with a specific database object identified by classoid and objoid parameters.

## Definition


## Detailed Description
This function efficiently searches through a sorted array of security labels to find all labels associated with a particular database object. It uses a binary search algorithm to locate the first matching entry, then expands the search to find all entries with the same classoid and objoid combination. Security labels in PostgreSQL are stored as a sorted array for efficient lookup, and this function takes advantage of that ordering.

The search works in two phases: first, it performs a standard binary search to find any matching entry based on classoid and objoid. Then, it expands both backward and forward from the found position to collect all entries that match the same object, since multiple security labels can be associated with a single object (from different providers or for different sub-objects like columns).

## Parameters / Member Variables
- : Object Identifier of the system catalog class (e.g., RelationRelationId for tables)
- : Object Identifier of the specific database object within that class
- : Output parameter - pointer to array of matching SecLabelItem structures

## Dependencies
- Functions called/Symbols referenced:
  - [SecLabelItem](../S/SecLabelItem.md): Structure type for security label entries
  - nseclabels: Global variable containing count of security labels
  - seclabels: Global array containing all security labels
- Called from:
  - [dumpSecLabel](../d/dumpSecLabel.md): For dumping security labels of general database objects
  - [dumpTableSecLabel](../d/dumpTableSecLabel.md): For dumping security labels of tables and their columns
  - fmtQualifiedDumpable: For formatting qualified names with security context

## Notes and Other Information
- Returns the number of matching security label items found, or 0 if none
- The function assumes the global seclabels array is sorted by (classoid, objoid, objsubid)
- Uses efficient binary search with O(log n) complexity for initial lookup
- Handles multiple labels per object by expanding search in both directions
- Sets *items to NULL and returns 0 if no security labels are loaded or no matches found
- Part of pg_dump's security label handling infrastructure
- The found items remain valid as long as the global seclabels array is not modified