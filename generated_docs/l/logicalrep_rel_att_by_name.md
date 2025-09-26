# logicalrep_rel_att_by_name

## Location
src/backend/replication/logical/relation.c: 209 - 225

## Overview
Searches for an attribute by name within a logical replication relation structure and returns its index position.

## Definition
static int logicalrep_rel_att_by_name(LogicalRepRelation *remoterel, const char *attname)

## Detailed Description
This utility function performs a linear search through the attribute names of a logical replication relation to find a matching attribute name. It provides a simple name-to-index lookup mechanism that is essential for mapping between remote relation schema and local relation schema during logical replication operations.

The function iterates through all attributes in the relation, comparing each attribute name using string comparison until a match is found. If the attribute name exists in the relation, it returns the zero-based index of that attribute. If no matching attribute is found, it returns -1 to indicate failure.

This function is typically used during relation mapping operations where the subscriber needs to correlate attribute names from the publisher with local attribute positions.

## Parameters / Member Variables
- : Pointer to LogicalRepRelation structure containing the relation metadata to search
- : C string containing the name of the attribute to search for

## Dependencies
- Functions called/Symbols referenced:
  - strcmp (standard C string comparison)
  - LogicalRepRelation (relation metadata structure)
- Called from (representative examples):
  - logicalrep_rel_open

## Notes and Other Information
- This is a static function, only accessible within the relation.c file
- Uses linear search algorithm with O(n) time complexity
- Returns -1 as a sentinel value for "not found" cases
- Case-sensitive string matching using strcmp()
- Essential for attribute name resolution in logical replication
- Simple and straightforward implementation suitable for typical relation sizes
- Part of the logical replication relation mapping infrastructure