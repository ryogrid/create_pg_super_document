# make_append_rel_info

## Location
src/backend/optimizer/util/appendinfo.c: 51 - 79

## Overview
Creates and initializes an AppendRelInfo structure to represent the relationship between a parent relation and its child relation in PostgreSQL's inheritance hierarchy.

## Definition


## Detailed Description
This function constructs an AppendRelInfo node that encapsulates the metadata needed to handle inheritance relationships between parent and child relations. It sets up the essential mapping information including relation identifiers, type information, and attribute translation lists that enable the query planner to properly handle inheritance hierarchies. The function is a key component in PostgreSQL's inheritance processing infrastructure.

## Parameters / Member Variables
- : The parent relation (table) in the inheritance hierarchy
- : The child relation (table) inheriting from the parent
- : Range table index of the parent relation
- : Range table index of the child relation

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to create AppendRelInfo structure)
  - [make_inh_translation_list](make_inh_translation_list.md) (to build attribute mapping between parent and child)
  - RelationGetRelid (to get relation OID)
- Called from (representative examples):
  - [expand_single_inheritance_child](../e/expand_single_inheritance_child.md)

## Notes and Other Information
- The function automatically populates the AppendRelInfo with relation type information from both parent and child relations
- It delegates the complex task of building attribute translation lists to make_inh_translation_list
- This is a foundational function in PostgreSQL's inheritance support, essential for query planning with table hierarchies