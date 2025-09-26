# ForeignKeyOptInfo

## Location
src/include/nodes/pathnodes.h: 1216 - 1257

## Overview
ForeignKeyOptInfo stores per-foreign-key information for planning and optimization, containing both basic foreign key constraint data from catalogs and derived information about how FK equality conditions match the query.

## Definition


## Detailed Description
ForeignKeyOptInfo is a data structure used by PostgreSQL's query planner to store information about foreign key constraints that can be leveraged for optimization. It combines static foreign key metadata retrieved from system catalogs with dynamically computed information about how the foreign key's equality conditions align with the current query's WHERE clause conditions.

The structure serves as a bridge between the foreign key constraint definition and the optimizer's equivalence class system, enabling optimizations such as join elimination, selectivity estimation improvements, and more efficient join ordering decisions. The derived matching information helps the planner understand which foreign key columns have corresponding equality conditions in the query, either through equivalence classes (ECs) or other restrictive conditions (RestrictInfos).

## Parameters / Member Variables
- : Standard NodeTag for node type identification
- : Range table index of the table containing the foreign key (referencing table)
- : Range table index of the table referenced by the foreign key
- : Number of columns participating in the foreign key constraint
- : Array of attribute numbers for foreign key columns in the referencing table
- : Array of attribute numbers for corresponding columns in the referenced table
- : Array of operator OIDs used for primary key = foreign key comparisons
- : Count of foreign key columns that have matching equivalence classes
- : Count of matching equivalence classes that contain constants (ec_has_const)
- : Count of foreign key columns matched by non-EC restrictive conditions
- : Total number of non-EC RestrictInfo nodes matched to this foreign key
- : Array of pointers to EquivalenceClass structures matching each FK column
- : Array of pointers to EquivalenceMember for referencing Var of each column
- : Array of lists containing RestrictInfo nodes matching each column's condition

## Dependencies
- Functions called/Symbols referenced:
  - INDEX_MAX_KEYS (constant for maximum key columns)
  - EquivalenceClass (structure for equivalence classes)
  - EquivalenceMember (structure for equivalence class members)

- Called from (representative examples):
  - get_foreign_key_join_selectivity (costsize.c:5558)
  - match_eclasses_to_foreign_key_col (equivclass.c:2501)
  - match_foreign_keys_to_quals (initsplan.c:3216)
  - get_relation_foreign_keys (plancat.c:648, 663)

## Notes and Other Information
- The structure uses fixed-size arrays limited by INDEX_MAX_KEYS, which is the maximum number of columns allowed in a foreign key constraint
- Only the first  entries in each array are valid and meaningful
- The pg_node_attr annotations indicate special handling for serialization and copying operations
- This structure is primarily used during query planning phases to enable foreign key-based optimizations
- The separation between EC-based and non-EC-based matching allows the optimizer to handle different types of equality conditions appropriately