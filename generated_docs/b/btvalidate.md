# btvalidate

## Location
src/backend/access/nbtree/nbtvalidate.c: 41 - 292

## Overview
Validates a btree operator class by checking the consistency and completeness of its operators and support functions within the broader operator family.

## Definition


## Detailed Description
The  function performs comprehensive validation of a btree operator class to ensure it contains all required operators and support functions with correct signatures. It validates both the individual components and the overall consistency of the operator family. The function checks:

1. **Support Function Validation**: Verifies that each support function has the correct signature based on its procedure number (BTORDER_PROC, BTSORTSUPPORT_PROC, BTINRANGE_PROC, BTEQUALIMAGE_PROC, BTOPTIONS_PROC).

2. **Operator Validation**: Ensures all operators have valid strategy numbers (1-5), proper signatures returning boolean, and are configured for search purposes only (no ORDER BY support).

3. **Completeness Checks**: Verifies that the operator family contains complete sets of operators (all 5 comparison operators: <, <=, =, >=, >) and required support functions for each supported data type combination.

4. **Cross-type Operator Coverage**: Ensures the operator family provides operators for all possible combinations of supported data types to maximize query optimization opportunities.

The validation covers the entire operator family, not just the specific operator class, which may result in some redundant checks when validating multiple classes in the same family.

## Parameters / Member Variables
- : The OID of the btree operator class to validate

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCache1
  - SearchSysCacheList1
  - check_amproc_signature
  - check_amoptsproc_signature
  - check_amop_signature
  - identify_opfamily_groups
  - format_procedure
  - format_operator
  - format_type_be
  - list_append_unique_oid
  - ReleaseCatCacheList
  - ReleaseSysCache
- Called from:
  - bthandler (in btree access method handler)

## Notes and Other Information
- The function accepts some redundancy in validation when multiple operator classes exist in the same family, prioritizing simplicity over performance
- Optional support functions (sortsupport, in_range, equalimage) are not required for validation to pass
- The function reports validation errors as INFO messages rather than throwing errors, allowing multiple issues to be reported in a single validation run
- Cross-type operator completeness is enforced to ensure optimal query planning capabilities
- Special handling exists for in_range functions that may have RHS types not otherwise relevant to the opfamily (e.g., datetime with interval offsets)