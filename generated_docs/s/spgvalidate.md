# spgvalidate

## Location
src/backend/access/spgist/spgvalidate.c: 39 - 331

## Overview
The `spgvalidate` function serves as the validation function for SP-GiST (Space-Partitioned Generalized Search Tree) operator classes, ensuring that all required support functions and operators are properly defined with correct signatures.

## Definition
```c
bool spgvalidate(Oid opclassoid)
```

## Detailed Description
The `spgvalidate` function performs comprehensive validation of an SP-GiST operator class. It checks that all required support functions are present and have the correct signatures, validates that all operators have appropriate strategy numbers and signatures, and ensures consistency between operators and functions within the operator family.

The function performs several key validation steps:
1. **Support Function Validation**: Checks that all required SP-GiST support functions (config, choose, picksplit, inner_consistent, leaf_consistent, and optionally compress and options) are present with correct signatures
2. **Operator Validation**: Ensures operators have valid strategy numbers (1-63) and correct signatures
3. **Cross-Reference Validation**: Verifies consistency between operators and support functions within the same operator family
4. **Configuration Validation**: Calls the config function and validates its output parameters

Some validation checks are performed across the entire operator family and may be redundant when validating multiple operator classes in the same family, but the performance impact is minimal.

## Parameters / Member Variables
- `opclassoid`: The OID of the SP-GiST operator class to be validated

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCache1
  - SearchSysCacheList1
  - identify_opfamily_groups
  - check_amproc_signature
  - check_amoptsproc_signature
  - check_amop_signature
  - OidFunctionCall2
  - get_op_rettype
  - opfamily_can_sort_type
  - format_procedure
  - format_operator
  - format_type_be
  - ReleaseCatCacheList
  - ReleaseSysCache
- Called from (representative examples):
  - spghandler (in spgutils.c:82)

## Notes and Other Information
- Returns `true` if the operator class is valid, `false` if validation errors are found
- Validation errors are reported using `ereport(INFO, ...)` calls rather than throwing errors
- The function handles both search operators and ORDER BY operators for SP-GiST indexes
- Special handling for compress functions: when leaf and attribute types are the same, the compress function is optional
- The validation covers support function numbers 1-7 corresponding to: config, choose, picksplit, inner_consistent, leaf_consistent, compress, and options procedures
- Strategy numbers for operators must be between 1 and 63
- Cross-type support functions are not used in SP-GiST, so validation only checks same-type function groups