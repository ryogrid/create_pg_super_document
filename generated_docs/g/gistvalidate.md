# gistvalidate

## Location
src/backend/access/gist/gistvalidate.c: 33 - 289

## Overview
Validates the completeness and correctness of a GiST (Generalized Search Tree) operator class by checking its support functions and operators against GiST access method requirements.

## Definition


## Detailed Description
The  function performs comprehensive validation of a GiST operator class to ensure it conforms to the requirements of the GiST access method. It validates both the structure and signatures of support functions and operators within the operator class and its associated operator family.

The validation process includes:
1. **Support Function Validation**: Checks that all GiST support functions have correct signatures and are registered with matching left/right input types
2. **Operator Validation**: Verifies that operators have valid strategy numbers, correct signatures, and proper ORDER BY support when applicable
3. **Completeness Check**: Ensures the operator class contains all required support functions
4. **Cross-Reference Validation**: Validates relationships between operators and their corresponding support functions

For each support function (GIST_CONSISTENT_PROC through GIST_SORTSUPPORT_PROC), the function validates the expected signature using  or . It also ensures that ORDER BY operators have corresponding distance functions and that the operator result types are compatible with the specified btree operator families.

## Parameters / Member Variables
- : The OID of the GiST operator class to validate

## Dependencies
- Functions called/Symbols referenced:
  -  - Cache lookups for operator class and family information
  -  - Retrieve operators and support functions
  -  - Validate support function signatures
  -  - Validate options support function signature
  -  - Validate operator signatures
  -  - Group operators and functions by datatype combinations
  -  - Verify btree compatibility for ORDER BY operators
  -  - Look up distance procedures for ORDER BY operators
  -  - Get operator return type
- Called from:
  -  at src/backend/access/gist/gist.c:97

## Notes and Other Information
- The function returns  if the operator class is valid,  otherwise
- Issues are reported using  rather than throwing errors, allowing multiple validation issues to be reported in a single validation run  
- Required GiST support functions include: CONSISTENT, UNION, PENALTY, PICKSPLIT, and EQUAL procedures
- Optional functions include: COMPRESS, DECOMPRESS, DISTANCE, FETCH, OPTIONS, and SORTSUPPORT procedures
- All GiST support functions must have matching left and right input types
- ORDER BY operators require corresponding distance functions and compatible btree operator families
- The validation leverages the processed symbols , , , , and  for comprehensive signature and compatibility checking