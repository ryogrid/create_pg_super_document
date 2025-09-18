# BuildDummyIndexInfo

## Location
src/backend/catalog/index.c: 2463 - 2510

## Overview
Constructs a safe dummy IndexInfo record for an open index that avoids executing user-defined code in index expressions or predicates, primarily used for index truncation operations.

## Definition
IndexInfo *BuildDummyIndexInfo(Relation index)

## Detailed Description
This function creates a simplified IndexInfo structure that safely avoids execution of any user-defined code that might exist in index expressions or predicates. It serves as a safer alternative to BuildIndexInfo when the full expression evaluation capabilities are not needed and could potentially be dangerous or unnecessary.

Key differences from BuildIndexInfo:
1. **Dummy Expressions**: Instead of real index expressions, returns null constants with correct types, type modifiers, and collations via RelationGetDummyIndexExpressions
2. **No Predicates**: Completely ignores any index predicate conditions (passes NIL instead)
3. **No Exclusion Constraints**: Ignores exclusion constraint information entirely
4. **Safe Execution**: Eliminates risk of executing arbitrary user code during index metadata operations

The primary use case is index truncation, where the system needs to construct tuple descriptors and understand the index structure without actually evaluating expressions or checking predicates. This makes operations faster and safer by avoiding potentially expensive or problematic user-defined functions.

The function maintains the same basic structure and attribute information as a full IndexInfo, ensuring compatibility with systems that expect IndexInfo structures while providing execution safety.

## Parameters / Member Variables
- : An open Relation structure representing the index for which to build the dummy IndexInfo

## Dependencies
- Functions called/Symbols referenced:
  - makeIndexInfo
  - RelationGetRelid
  - RelationGetDummyIndexExpressions
  - INDEX_MAX_KEYS
  - Form_pg_index
  - NIL
- Called from (representative examples):
  - RelationTruncateIndexes

## Notes and Other Information
- Specifically designed to avoid executing user-defined code that might exist in index expressions or predicates
- Validates index attribute count limits same as BuildIndexInfo (1 to INDEX_MAX_KEYS)
- The dummy expressions have correct data types and properties but are null constants rather than executable expressions
- Exclusion constraints are completely ignored since they're not needed for truncation operations
- Primary use case is TRUNCATE operations where index structure information is needed but expression evaluation is unnecessary
- Provides performance benefits by avoiding potentially expensive expression evaluation during metadata operations
- Maintains the same IndexInfo interface as BuildIndexInfo, making it a drop-in replacement for specific use cases
- The ii_Concurrent flag is set to false, consistent with BuildIndexInfo
- Access method properties are still copied correctly from the index access method structure
- Safer for operations where index expressions might contain volatile functions or functions that should not be executed in the current context