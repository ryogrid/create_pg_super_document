# to_regcollation

## Location
src/backend/utils/adt/regproc.c: 1068 - 1085

## Overview
Safely converts a collation name text input to regcollation OID, returning NULL instead of raising an error when the collation is not found.

## Definition


## Detailed Description
The  function provides a safe conversion mechanism from text collation names to regcollation OID values. Unlike , which raises errors for invalid collation names,  returns NULL when a collation cannot be found or parsed.

This function serves as a user-friendly wrapper around  that implements PostgreSQL's "soft error" pattern. It:

1. Extracts the text input and converts it to a C string
2. Sets up an error context for safe error handling  
3. Calls  via  to perform the actual conversion
4. Returns NULL if any error occurs during the conversion process
5. Returns the OID result if successful

This approach allows SQL queries to handle missing collations gracefully without aborting the entire operation, making it suitable for conditional logic and data validation scenarios.

The function accepts the same input formats as :
- Collation names (simple or schema-qualified)
- Numeric OID strings
- Special value "-" for unknown collation

## Parameters / Member Variables
- Input: Text value containing collation name, schema.name, numeric OID, or "-"

## Dependencies
- Functions called/Symbols referenced:
  -  - Convert PostgreSQL text type to C string
  -  - Extract text argument with potential detoasting
  -  - Error context structure for safe error handling
  -  - Safely call input function with error capture
  -  - Core collation name-to-OID conversion function
  -  - Return NULL result
  -  - Return successful result

- Called from (representative examples):
  - (No direct references found - typically used in SQL contexts)

## Notes and Other Information
- Implements PostgreSQL's "to_" function pattern for safe type conversion
- Returns NULL instead of raising errors, making it suitable for conditional SQL logic
- Uses the same underlying conversion logic as regcollationin but with error suppression
- Part of the regcollation type function family alongside regcollationin
- The ErrorSaveContext mechanism allows catching and suppressing conversion errors
- Accepts all the same input formats as regcollationin (names, OIDs, special values)
- Useful in scenarios where collation existence should be tested rather than enforced
- Commonly used in data validation and migration scenarios where graceful handling of missing collations is required