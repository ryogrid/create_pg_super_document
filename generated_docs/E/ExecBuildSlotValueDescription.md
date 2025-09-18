# ExecBuildSlotValueDescription

## Location
src/backend/executor/execMain.c: 2216 - 2352

## Overview
Constructs a human-readable string representation of tuple values with security-aware column filtering and field length truncation for error reporting.

## Definition
```c
static char * ExecBuildSlotValueDescription(Oid reloid,
                                           TupleTableSlot *slot,
                                           TupleDesc tupdesc,
                                           Bitmapset *modifiedCols,
                                           int maxfieldlen)
```

## Detailed Description
ExecBuildSlotValueDescription generates formatted tuple representations for error messages while respecting PostgreSQL's security model. The function performs comprehensive permission checking at both table and column levels, ensuring users only see data they are authorized to access. It handles Row Level Security (RLS) by returning NULL when RLS is enabled, preventing data leakage. For users with partial column access, it constructs column-qualified output showing which specific columns are included. The function truncates long field values to maintain readability and properly handles dropped columns and NULL values.

## Parameters / Member Variables
- `reloid`: OID of the relation for permission checking and RLS evaluation
- `slot`: TupleTableSlot containing the tuple data to be formatted into a description string
- `tupdesc`: Tuple descriptor for the relation, used to identify column metadata and handle dropped columns
- `modifiedCols`: Bitmapset representing columns that were modified, which users can always see regardless of SELECT permissions
- `maxfieldlen`: Maximum length for individual field values before truncation with ellipsis

## Dependencies
- Functions called/Symbols referenced:
  - check_enable_rls
  - pg_class_aclcheck
  - pg_attribute_aclcheck
  - slot_getallattrs
  - bms_is_member
  - getTypeOutputInfo
  - OidOutputFunctionCall
  - pg_mbcliplen
  - appendBinaryStringInfo
- Called from (representative examples):
  - ExecPartitionCheckEmitError
  - ExecConstraints
  - ExecWithCheckOptions

## Notes and Other Information
- Returns NULL when RLS is enabled to prevent unauthorized data disclosure
- Supports partial column access by showing only authorized columns with column name qualifiers
- Performs UTF-8 aware truncation using pg_mbcliplen to avoid breaking multibyte characters
- Users can always see columns they provided data for (in modifiedCols) regardless of SELECT permissions
- Handles dropped columns by skipping them entirely in the output
- Formats output as either '(val1, val2, ...)' for full table access or '(col1, col2) = (val1, val2, ...)' for partial access
- Static function used internally by constraint and error reporting functions