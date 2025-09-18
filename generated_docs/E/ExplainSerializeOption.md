# ExplainSerializeOption

## Location
src/include/commands/explain.h: 25 - 26

## Overview
ExplainSerializeOption is an enumeration that controls how query output data is serialized when using the EXPLAIN command with data output options.

## Definition


## Detailed Description
This enumeration defines the serialization modes for query result data when using EXPLAIN with the ANALYZE option or other features that need to capture and serialize query output data. The serialization option determines how the actual query results are formatted and stored during query execution analysis.

The enum works in conjunction with ExplainState to control data serialization behavior. When set to a non-NONE value, it enables serialization of query output data alongside the normal explain plan information.

## Parameters / Member Variables
- `EXPLAIN_SERIALIZE_NONE`: No serialization of query output data is performed (default mode)
- `EXPLAIN_SERIALIZE_TEXT`: Query output data is serialized in text format
- `EXPLAIN_SERIALIZE_BINARY`: Query output data is serialized in binary format

## Dependencies
- Functions called/Symbols referenced:
  - Used within ExplainState struct
  - Referenced in explain.c for serialization logic
- Called from (representative examples):
  - ExplainQuery parsing logic (lines 233-248 in explain.c)
  - CreateExplainSerializeDestReceiver function (around line 5498 in explain.c)

## Notes and Other Information
- The serialization option is typically set during EXPLAIN statement parsing based on command options
- When serialize != EXPLAIN_SERIALIZE_NONE, the ANALYZE option is required (enforced at line 294 in explain.c)
- Binary serialization provides more efficient storage for large result sets
- Text serialization is human-readable but less space-efficient
- This feature enables capturing both the execution plan and actual result data in a single EXPLAIN operation