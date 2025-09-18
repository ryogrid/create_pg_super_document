# tsvector_update_trigger

## Location
[src/backend/utils/adt/tsvector_op.c:2739-2891](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsvector_op.c#L2739-L2891)

## Overview
The core implementation function for PostgreSQL triggers that automatically update tsvector columns when text columns are modified, supporting both named configurations and column-based configurations.

## Definition


## Detailed Description
The  function is the main implementation for automatic tsvector maintenance triggers in PostgreSQL. This static function handles the complete workflow of parsing text from specified columns, applying text search configuration, and updating the target tsvector column. It supports two modes of operation based on the  parameter: using a literal configuration name or referencing a regconfig column.

The function performs extensive validation of trigger arguments, column types, and trigger context. It processes text from multiple source columns, parses them using the specified text search configuration, and generates a combined tsvector. The function is designed to work with BEFORE INSERT or BEFORE UPDATE row-level triggers and includes optimization logic to avoid unnecessary updates when text columns haven't changed.

The function handles memory management carefully, using PostgreSQL's memory context system and properly cleaning up allocated resources. It also integrates with PostgreSQL's trigger infrastructure, including proper handling of trigger data structures and column update tracking.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing trigger information
- : Boolean flag indicating configuration source mode:
  - : Configuration specified by name (literal string)
  - : Configuration specified by regconfig column ID

## Dependencies
- Functions called/Symbols referenced:
  -  - Verify function called in trigger context
  -  - Check if trigger fired for row-level operation
  -  - Verify trigger is BEFORE trigger
  -  - Check if triggered by INSERT
  -  - Check if triggered by UPDATE
  -  - Get column number by name
  -  - Get column data type OID
  -  - Check type compatibility
  -  - Extract column value from tuple
  -  - Convert Datum to OID
  -  - Parse qualified configuration name
  -  - Look up text search configuration OID
  -  - Check if column was updated
  -  - Convert Datum to text
  -  - Parse text using text search configuration
  -  - Create tsvector from parsed text
  -  - Convert tsvector to Datum
  -  - Modify tuple columns
- Called from (representative examples):
  -  - Called with config_column=false
  -  - Called with config_column=true

## Notes and Other Information
- This is a static function, not directly callable from SQL - accessed through wrapper functions
- Must be called in BEFORE INSERT or BEFORE UPDATE trigger context, not AFTER triggers
- Requires minimum of 3 arguments: tsvector_column, config_source, text_column1
- Supports multiple text source columns that are concatenated during processing
- Includes optimization to skip tsvector updates when no relevant text columns changed
- Configuration names must be schema-qualified when using literal names for security
- Handles NULL values gracefully - NULL text columns are skipped, but NULL config columns cause errors
- Memory allocation uses palloc and is automatically cleaned up by PostgreSQL's memory context system
- Integrates with PostgreSQL's column update tracking system for efficient UPDATE operations
- Part of PostgreSQL's comprehensive full-text search infrastructure
- Returns modified HeapTuple that will be used for the actual database operation