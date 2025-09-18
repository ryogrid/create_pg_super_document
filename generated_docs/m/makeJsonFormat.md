# makeJsonFormat

## Location
src/backend/nodes/makefuncs.c: 894 - 909

## Overview
Creates a JsonFormat node that specifies formatting and encoding options for JSON data processing operations in PostgreSQL.

## Definition


## Detailed Description
This function constructs a JsonFormat structure used in PostgreSQL's JSON processing functionality. The JsonFormat node encapsulates formatting specifications for JSON operations such as JSON_SERIALIZE, JSON output formatting, and JSON table operations. It defines how JSON data should be formatted and what encoding should be used when processing or outputting JSON values.

The structure supports various JSON format types (such as JSON, JSONB) and different encoding options to handle character set conversions and formatting requirements for different client applications and use cases.

## Parameters / Member Variables
- : JsonFormatType enumeration value specifying the format type (e.g., JS_FORMAT_JSON, JS_FORMAT_JSONB)
- : JsonEncoding enumeration specifying the character encoding for the JSON output (e.g., JS_ENC_DEFAULT, JS_ENC_UTF8, JS_ENC_UTF16, etc.)
- : Source code location (character position) in the original SQL query for error reporting and debugging

## Dependencies
- Functions called/Symbols referenced:
  -  - Creates a new node of type JsonFormat
  -  - The JSON format specification node structure type
  -  - Enumeration defining different JSON format types
  -  - Enumeration defining character encoding options for JSON
- Called from (representative examples):
  -  - Transforms JSON output specifications in queries
  -  - Processes RETURNING clauses with JSON formatting
  -  - Handles JSON_SERIALIZE expressions
  -  - Processes JSON table column specifications

## Notes and Other Information
- Part of PostgreSQL's comprehensive JSON support introduced in recent versions
- Essential for SQL/JSON standard compliance, particularly for JSON_SERIALIZE and related functions
- The location field enables precise error reporting when JSON format specifications are invalid
- Used in advanced JSON operations including JSON path queries, JSON table functions, and JSON serialization
- Supports various encoding options to ensure proper character handling across different client environments and use cases