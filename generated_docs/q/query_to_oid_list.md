# query_to_oid_list

## Location
src/backend/utils/adt/xml.c: 2786 - 2814

## Overview
Executes a SQL query that returns OID values and converts the results into a List of Oids for use in XML mapping operations.

## Definition


## Detailed Description
This function serves as a utility for XML mapping operations in PostgreSQL, specifically designed to execute SQL queries that return object identifiers (OIDs) as their first column. It uses the Server Programming Interface (SPI) to execute the provided query and collects all non-null OID values from the result set into a PostgreSQL List data structure. The function is part of the SQL to XML mapping infrastructure that supports visibility determination for database objects in XML output generation.

## Parameters / Member Variables
- : A SQL query string that must return type oid as the first column

## Dependencies
- Functions called/Symbols referenced:
  - SPI_execute
  - SPI_OK_SELECT
  - SPI_result_code_string
  - SPI_getbinval
  - lappend_oid
  - DatumGetObjectId
- Called from (representative examples):
  - schema_get_xml_visible_tables
  - database_get_xml_visible_schemas
  - database_get_xml_visible_tables

## Notes and Other Information
- This is a static function, only accessible within the xml.c file
- Part of the SQL/XML:2008 standard implementation for object visibility
- Uses SPI (Server Programming Interface) to execute queries within the backend
- Filters out NULL values from the result set
- Returns NIL (empty list) if no valid OIDs are found
- Error handling includes logging with specific SPI result codes