# check_publications

## Location
[src/backend/commands/subscriptioncmds.c:486-548](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/subscriptioncmds.c#L486-L548)

## Overview
Validates that all specified publications exist on the publisher server by querying the remote pg_publication catalog and reports warnings for any missing publications.

## Definition


## Detailed Description
This function verifies the existence of publications on the publisher side during subscription creation or modification. It constructs and executes a SQL query against the publisher's pg_publication catalog to check if all specified publications are present. The function uses the WAL receiver connection to communicate with the publisher.

The validation process involves:
1. Building a SQL query with an IN clause containing all publication names
2. Executing the query on the publisher using the WAL receiver connection
3. Processing the returned tuples to identify which publications exist
4. Comparing the results against the originally requested list
5. Reporting warnings for any publications that don't exist on the publisher

This validation helps prevent subscription creation with non-existent publications, which would lead to replication failures. The function issues warnings rather than errors, allowing administrators to proceed if they understand the implications.

## Parameters / Member Variables
- : Active WAL receiver connection to the publisher server
- : List of publication names to validate on the publisher

## Dependencies
- Functions called/Symbols referenced:
  - [get_publications_str](../g/get_publications_str.md): Formats publication list for SQL query
  - walrcv_exec: Executes SQL query on publisher via WAL receiver
  - makeStringInfo/destroyStringInfo: String buffer management
  - [MakeSingleTupleTableSlot](../M/MakeSingleTupleTableSlot.md)/ExecDropSingleTupleTableSlot: Tuple processing utilities
  - [tuplestore_gettupleslot](../t/tuplestore_gettupleslot.md): Retrieves query result tuples
  - [list_copy](../l/list_copy.md)/list_delete: List manipulation for tracking missing publications
  - TextDatumGetCString: Extracts string data from query results
- Called from (representative examples):
  - [CreateSubscription](../C/CreateSubscription.md): During subscription creation to validate publication list

## Notes and Other Information
- The function issues warnings rather than errors for missing publications, allowing subscription creation to proceed
- Uses errmsg_plural to provide grammatically correct error messages for single vs multiple missing publications
- The SQL query targets pg_catalog.pg_publication to ensure it works across PostgreSQL versions
- Creates a copy of the original publication list to track which publications are found during result processing
- [Query](../Q/Query.md) results are processed using tuple table slots for efficient data access
- WAL receiver connection must be established and valid before calling this function