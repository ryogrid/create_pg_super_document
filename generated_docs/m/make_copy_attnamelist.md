# make_copy_attnamelist

## Location
[src/backend/replication/logical/tablesync.c:724-743](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/tablesync.c#L724-L743)

## Overview
Creates a list of column names for PostgreSQL's COPY command based on the attribute information from a logical replication relation mapping.

## Definition

```c
static List *
make_copy_attnamelist(LogicalRepRelMapEntry *rel)
```
## Detailed Description
The  function constructs a list of column names that will be used in PostgreSQL's COPY command during logical replication table synchronization. It iterates through all attributes of the remote relation stored in the  and creates string nodes for each column name.

This function is essential for table synchronization operations where data needs to be copied from the publisher to the subscriber. The returned list contains the exact column names from the remote (publisher) relation, which ensures that the COPY command targets the correct columns in the proper order.

The function uses PostgreSQL's list manipulation functions to build a list of  nodes, where each node contains the name of one column from the remote relation.

## Parameters / Member Variables
- : Pointer to a LogicalRepRelMapEntry structure containing the logical replication relation mapping information, including the remote relation details with column names and count

## Dependencies
- Functions called/Symbols referenced:
  - [lappend](../l/lappend.md) (list manipulation function)
  - [makeString](makeString.md) (creates a String node for each column name)
  - [LogicalRepRelMapEntry](../L/LogicalRepRelMapEntry.md) (structure type)
  - NIL (empty list constant)

- Called from (representative examples):
  - [copy_table](../c/copy_table.md) (uses the returned list for COPY operations during table synchronization)

## Notes and Other Information
- Located in src/backend/replication/logical/tablesync.c:724-743
- This is a static helper function used internally within the tablesync module
- The function accesses  to determine the number of attributes and  to get each column name
- The returned list contains  nodes that can be directly used with PostgreSQL's COPY command infrastructure
- Memory for the list and string nodes is allocated in the current memory context
- The function assumes that the LogicalRepRelMapEntry has been properly initialized with valid remote relation information