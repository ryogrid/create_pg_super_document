# descriptor

## Location
[src/interfaces/ecpg/preproc/type.h:203-209](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/preproc/type.h#L203-L209)

## Overview
A structure used in ECPG (Embedded C for PostgreSQL) to store SQL descriptor information, managing result sets and their associated metadata in embedded C applications.

## Definition


## Detailed Description
The  struct is a core component of ECPG's descriptor management system, designed to handle SQL descriptors in embedded C programs. It forms a linked list of named descriptors, each containing a PostgreSQL result set and associated metadata. This structure is essential for managing dynamic SQL operations where the structure of result sets may not be known at compile time.

Each descriptor maintains a reference to a PGresult object (the actual query result from libpq), a count of items, and a linked list of descriptor_item structures that provide detailed metadata about each column or parameter. The descriptor system allows ECPG applications to introspect and manipulate SQL results dynamically.

## Parameters / Member Variables
- : String identifier for the descriptor, used for lookup and management operations
- : Pointer to the PGresult structure containing the actual query results from libpq
- : Pointer to the next descriptor in the linked list, enabling multiple named descriptors
- : Number of items (columns/parameters) contained in this descriptor
- : Pointer to the first descriptor_item in a linked list containing detailed metadata for each column

## Dependencies
- Functions called/Symbols referenced:
  -  (struct type from ecpglib_extern.h)
  -  (libpq result structure)
- Called from (representative examples):
  -  (descriptor.c:794, 805, 831)
  -  (descriptor.c:750, 751)
  -  (descriptor.c:907)
  -  (descriptor.c:834)
  -  (descriptor.c:728)
  -  (execute.c:2293, 2296)
  -  (preproc/descriptor.c:78, 83)
  -  (preproc/descriptor.c:101, 102, 129)

## Notes and Other Information
- Located in the ECPG library external interface (src/interfaces/ecpg/ecpglib/ecpglib_extern.h:115-123)
- Central to ECPG's dynamic SQL support, allowing runtime inspection of result structures
- Used extensively in both the ECPG preprocessor and runtime library
- Integrates with PostgreSQL's libpq library through the PGresult pointer
- Supports descriptor-based SQL operations as defined by the SQL standard
- The descriptor system provides a standardized way to handle variable result set structures in embedded SQL applications
- Thread-safe operations depend on proper synchronization of the descriptor list