# make_attrmap

## Location
src/backend/access/common/attmap.c: 40 - 55

## Overview
A utility function that allocates and initializes an attribute map structure in the current memory context, providing the foundation for PostgreSQL's attribute mapping functionality.

## Definition


## Detailed Description
The  function creates a new attribute map structure () with a specified length. It allocates memory for both the main AttrMap structure and its internal array of attribute numbers (). The function uses  to ensure that all allocated memory is zero-initialized, providing a clean starting state for the attribute map. This is a foundational utility used by other attribute mapping functions to establish the basic data structure.

## Parameters / Member Variables
- : The length of the attribute map, specifying how many attribute number mappings this map can hold

## Dependencies
- Functions called/Symbols referenced:
  -  (memory allocation function)
  -  (structure type)
  -  (attribute number type)
- Called from (representative examples):
  - 
  - 
  - 
  - 
  - 

## Notes and Other Information
- Memory is allocated in the current memory context, so cleanup depends on the context lifecycle
- Uses zero-initialization () to ensure predictable initial state
- Serves as the basic constructor for attribute mapping structures used throughout PostgreSQL
- Located in 