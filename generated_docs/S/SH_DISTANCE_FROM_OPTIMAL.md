# SH_DISTANCE_FROM_OPTIMAL

## Location
src/include/lib/simplehash.h: 386 - 394

## Overview
Calculates the distance between a hash bucket's actual position and its optimal position in the PostgreSQL simplehash open-addressing hash table implementation.

## Definition


## Detailed Description
This function is a core component of the Robin Hood hashing algorithm used in PostgreSQL's simplehash implementation. It calculates how far an element is from its ideal position in the hash table, which is crucial for the Robin Hood hashing strategy that optimizes average lookup performance by minimizing variance in probe distances.

The function handles wraparound in the circular hash table array. When the optimal position is before or at the current bucket position, it returns the simple difference. When the optimal position wraps around (optimal > bucket), it calculates the distance considering the circular nature of the hash table.

This distance metric is used during insertion to implement the "Robin Hood" strategy - if a new element to be inserted has a greater distance from its optimal position than an existing element, the existing element is displaced to make room for the "poorer" (further from optimal) new element.

## Parameters / Member Variables
- : Pointer to the hash table structure containing size information
- : The optimal bucket position for an element (calculated by SH_INITIAL_BUCKET)
- : The actual current bucket position of the element

## Dependencies
- Functions called/Symbols referenced:
  - SH_MAKE_NAME (macro for name generation)
  - [SH_TYPE](SH_TYPE.md) (hash table type)
- Called from (representative examples):
  - [SH_INSERT_HASH_INTERNAL](SH_INSERT_HASH_INTERNAL.md) (during element insertion for Robin Hood algorithm)
  - [SH_STAT](SH_STAT.md) (for hash table statistics and analysis)

## Notes and Other Information
- This is an internal helper function for the simplehash template system
- The macro SH_DISTANCE_FROM_OPTIMAL expands to a function name with the user-defined prefix
- Essential for Robin Hood hashing performance optimization
- Handles circular wraparound in hash table bucket addressing
- Used to maintain optimal fill factors and minimize clustering effects in open addressing