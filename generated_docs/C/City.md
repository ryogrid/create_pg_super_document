# City

## Location
src/include/optimizer/geqo_recombination.h: 62 - 68

## Overview
The City struct is a data structure used in PostgreSQL's Genetic Query Optimizer (GEQO) for various crossover operations during genetic algorithm-based query optimization, representing positions and states of elements during recombination processes.

## Definition

```c
typedef struct City
{
	int			tour2_position;
	int			tour1_position;
	int			used;
	int			select_list;
}			City;
```
## Detailed Description
The City structure is used in multiple genetic crossover algorithms within GEQO, including cycle crossover (CX), order crossover variants (OX1, OX2), and position crossover (PX). Despite its name suggesting a geographic context, this structure represents abstract positions and states during genetic recombination operations in query optimization. Each City tracks positions in two different parent tours and maintains flags for usage status and selection criteria during the crossover process.

The structure serves as a mapping and tracking mechanism that allows different crossover algorithms to maintain correspondence between elements in parent solutions while constructing offspring solutions. This is crucial for preserving beneficial characteristics from parent query plans during genetic optimization.

## Parameters / Member Variables
- : Integer representing the position of this element in the second parent tour/solution
- : Integer representing the position of this element in the first parent tour/solution  
- : Integer flag indicating whether this city/element has been used during the current crossover operation
- : Integer flag or identifier used for selection criteria during crossover operations

## Dependencies
- Functions called/Symbols referenced:
  - None (this is a simple data structure with no direct symbol dependencies)
- Called from (representative examples):
  - cx (cycle crossover algorithm)
  - ox1 (order crossover algorithm variant 1)
  - ox2 (order crossover algorithm variant 2) 
  - px (position crossover algorithm)
  - init_tour (tour initialization function)
  - alloc_city_table (allocates arrays of City structures)
  - free_city_table (deallocates City structure arrays)

## Notes and Other Information
- The City structure is used across multiple different crossover algorithms in GEQO, making it a versatile data structure for genetic operations
- The naming convention reflects the historical origins of genetic algorithms in solving traveling salesman problems, where 'cities' were literal geographic locations
- In the context of PostgreSQL query optimization, each 'city' typically represents a relation or join operation in the query plan
- The dual position tracking (tour1_position and tour2_position) enables crossover algorithms to maintain mappings between corresponding elements in different parent solutions
- This structure is essential for implementing sophisticated genetic operators that preserve structural relationships while allowing beneficial variations in query plan optimization