# ox1

## Location
src/backend/optimizer/geqo/geqo_ox1.c: 49 - 96

## Overview
The ox1 function implements the Order Crossover 1 (OX1) genetic algorithm crossover operator used in PostgreSQL's GEQO (Genetic Query Optimizer) for generating offspring from two parent gene sequences.

## Definition


## Detailed Description
The ox1 function performs position-based crossover between two parent tours (gene sequences) to produce an offspring tour. This is a standard genetic algorithm crossover technique specifically designed for permutation problems like the Traveling Salesman Problem (TSP), which GEQO uses as an analogy for query optimization.

The algorithm works by:
1. Selecting a random contiguous segment from the first parent tour
2. Copying this segment to the same positions in the offspring
3. Filling the remaining positions with elements from the second parent tour in the order they appear, skipping elements already copied from the first parent
4. Using a circular approach to maintain the permutation property

The crossover preserves the relative order of elements from the second parent while incorporating a contiguous segment from the first parent, helping maintain genetic diversity while preserving beneficial gene sequences.

## Parameters / Member Variables
- : PlannerInfo structure containing planner state and random number generation context
- : First parent gene sequence (array of Gene elements)
- : Second parent gene sequence (array of Gene elements) 
- : Output array where the resulting offspring gene sequence is stored
- : Number of genes in the sequences (length of the arrays)
- : Array of City structures used to track which genes have been used during crossover

## Dependencies
- Functions called/Symbols referenced:
  - geqo_randint (for generating random segment boundaries)
  - Gene (typedef for gene representation)
  - City (structure with 'used' field for tracking gene usage)
- Called from (representative examples):
  - geqo (main GEQO algorithm function in geqo_main.c:213)

## Notes and Other Information
- This is one of several crossover operators available in PostgreSQL's GEQO implementation
- The function uses modular arithmetic to handle circular array indexing
- The city_table array must be properly sized (at least num_gene + 1 elements) and is used as temporary storage
- The crossover operator is designed to preserve the permutation property essential for TSP-like problems
- Part of the genetic algorithm suite used for optimizing complex join orders in PostgreSQL query planning