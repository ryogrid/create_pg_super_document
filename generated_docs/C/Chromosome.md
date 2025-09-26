# Chromosome

## Location
src/include/optimizer/geqo_gene.h: 32 - 36

## Overview
A structure representing a chromosome in PostgreSQL's Genetic Query Optimizer (GEQO), which contains a gene sequence and its associated fitness value.

## Definition

```c
typedef struct Chromosome
{
	Gene	   *string;
	Cost		worth;
} Chromosome;
```
## Detailed Description
The Chromosome structure is a core component of PostgreSQL's Genetic Query Optimizer (GEQO), which uses genetic algorithms to solve complex join ordering problems. Each chromosome represents a potential solution (a specific join order) in the genetic algorithm's population. The chromosome contains a sequence of genes (representing relations/tables) and a cost value that indicates the fitness of this particular solution.

The GEQO module is used when the number of relations in a query exceeds a threshold (typically 12), where exhaustive search becomes impractical. The genetic algorithm evolves populations of chromosomes through selection, crossover, and mutation operations to find near-optimal join orders.

## Parameters / Member Variables
- : A pointer to an array of Gene values representing the sequence of relations in a particular join order
- : The cost (fitness value) associated with this chromosome, representing the estimated execution cost of the join order it represents

## Dependencies
- Functions called/Symbols referenced:
  - Gene
  - Cost
- Called from (representative examples):
  - geqo_copy
  - geqo (main genetic algorithm function)
  - alloc_pool
  - free_pool
  - random_init_pool
  - sort_pool
  - alloc_chromo
  - free_chromo
  - spread_chromo
  - geqo_selection

## Notes and Other Information
- The Chromosome structure is defined in the GEQO (Genetic Query Optimizer) module, which was contributed by Martin Utesch from the University of Mining and Technology, Freiberg, Germany
- This structure is fundamental to the genetic algorithm approach used for query optimization in PostgreSQL
- The  member points to an array of integers (Gene typedef) where each Gene represents a relation ID
- The  member stores the Cost type, which represents the estimated execution cost for the join order represented by this chromosome
- Chromosomes are typically managed in pools (Pool structure) during the genetic algorithm execution
- The genetic algorithm operations (selection, crossover, mutation) work on populations of these chromosomes to evolve better solutions over generations