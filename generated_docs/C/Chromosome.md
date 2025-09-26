# Chromosome

## Location
[src/include/optimizer/geqo_gene.h:32-36](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/optimizer/geqo_gene.h#L32-L36)

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
- `*string`: A pointer to an array of Gene values representing the sequence of relations in a particular join order
- `worth`: The cost (fitness value) associated with this chromosome, representing the estimated execution cost of the join order it represents
## Dependencies
- Functions called/Symbols referenced:
  - [Gene](../G/Gene.md)
  - Cost
- Called from (representative examples):
  - [geqo_copy](../g/geqo_copy.md)
  - [geqo](../g/geqo.md) (main genetic algorithm function)
  - [alloc_pool](../a/alloc_pool.md)
  - [free_pool](../f/free_pool.md)
  - [random_init_pool](../r/random_init_pool.md)
  - [sort_pool](../s/sort_pool.md)
  - [alloc_chromo](../a/alloc_chromo.md)
  - [free_chromo](../f/free_chromo.md)
  - [spread_chromo](../s/spread_chromo.md)
  - [geqo_selection](../g/geqo_selection.md)

## Notes and Other Information
- The Chromosome structure is defined in the GEQO (Genetic Query Optimizer) module, which was contributed by Martin Utesch from the University of Mining and Technology, Freiberg, Germany
- This structure is fundamental to the genetic algorithm approach used for query optimization in PostgreSQL
- The  member points to an array of integers (Gene typedef) where each Gene represents a relation ID
- The  member stores the Cost type, which represents the estimated execution cost for the join order represented by this chromosome
- Chromosomes are typically managed in pools (Pool structure) during the genetic algorithm execution
- The genetic algorithm operations (selection, crossover, mutation) work on populations of these chromosomes to evolve better solutions over generations