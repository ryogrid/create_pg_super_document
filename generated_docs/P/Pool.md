# Pool

## Location
[src/include/optimizer/geqo_gene.h:38-43](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/optimizer/geqo_gene.h#L38-L43)

## Overview
A structure representing a population of chromosomes in PostgreSQL's Genetic Query Optimizer (GEQO), managing a collection of potential solutions for join ordering optimization.

## Definition

```c
typedef struct Pool
{
	Chromosome *data;
	int			size;
	int			string_length;
} Pool;
```
## Detailed Description
The Pool structure is a fundamental component of PostgreSQL's Genetic Query Optimizer (GEQO) that represents a population of chromosomes in the genetic algorithm. It manages a collection of potential solutions (chromosomes) for complex join ordering problems. The pool serves as the working set for genetic algorithm operations such as selection, crossover, and mutation.

During query optimization, when the number of relations exceeds the threshold for exhaustive search, GEQO creates and maintains pools of chromosomes representing different join orders. The genetic algorithm evolves these pools over multiple generations to find near-optimal solutions.

## Parameters / Member Variables
- : A pointer to an array of Chromosome structures representing the population of potential solutions
- : The number of chromosomes currently in the pool (population size)
- : The length of the gene string in each chromosome, typically corresponding to the number of relations in the query

## Dependencies
- Functions called/Symbols referenced:
  - [Chromosome](../C/Chromosome.md)
- Called from (representative examples):
  - [geqo](../g/geqo.md) (main genetic algorithm function)
  - [avg_pool](../a/avg_pool.md)
  - [print_pool](../p/print_pool.md)
  - [print_gen](../p/print_gen.md)
  - [alloc_pool](../a/alloc_pool.md)
  - [free_pool](../f/free_pool.md)
  - [random_init_pool](../r/random_init_pool.md)
  - [sort_pool](../s/sort_pool.md)
  - [spread_chromo](../s/spread_chromo.md)
  - [geqo_selection](../g/geqo_selection.md)

## Notes and Other Information
- The Pool structure is central to the genetic algorithm implementation in PostgreSQL's query optimizer
- Pools are dynamically allocated and contain arrays of chromosomes that represent the current population
- The  member determines how many chromosomes are active in the current generation
- The  member ensures all chromosomes in the pool have consistent gene string lengths
- [Pool](Pool.md) management functions handle allocation, initialization, sorting, and deallocation of chromosome populations
- The genetic algorithm typically maintains one or more pools and performs operations like selection and reproduction between them
- [Pool](Pool.md) operations are essential for GEQO's ability to handle complex queries with many join relations efficiently