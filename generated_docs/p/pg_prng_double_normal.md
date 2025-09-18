# pg_prng_double_normal

## Location
src/common/pg_prng.c: 290 - 312

## Overview
Generates a random double precision floating-point number from the standard normal distribution (mean = 0.0, standard deviation = 1.0) using the Box-Muller transformation algorithm.

## Definition
```c
double pg_prng_double_normal(pg_prng_state *state)
```

## Detailed Description
This function implements the Box-Muller transform to convert two independent uniformly distributed random numbers into a normally distributed random number. The function generates random numbers from the standard normal distribution (Gaussian distribution with mean 0 and standard deviation 1).

The implementation uses the basic version of the Box-Muller transform:
1. Generates two uniform random numbers u1 and u2 from the range (0, 1]
2. Applies the mathematical transformation: z0 = sqrt(-2 * ln(u1)) * sin(2π * u2)
3. Returns the normally distributed value z0

To obtain values from a different normal distribution, the result should be scaled and shifted: .

The function carefully handles the domain requirements of the Box-Muller transform by ensuring the uniform random numbers are in the range (0, 1] rather than [0, 1) to avoid computing log(0).

## Parameters / Member Variables
- : Pointer to the pseudo-random number generator state structure that maintains the internal state for generating random numbers

## Dependencies
- Functions called/Symbols referenced:
  - pg_prng_double: Generates uniform random doubles in [0, 1)
  - M_PI: Mathematical constant π
  - sqrt: Square root function
  - log: Natural logarithm function
  - sin: Sine function
- Called from (representative examples):
  - drandom_normal: Database function for generating normal random numbers
  - getGaussianRand: pgbench utility function for Gaussian random generation

## Notes and Other Information
- Implements the Box-Muller transform algorithm as described in https://en.wikipedia.org/wiki/Box–Muller_transform
- The function generates one normal-valued output per call (the Box-Muller transform actually produces two independent normal values, but this implementation only returns one)
- Care is taken to avoid numerical issues by ensuring the input to log() is never zero
- The generated values follow the standard normal distribution N(0,1)
- For different normal distributions, users should apply linear transformation to the result
- Located in src/common/pg_prng.c at lines 290-312