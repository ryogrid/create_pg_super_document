# Point

## Location
src/include/utils/geo_decls.h: 100 - 108

## Overview
Point is a fundamental geometric data type in PostgreSQL that represents a 2D point with x and y coordinates using double precision floating point numbers.

## Definition


## Detailed Description
Point is the most basic geometric data type in PostgreSQL's geometric type system. It stores a single point in 2D space using two  (double precision) values for the x and y coordinates. This type serves as the foundation for more complex geometric types like LSEG (line segment), BOX, PATH, POLYGON, and CIRCLE, which all use Point structures internally.

The Point type is extensively used throughout PostgreSQL's geometric operations and indexing systems, particularly in GiST (Generalized Search Tree) and SP-GiST (Space-partitioned Generalized Search Tree) indexes for spatial queries.

## Parameters / Member Variables
- : The x-coordinate of the point (double precision floating point)
- : The y-coordinate of the point (double precision floating point)

## Dependencies
- Functions called/Symbols referenced:
  - float8 (PostgreSQL's double precision type)
  
- Called from (representative examples):
  - LSEG (line segment structure)
  - BOX (bounding box structure) 
  - PATH (geometric path structure)
  - POLYGON (polygon structure)
  - CIRCLE (circle structure)
  - point_in/point_out (I/O functions)
  - point_distance (distance calculations)
  - GiST and SP-GiST index operations
  - Various geometric operators and functions

## Notes and Other Information
- Point coordinates use PostgreSQL's float8 type, providing double precision accuracy
- Point is used as a building block for all other geometric types in PostgreSQL
- The type supports standard geometric operations like distance calculations, containment tests, and spatial indexing
- Point values are typically represented in text format as '(x,y)' 
- The type is heavily optimized for spatial index operations in both GiST and SP-GiST access methods
- Point arithmetic operations (addition, subtraction, multiplication, division) are supported for geometric transformations