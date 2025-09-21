2\. A Brief History of PostgreSQL  
---  
[Prev](intro-whatis.md "1.  What Is PostgreSQL?") | [Up](preface.md "Preface")| Preface| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](notation.md "3. Conventions")  
  
* * *

## 2\. A Brief History of PostgreSQL #

[2.1. The Berkeley POSTGRES Project](history.md#HISTORY-BERKELEY)
[2.2. Postgres95](history.md#HISTORY-POSTGRES95)
[2.3. PostgreSQL](history.md#HISTORY-POSTGRESQL)

The object-relational database management system now known as PostgreSQL is derived from the POSTGRES package written at the University of California at Berkeley. With decades of development behind it, PostgreSQL is now the most advanced open-source database available anywhere. 

### 2.1. The Berkeley POSTGRES Project #

The POSTGRES project, led by Professor Michael Stonebraker, was sponsored by the Defense Advanced Research Projects Agency (DARPA), the Army Research Office (ARO), the National Science Foundation (NSF), and ESL, Inc. The implementation of POSTGRES began in 1986. The initial concepts for the system were presented in [[ston86]](biblio.md#STON86), and the definition of the initial data model appeared in [[rowe87]](biblio.md#ROWE87). The design of the rule system at that time was described in [[ston87a]](biblio.md#STON87A). The rationale and architecture of the storage manager were detailed in [[ston87b]](biblio.md#STON87B). 

POSTGRES has undergone several major releases since then. The first “demoware” system became operational in 1987 and was shown at the 1988 ACM-SIGMOD Conference. Version 1, described in [[ston90a]](biblio.md#STON90A), was released to a few external users in June 1989. In response to a critique of the first rule system ([[ston89]](biblio.md#STON89)), the rule system was redesigned ([[ston90b]](biblio.md#STON90B)), and Version 2 was released in June 1990 with the new rule system. Version 3 appeared in 1991 and added support for multiple storage managers, an improved query executor, and a rewritten rule system. For the most part, subsequent releases until Postgres95 (see below) focused on portability and reliability. 

POSTGRES has been used to implement many different research and production applications. These include: a financial data analysis system, a jet engine performance monitoring package, an asteroid tracking database, a medical information database, and several geographic information systems. POSTGRES has also been used as an educational tool at several universities. Finally, Illustra Information Technologies (later merged into [Informix](https://www.ibm.com/analytics/informix), which is now owned by [IBM](https://www.ibm.com/)) picked up the code and commercialized it. In late 1992, POSTGRES became the primary data manager for the Sequoia 2000 scientific computing project described in [[ston92]](biblio.md#STON92). 

The size of the external user community nearly doubled during 1993. It became increasingly obvious that maintenance of the prototype code and support was taking up large amounts of time that should have been devoted to database research. In an effort to reduce this support burden, the Berkeley POSTGRES project officially ended with Version 4.2. 

### 2.2. Postgres95 #

In 1994, Andrew Yu and Jolly Chen added an SQL language interpreter to POSTGRES. Under a new name, Postgres95 was subsequently released to the web to find its own way in the world as an open-source descendant of the original POSTGRES Berkeley code. 

Postgres95 code was completely ANSI C and trimmed in size by 25%. Many internal changes improved performance and maintainability. Postgres95 release 1.0.x ran about 30–50% faster on the Wisconsin Benchmark compared to POSTGRES, Version 4.2. Apart from bug fixes, the following were the major enhancements: 

  * The query language PostQUEL was replaced with SQL (implemented in the server). (Interface library [libpq](libpq.md "Chapter 32. libpq — C Library") was named after PostQUEL.) Subqueries were not supported until PostgreSQL (see below), but they could be imitated in Postgres95 with user-defined SQL functions. Aggregate functions were re-implemented. Support for the `GROUP BY` query clause was also added. 

  * A new program (psql) was provided for interactive SQL queries, which used GNU Readline. This largely superseded the old monitor program. 

  * A new front-end library, `libpgtcl`, supported Tcl-based clients. A sample shell, `pgtclsh`, provided new Tcl commands to interface Tcl programs with the Postgres95 server. 

  * The large-object interface was overhauled. The inversion large objects were the only mechanism for storing large objects. (The inversion file system was removed.) 

  * The instance-level rule system was removed. Rules were still available as rewrite rules. 

  * A short tutorial introducing regular SQL features as well as those of Postgres95 was distributed with the source code 

  * GNU make (instead of BSD make) was used for the build. Also, Postgres95 could be compiled with an unpatched GCC (data alignment of doubles was fixed). 




### 2.3. PostgreSQL #

By 1996, it became clear that the name “Postgres95” would not stand the test of time. We chose a new name, PostgreSQL, to reflect the relationship between the original POSTGRES and the more recent versions with SQL capability. At the same time, we set the version numbering to start at 6.0, putting the numbers back into the sequence originally begun by the Berkeley POSTGRES project. 

Postgres is still considered an official project name, both because of tradition and because people find it easier to pronounce Postgres than PostgreSQL. 

The emphasis during development of Postgres95 was on identifying and understanding existing problems in the server code. With PostgreSQL, the emphasis has shifted to augmenting features and capabilities, although work continues in all areas. 

Details about what has happened in PostgreSQL since then can be found in [Appendix E](release.md "Appendix E. Release Notes"). 

* * *

[Prev](intro-whatis.md "1.  What Is PostgreSQL?") | [Up](preface.md "Preface")|  [Next](notation.md "3. Conventions")  
---|---|---  
1\. What Is PostgreSQL? | [Home](index.md "PostgreSQL 17.5 Documentation")|  3\. Conventions
