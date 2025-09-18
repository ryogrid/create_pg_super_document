# IndexOnlyScanState

## Location
[src/include/nodes/execnodes.h:1699-1718](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/execnodes.h#L1699-L1718)

## Overview
IndexOnlyScanState is an execution state structure that manages the runtime state for index-only scan operations, allowing PostgreSQL to return tuples directly from index data without accessing the heap table when all required columns are available in the index.

## Definition


## Detailed Description
IndexOnlyScanState extends ScanState to provide specialized functionality for index-only scans, a PostgreSQL optimization that can return query results directly from index pages without accessing the underlying heap table. This optimization is particularly effective when the index contains all columns needed by the query and the visibility map indicates that all tuples on the index page are visible to all transactions. The structure manages scan keys for filtering and ordering, runtime key evaluation, visibility map buffers for tuple visibility checks, and parallel scan coordination.

## Parameters / Member Variables
- Netid State   Recv-Q Send-Q                              Local Address:Port       Peer Address:Port      Process
u_str ESTAB   0      0                                               * 16331009              * 16331008         
u_str ESTAB   0      0                                               * 18065001              * 18065000         
u_str ESTAB   0      0                                               * 16883729              * 16883728         
u_str ESTAB   0      0                                               * 16876188              * 16876189         
u_str ESTAB   0      0                                               * 1998                  * 3655             
u_str ESTAB   0      0                                               * 16331008              * 16331009         
u_str ESTAB   0      0                                               * 19581223              * 19581224         
u_str ESTAB   0      0                                               * 16323072              * 16323071         
u_str ESTAB   0      0                                               * 16930623              * 16930624         
u_str ESTAB   0      0                                               * 19581224              * 19581223         
u_str ESTAB   0      0                                               * 16948454              * 16948455         
u_str ESTAB   0      0                                               * 16318726              * 16318727         
u_str ESTAB   0      0                                               * 18061758              * 18061757         
u_str ESTAB   0      0                               /tmp/.X11-unix/X0 15515568              * 15531228         
u_str ESTAB   0      0                                               * 16876182              * 16876183         
u_str ESTAB   0      0                                               * 10305                 * 10304            
u_str ESTAB   0      0                                               * 19581221              * 19581222         
u_str ESTAB   0      0                                               * 16321329              * 16321330         
u_str ESTAB   0      0                                               * 16318723              * 16318722         
u_str ESTAB   0      0                                               * 18061762              * 18061761         
u_str ESTAB   0      0                                               * 16948457              * 16948456         
u_str ESTAB   0      0                                               * 16876189              * 16876188         
u_str ESTAB   0      0                            /tmp/dbus-vEvJ09Fzqf 10314                 * 3654             
u_str ESTAB   0      0                                               * 16321327              * 16321328         
u_str ESTAB   0      0                               /tmp/.X11-unix/X0 15372                 * 3606             
u_str ESTAB   0      0                                               * 16321326              * 16321325         
u_str ESTAB   0      0                                               * 16948458              * 16948459         
u_str ESTAB   0      0                                               * 16876185              * 16876184         
u_str ESTAB   0      0                                               * 19581225              * 19581226         
u_str ESTAB   0      0                                               * 18065005              * 18065004         
u_str ESTAB   0      0                                               * 16323069              * 16323070         
u_str ESTAB   0      0                                               * 18061759              * 18061760         
u_str ESTAB   0      0                                               * 16883731              * 16883730         
u_str ESTAB   0      0                                               * 18065007              * 18065006         
u_str ESTAB   0      0                                               * 16331007              * 16331006         
u_str ESTAB   0      0                                               * 16318725              * 16318724         
u_str ESTAB   0      0                                               * 16930624              * 16930623         
u_str ESTAB   0      0                                               * 16930619              * 16930620         
u_str ESTAB   0      0                                               * 16948461              * 16948460         
u_str ESTAB   0      0                                               * 16876184              * 16876185         
u_str ESTAB   0      0                                               * 18061764              * 18061763         
u_str ESTAB   0      0                                               * 18065000              * 18065001         
u_str ESTAB   0      0                                               * 16883735              * 16883734         
u_str ESTAB   0      0                                               * 15536473              * 15536472         
u_str ESTAB   0      0                                               * 16948455              * 16948454         
u_str ESTAB   0      0                                               * 7569                  * 1891             
u_str ESTAB   0      0                                               * 19581222              * 19581221         
u_str ESTAB   0      0                                               * 11302                 * 11303            
u_str ESTAB   0      0                                               * 16883730              * 16883731         
u_str ESTAB   0      0                                               * 9317                  * 9318             
u_str ESTAB   0      0                                               * 16323070              * 16323069         
u_str ESTAB   0      0                                               * 10304                 * 10305            
u_str ESTAB   0      0                                               * 14343                 * 0                
u_str ESTAB   0      0                                               * 16876183              * 16876182         
u_str ESTAB   0      0      /var/run/docker/containerd/containerd.sock 1891                  * 7569             
u_str ESTAB   0      0                                               * 16323076              * 16323075         
u_str ESTAB   0      0                                               * 16330961              * 16330960         
u_str ESTAB   0      0                                               * 16883733              * 16883732         
u_str ESTAB   0      0                                               * 16323073              * 16323074         
u_str ESTAB   0      0                                               * 16331006              * 16331007         
u_str ESTAB   0      0                                               * 16883732              * 16883733         
u_str ESTAB   0      0                                               * 15536472              * 15536473         
u_str ESTAB   0      0                                               * 9318                  * 9317             
u_str ESTAB   0      0                                               * 16930620              * 16930619         
u_str ESTAB   0      0                                               * 16323075              * 16323076         
u_str ESTAB   0      0                                               * 16331002              * 16331003         
u_str ESTAB   0      0                                               * 16948456              * 16948457         
u_str ESTAB   0      0                                               * 16331004              * 16331005         
u_str ESTAB   0      0                                               * 18065002              * 18065003         
u_str ESTAB   0      0                                               * 16930622              * 16930621         
u_str ESTAB   0      0                                               * 16318724              * 16318725         
u_str ESTAB   0      0                               /tmp/.X11-unix/X0 15515571              * 15519569         
u_str ESTAB   0      0                                               * 16330957              * 16330958         
u_str ESTAB   0      0                                               * 3654                  * 10314            
u_str ESTAB   0      0                                               * 16321323              * 16321324         
u_str ESTAB   0      0                                               * 18061757              * 18061758         
u_str ESTAB   0      0                                               * 16323071              * 16323072         
u_str ESTAB   0      0                                               * 16330960              * 16330961         
u_str ESTAB   0      0                                               * 16321330              * 16321329         
u_str ESTAB   0      0      /var/run/docker/containerd/containerd.sock 4219                  * 15398            
u_str ESTAB   0      0                                               * 3614                  * 3615             
u_str ESTAB   0      0                                               * 16930621              * 16930622         
u_str ESTAB   0      0                                               * 16930626              * 16930625         
u_str ESTAB   0      0                                               * 16948459              * 16948458         
u_str ESTAB   0      0                                               * 16876186              * 16876187         
u_str ESTAB   0      0                                               * 18061763              * 18061764         
u_str ESTAB   0      0                                               * 18065004              * 18065005         
u_str ESTAB   0      0                                               * 16883728              * 16883729         
u_str ESTAB   0      0                                               * 16321325              * 16321326         
u_str ESTAB   0      0                                               * 11303                 * 11302            
u_str ESTAB   0      0                                               * 11300                 * 11301            
u_str ESTAB   0      0                                               * 16323074              * 16323073         
u_str ESTAB   0      0                                               * 16330958              * 16330957         
u_str ESTAB   0      0                                               * 16318722              * 16318723         
u_str ESTAB   0      0                     /mnt/wslg/PulseAudioRDPSink 3655                  * 1998             
u_str ESTAB   0      0                                               * 16876187              * 16876186         
u_str ESTAB   0      0                                               * 16883734              * 16883735         
u_str ESTAB   0      0                                               * 3615                  * 3614             
u_str ESTAB   0      0                                               * 16948460              * 16948461         
u_str ESTAB   0      0                                               * 16331005              * 16331004         
u_str ESTAB   0      0                                               * 16930625              * 16930626         
u_str ESTAB   0      0                                               * 18061761              * 18061762         
u_str ESTAB   0      0                                               * 11301                 * 11300            
u_str ESTAB   0      0                                               * 3606                  * 15372            
u_str ESTAB   0      0                                               * 15531228              * 15515568         
u_str ESTAB   0      0                                               * 18065006              * 18065007         
u_str ESTAB   0      0                                               * 16321328              * 16321327         
u_str ESTAB   0      0                                               * 15398                 * 4219             
u_str ESTAB   0      0                                               * 16318727              * 16318726         
u_str ESTAB   0      0                                               * 18061760              * 18061759         
u_str ESTAB   0      0                                               * 15519569              * 15515571         
u_str ESTAB   0      0                                               * 19581226              * 19581225         
u_str ESTAB   0      0                                               * 16321324              * 16321323         
u_str ESTAB   0      0                                               * 18065003              * 18065002         
u_str ESTAB   0      0                                               * 16331003              * 16331002         
tcp   ESTAB   0      0                                       127.0.0.1:37962         127.0.0.1:37353            
tcp   ESTAB   0      0                                       127.0.0.1:59952         127.0.0.1:37353            
tcp   ESTAB   0      0                                  172.30.249.175:3400       172.30.240.1:60802            
tcp   ESTAB   0      0                                       127.0.0.1:37353         127.0.0.1:37962            
tcp   ESTAB   0      0                                       127.0.0.1:59958         127.0.0.1:37353            
tcp   ESTAB   0      0                                       127.0.0.1:48262         127.0.0.1:37353            
tcp   ESTAB   0      0                                       127.0.0.1:37353         127.0.0.1:48268            
tcp   ESTAB   0      0                                       127.0.0.1:37353         127.0.0.1:37970            
tcp   ESTAB   0      0                                       127.0.0.1:37353         127.0.0.1:48262            
tcp   ESTAB   0      0                                       127.0.0.1:37353         127.0.0.1:59958            
tcp   ESTAB   0      0                                       127.0.0.1:48268         127.0.0.1:37353            
tcp   ESTAB   0      0                                       127.0.0.1:37970         127.0.0.1:37353            
tcp   ESTAB   0      0                                       127.0.0.1:45894         127.0.0.1:37353            
tcp   ESTAB   0      0                                       127.0.0.1:37353         127.0.0.1:45894            
tcp   ESTAB   0      0                                       127.0.0.1:45882         127.0.0.1:37353            
tcp   ESTAB   0      0                                  172.30.249.175:49410     160.79.104.10:https            
tcp   ESTAB   0      0                                  172.30.249.175:59978     160.79.104.10:https            
tcp   ESTAB   0      0                                  172.30.249.175:47144     160.79.104.10:https            
tcp   ESTAB   0      0                                       127.0.0.1:37353         127.0.0.1:45882            
tcp   ESTAB   0      0                                       127.0.0.1:37353         127.0.0.1:59952            
v_str ESTAB   0      0                                               *:633275402             2:50000            
v_str ESTAB   0      0                                               *:633275403             2:50000            
v_str ESTAB   0      0                                               *:633275404             2:50000            
v_str ESTAB   0      0                                               *:633275405             2:50000            
v_str ESTAB   0      0                                               *:633275406             2:50000            
v_str ESTAB   0      0                                               *:633275408             2:50001            
v_str ESTAB   0      0                                               *:633275409             2:50001            
v_str ESTAB   0      0                                               *:633275410             2:50001            
v_str ESTAB   0      0                                               *:633275424             2:50000            
v_str ESTAB   0      0                                               *:633275425             2:50000            
v_str ESTAB   0      0                                               *:633275426             2:50002            
v_str ESTAB   0      0                                               *:633275427             2:50002            
v_str ESTAB   0      0                                               *:633275428             2:50002            
v_str ESTAB   0      0                                               *:633275431             2:50002            
v_str ESTAB   0      0                                               *:633275432             2:50002            
v_str ESTAB   0      0                                               *:633275433             2:50002            
v_str ESTAB   0      0                                               *:1                     2:4102841729       
v_str ESTAB   0      0                                               *:633275411             2:4102841364       
v_str ESTAB   0      0                                               *:633275671             2:342791897        
v_str ESTAB   0      0                                               *:633275674             2:342791913        
v_str ESTAB   0      0                                               *:633275674             2:342791912        
v_str ESTAB   0      0                                               *:633275674             2:342791911        
v_str ESTAB   0      0                                               *:633275674             2:342791910        
v_str ESTAB   0      0                                               *:633275674             2:342791909        
v_str ESTAB   0      0                                               *:633275675             2:342791919        
v_str ESTAB   0      0                                               *:633275672             2:342791902        
v_str ESTAB   0      0                                               *:633275672             2:342791901        
v_str ESTAB   0      0                                               *:633275672             2:342791900        
v_str ESTAB   0      0                                               *:633275672             2:342791899        
v_str ESTAB   0      0                                               *:633275672             2:342791898        
v_str ESTAB   0      0                                               *:633275673             2:342791908        
v_str ESTAB   0      0                                               *:633275676             2:342791924        
v_str ESTAB   0      0                                               *:633275676             2:342791923        
v_str ESTAB   0      0                                               *:633275676             2:342791922        
v_str ESTAB   0      0                                               *:633275676             2:342791921        
v_str ESTAB   0      0                                               *:633275676             2:342791920        
v_str ESTAB   0      0                                               *:633275430             2:4102841703       
v_str ESTAB   0      0                                               *:633275430             2:4102841702       
v_str ESTAB   0      0                                               *:633275430             2:4102841701       
v_str CLOSING 0      0                                               *:633275430             2:4102841700       
v_str ESTAB   0      0                                               *:633275429             2:4102841697       
v_str ESTAB   0      0                                               *:633275435             2:4102841707       
v_str ESTAB   0      0                                               *:633275691             2:342792670        
v_str ESTAB   0      0                                               *:633275694             2:342792686        
v_str ESTAB   0      0                                               *:633275694             2:342792685        
v_str ESTAB   0      0                                               *:633275694             2:342792684        
v_str ESTAB   0      0                                               *:633275694             2:342792683        
v_str ESTAB   0      0                                               *:633275694             2:342792682        
v_str ESTAB   0      0                                               *:633275692             2:342792675        
v_str ESTAB   0      0                                               *:633275692             2:342792674        
v_str ESTAB   0      0                                               *:633275692             2:342792673        
v_str ESTAB   0      0                                               *:633275692             2:342792672        
v_str ESTAB   0      0                                               *:633275692             2:342792671        
v_str ESTAB   0      0                                               *:633275693             2:342792681        
v_str ESTAB   0      0                                               *:633275703             2:342792823        
v_str ESTAB   0      0                                               *:633275706             2:342792839        
v_str ESTAB   0      0                                               *:633275706             2:342792838        
v_str ESTAB   0      0                                               *:633275706             2:342792837        
v_str ESTAB   0      0                                               *:633275706             2:342792836        
v_str ESTAB   0      0                                               *:633275706             2:342792835        
v_str ESTAB   0      0                                               *:633275704             2:342792828        
v_str ESTAB   0      0                                               *:633275704             2:342792827        
v_str ESTAB   0      0                                               *:633275704             2:342792826        
v_str ESTAB   0      0                                               *:633275704             2:342792825        
v_str ESTAB   0      0                                               *:633275704             2:342792824        
v_str ESTAB   0      0                                               *:633275705             2:342792834        
v_str ESTAB   0      0                                               *:633275458             2:4102841830       
v_str ESTAB   0      0                                               *:633275458             2:4102841829       
v_str CLOSING 0      0                                               *:633275458             2:4102841828       
v_str CLOSING 0      0                                               *:633275458             2:4102841827       
v_str ESTAB   0      0                                               *:633275458             2:4102841826       
v_str ESTAB   0      0                                               *:633275711             2:342793593        
v_str ESTAB   0      0                                               *:633275457             2:4102841825       
v_str ESTAB   0      0                                               *:633275462             2:4102842074       
v_str ESTAB   0      0                                               *:633275462             2:4102842073       
v_str ESTAB   0      0                                               *:633275462             2:4102842072       
v_str ESTAB   0      0                                               *:633275462             2:4102842071       
v_str ESTAB   0      0                                               *:633275462             2:4102842070       
v_str ESTAB   0      0                                               *:633275714             2:342793609        
v_str ESTAB   0      0                                               *:633275714             2:342793608        
v_str ESTAB   0      0                                               *:633275714             2:342793607        
v_str ESTAB   0      0                                               *:633275714             2:342793606        
v_str ESTAB   0      0                                               *:633275714             2:342793605        
v_str ESTAB   0      0                                               *:633275463             2:4102842086       
v_str ESTAB   0      0                                               *:633275712             2:342793598        
v_str ESTAB   0      0                                               *:633275712             2:342793597        
v_str ESTAB   0      0                                               *:633275712             2:342793596        
v_str ESTAB   0      0                                               *:633275712             2:342793595        
v_str ESTAB   0      0                                               *:633275712             2:342793594        
v_str ESTAB   0      0                                               *:633275461             2:4102842069       
v_str ESTAB   0      0                                               *:633275713             2:342793604        
v_str ESTAB   0      0                                               *:633275466             2:4102842133       
v_str ESTAB   0      0                                               *:633275466             2:4102842132       
v_str ESTAB   0      0                                               *:633275466             2:4102842131       
v_str ESTAB   0      0                                               *:633275466             2:4102842130       
v_str ESTAB   0      0                                               *:633275466             2:4102842129       
v_str ESTAB   0      0                                               *:633275467             2:4102842156       
v_str ESTAB   0      0                                               *:633275464             2:4102842091       
v_str ESTAB   0      0                                               *:633275464             2:4102842090       
v_str ESTAB   0      0                                               *:633275464             2:4102842089       
v_str ESTAB   0      0                                               *:633275464             2:4102842088       
v_str ESTAB   0      0                                               *:633275464             2:4102842087       
v_str ESTAB   0      0                                               *:633275465             2:4102842128       
v_str ESTAB   0      0                                               *:633275470             2:4102842564       
v_str ESTAB   0      0                                               *:633275470             2:4102842563       
v_str ESTAB   0      0                                               *:633275470             2:4102842562       
v_str ESTAB   0      0                                               *:633275470             2:4102842561       
v_str ESTAB   0      0                                               *:633275470             2:4102842560       
v_str ESTAB   0      0                                               *:633275471             2:4102843465       
v_str ESTAB   0      0                                               *:633275468             2:4102842161       
v_str ESTAB   0      0                                               *:633275468             2:4102842160       
v_str ESTAB   0      0                                               *:633275468             2:4102842159       
v_str ESTAB   0      0                                               *:633275468             2:4102842158       
v_str ESTAB   0      0                                               *:633275468             2:4102842157       
v_str ESTAB   0      0                                               *:633275469             2:4102842559       
v_str ESTAB   0      0                                               *:633275472             2:4102843470       
v_str ESTAB   0      0                                               *:633275472             2:4102843469       
v_str ESTAB   0      0                                               *:633275472             2:4102843468       
v_str ESTAB   0      0                                               *:633275472             2:4102843467       
v_str ESTAB   0      0                                               *:633275472             2:4102843466       : Base ScanState structure containing common scan state information
- : Expression state for recheck qualification expressions that must be re-evaluated
- : Array of ScanKey structures defining the index scan conditions
- : Number of scan keys in the ioss_ScanKeys array
- : Array of ScanKey structures for index ordering operators
- : Number of ordering keys in the ioss_OrderByKeys array
- : Information about scan keys that must be evaluated at runtime
- : Number of runtime keys in the ioss_RuntimeKeys array
- : Boolean flag indicating if runtime scan keys have been computed
- : Expression context for evaluating runtime scan keys
- : Relation descriptor for the index being scanned
- : Index scan descriptor managing the actual index scan state
- : Tuple table slot for holding tuples fetched from the table when heap access is required
- : Buffer containing visibility map data for tuple visibility testing
- : Size of the parallel index-only scan descriptor for parallel execution
- : Array of attribute numbers for name-typed columns that need padding to NAMEDATALEN
- : Number of elements in the ioss_NameCStringAttNums array

## Dependencies
- Functions called/Symbols referenced:
  - [ScanState](../S/ScanState.md)
  - [IndexRuntimeKeyInfo](IndexRuntimeKeyInfo.md)
  - [IndexScanDescData](IndexScanDescData.md)
- Called from (representative examples):
  - [ExecIndexOnlyScan](../E/ExecIndexOnlyScan.md)
  - [ExecInitIndexOnlyScan](../E/ExecInitIndexOnlyScan.md)
  - [ExecEndIndexOnlyScan](../E/ExecEndIndexOnlyScan.md)
  - [ExecReScanIndexOnlyScan](../E/ExecReScanIndexOnlyScan.md)
  - [IndexOnlyNext](IndexOnlyNext.md)
  - [IndexOnlyRecheck](IndexOnlyRecheck.md)

## Notes and Other Information
- Index-only scans are a critical optimization that can significantly improve query performance by avoiding heap access
- The visibility map (VM) is essential for determining when tuples can be returned directly from the index
- When the visibility map indicates potential visibility issues, the scan may need to fall back to checking the heap table
- Parallel index-only scans are supported through the parallel scan descriptor mechanism
- Runtime key evaluation allows for dynamic scan conditions that depend on parameter values or subquery results
- The structure supports both filtering (ScanKeys) and ordering (OrderByKeys) operations directly on the index