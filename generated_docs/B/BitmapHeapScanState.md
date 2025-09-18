# BitmapHeapScanState

## Location
[src/include/nodes/execnodes.h:1815-1833](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/execnodes.h#L1815-L1833)

## Overview
BitmapHeapScanState is an execution state structure that manages bitmap heap scan operations, retrieving actual tuples from heap pages identified by TID bitmaps created by bitmap index scans, with support for prefetching and parallel execution.

## Definition


## Detailed Description
BitmapHeapScanState extends ScanState to implement the heap-scanning portion of PostgreSQL's bitmap scan strategy. It takes TID bitmaps produced by bitmap index scans and efficiently retrieves the corresponding tuples from heap pages in physical storage order, minimizing random I/O. The structure supports both exact and lossy page representations, implements sophisticated prefetching mechanisms to optimize I/O performance, and provides full support for parallel execution where multiple workers can collaborate on scanning the same bitmap. It maintains statistics on exact vs lossy pages and dynamically adjusts prefetch distances based on system performance.

## Parameters / Member Variables
- Netid State      Recv-Q Send-Q                              Local Address:Port       Peer Address:Port      Process
u_str ESTAB      0      0                                               * 16331009              * 16331008         
u_str ESTAB      0      0                                               * 18065001              * 18065000         
u_str ESTAB      0      0                                               * 16883729              * 16883728         
u_str ESTAB      0      0                                               * 19603078              * 19603079         
u_str ESTAB      0      0                                               * 16876188              * 16876189         
u_str ESTAB      0      0                                               * 1998                  * 3655             
u_str ESTAB      0      0                                               * 16331008              * 16331009         
u_str ESTAB      0      0                                               * 19603083              * 19603082         
u_str ESTAB      0      0                                               * 16323072              * 16323071         
u_str ESTAB      0      0                                               * 16930623              * 16930624         
u_str ESTAB      0      0                                               * 16948454              * 16948455         
u_str ESTAB      0      0                                               * 16318726              * 16318727         
u_str ESTAB      0      0                                               * 18061758              * 18061757         
u_str ESTAB      0      0                               /tmp/.X11-unix/X0 15515568              * 15531228         
u_str ESTAB      0      0                                               * 16876182              * 16876183         
u_str ESTAB      0      0                                               * 10305                 * 10304            
u_str ESTAB      0      0                                               * 16321329              * 16321330         
u_str ESTAB      0      0                                               * 16318723              * 16318722         
u_str ESTAB      0      0                                               * 18061762              * 18061761         
u_str ESTAB      0      0                                               * 16948457              * 16948456         
u_str ESTAB      0      0                                               * 16876189              * 16876188         
u_str ESTAB      0      0                            /tmp/dbus-vEvJ09Fzqf 10314                 * 3654             
u_str ESTAB      0      0                                               * 16321327              * 16321328         
u_str ESTAB      0      0                               /tmp/.X11-unix/X0 15372                 * 3606             
u_str ESTAB      0      0                                               * 16321326              * 16321325         
u_str ESTAB      0      0                                               * 16948458              * 16948459         
u_str ESTAB      0      0                                               * 16876185              * 16876184         
u_str ESTAB      0      0                                               * 18065005              * 18065004         
u_str ESTAB      0      0                                               * 16323069              * 16323070         
u_str ESTAB      0      0                                               * 18061759              * 18061760         
u_str ESTAB      0      0                                               * 16883731              * 16883730         
u_str ESTAB      0      0                                               * 18065007              * 18065006         
u_str ESTAB      0      0                                               * 16331007              * 16331006         
u_str ESTAB      0      0                                               * 16318725              * 16318724         
u_str ESTAB      0      0                                               * 16930624              * 16930623         
u_str ESTAB      0      0                                               * 16930619              * 16930620         
u_str ESTAB      0      0                                               * 19603079              * 19603078         
u_str ESTAB      0      0                                               * 16948461              * 16948460         
u_str ESTAB      0      0                                               * 16876184              * 16876185         
u_str ESTAB      0      0                                               * 18061764              * 18061763         
u_str ESTAB      0      0                                               * 18065000              * 18065001         
u_str ESTAB      0      0                                               * 16883735              * 16883734         
u_str ESTAB      0      0                                               * 15536473              * 15536472         
u_str ESTAB      0      0                                               * 19603082              * 19603083         
u_str ESTAB      0      0                                               * 16948455              * 16948454         
u_str ESTAB      0      0                                               * 7569                  * 1891             
u_str ESTAB      0      0                                               * 11302                 * 11303            
u_str ESTAB      0      0                                               * 16883730              * 16883731         
u_str ESTAB      0      0                                               * 9317                  * 9318             
u_str ESTAB      0      0                                               * 16323070              * 16323069         
u_str ESTAB      0      0                                               * 10304                 * 10305            
u_str ESTAB      0      0                                               * 14343                 * 0                
u_str ESTAB      0      0                                               * 16876183              * 16876182         
u_str ESTAB      0      0      /var/run/docker/containerd/containerd.sock 1891                  * 7569             
u_str ESTAB      0      0                                               * 16323076              * 16323075         
u_str ESTAB      0      0                                               * 16330961              * 16330960         
u_str ESTAB      0      0                                               * 16883733              * 16883732         
u_str ESTAB      0      0                                               * 16323073              * 16323074         
u_str ESTAB      0      0                                               * 16331006              * 16331007         
u_str ESTAB      0      0                                               * 16883732              * 16883733         
u_str ESTAB      0      0                                               * 15536472              * 15536473         
u_str ESTAB      0      0                                               * 9318                  * 9317             
u_str ESTAB      0      0                                               * 16930620              * 16930619         
u_str ESTAB      0      0                                               * 16323075              * 16323076         
u_str ESTAB      0      0                                               * 16331002              * 16331003         
u_str ESTAB      0      0                                               * 16948456              * 16948457         
u_str ESTAB      0      0                                               * 16331004              * 16331005         
u_str ESTAB      0      0                                               * 19603081              * 19603080         
u_str ESTAB      0      0                                               * 18065002              * 18065003         
u_str ESTAB      0      0                                               * 16930622              * 16930621         
u_str ESTAB      0      0                                               * 16318724              * 16318725         
u_str ESTAB      0      0                               /tmp/.X11-unix/X0 15515571              * 15519569         
u_str ESTAB      0      0                                               * 16330957              * 16330958         
u_str ESTAB      0      0                                               * 3654                  * 10314            
u_str ESTAB      0      0                                               * 16321323              * 16321324         
u_str ESTAB      0      0                                               * 18061757              * 18061758         
u_str ESTAB      0      0                                               * 16323071              * 16323072         
u_str ESTAB      0      0                                               * 16330960              * 16330961         
u_str ESTAB      0      0                                               * 16321330              * 16321329         
u_str ESTAB      0      0      /var/run/docker/containerd/containerd.sock 4219                  * 15398            
u_str ESTAB      0      0                                               * 3614                  * 3615             
u_str ESTAB      0      0                                               * 16930621              * 16930622         
u_str ESTAB      0      0                                               * 16930626              * 16930625         
u_str ESTAB      0      0                                               * 19603080              * 19603081         
u_str ESTAB      0      0                                               * 16948459              * 16948458         
u_str ESTAB      0      0                                               * 16876186              * 16876187         
u_str ESTAB      0      0                                               * 18061763              * 18061764         
u_str ESTAB      0      0                                               * 18065004              * 18065005         
u_str ESTAB      0      0                                               * 16883728              * 16883729         
u_str ESTAB      0      0                                               * 16321325              * 16321326         
u_str ESTAB      0      0                                               * 11303                 * 11302            
u_str ESTAB      0      0                                               * 11300                 * 11301            
u_str ESTAB      0      0                                               * 16323074              * 16323073         
u_str ESTAB      0      0                                               * 16330958              * 16330957         
u_str ESTAB      0      0                                               * 16318722              * 16318723         
u_str ESTAB      0      0                     /mnt/wslg/PulseAudioRDPSink 3655                  * 1998             
u_str ESTAB      0      0                                               * 16876187              * 16876186         
u_str ESTAB      0      0                                               * 16883734              * 16883735         
u_str ESTAB      0      0                                               * 3615                  * 3614             
u_str ESTAB      0      0                                               * 16948460              * 16948461         
u_str ESTAB      0      0                                               * 16331005              * 16331004         
u_str ESTAB      0      0                                               * 16930625              * 16930626         
u_str ESTAB      0      0                                               * 18061761              * 18061762         
u_str ESTAB      0      0                                               * 11301                 * 11300            
u_str ESTAB      0      0                                               * 3606                  * 15372            
u_str ESTAB      0      0                                               * 15531228              * 15515568         
u_str ESTAB      0      0                                               * 18065006              * 18065007         
u_str ESTAB      0      0                                               * 16321328              * 16321327         
u_str ESTAB      0      0                                               * 15398                 * 4219             
u_str ESTAB      0      0                                               * 16318727              * 16318726         
u_str ESTAB      0      0                                               * 18061760              * 18061759         
u_str ESTAB      0      0                                               * 15519569              * 15515571         
u_str ESTAB      0      0                                               * 16321324              * 16321323         
u_str ESTAB      0      0                                               * 18065003              * 18065002         
u_str ESTAB      0      0                                               * 16331003              * 16331002         
tcp   ESTAB      0      0                                       127.0.0.1:37962         127.0.0.1:37353            
tcp   ESTAB      0      0                                       127.0.0.1:59952         127.0.0.1:37353            
tcp   ESTAB      0      0                                  172.30.249.175:3400       172.30.240.1:60802            
tcp   ESTAB      0      0                                       127.0.0.1:37353         127.0.0.1:37962            
tcp   ESTAB      0      0                                       127.0.0.1:59958         127.0.0.1:37353            
tcp   ESTAB      0      0                                       127.0.0.1:48262         127.0.0.1:37353            
tcp   ESTAB      0      0                                       127.0.0.1:37353         127.0.0.1:48268            
tcp   ESTAB      0      0                                       127.0.0.1:37353         127.0.0.1:37970            
tcp   ESTAB      0      0                                       127.0.0.1:37353         127.0.0.1:48262            
tcp   ESTAB      0      0                                       127.0.0.1:37353         127.0.0.1:59958            
tcp   ESTAB      0      0                                       127.0.0.1:48268         127.0.0.1:37353            
tcp   ESTAB      0      0                                       127.0.0.1:37970         127.0.0.1:37353            
tcp   ESTAB      0      0                                       127.0.0.1:45894         127.0.0.1:37353            
tcp   ESTAB      0      0                                  172.30.249.175:3400       172.30.240.1:50461            
tcp   ESTAB      0      0                                       127.0.0.1:37353         127.0.0.1:45894            
tcp   ESTAB      0      0                                       127.0.0.1:45882         127.0.0.1:37353            
tcp   ESTAB      0      0                                  172.30.249.175:3400       172.30.240.1:55228            
tcp   ESTAB      0      0                                  172.30.249.175:49410     160.79.104.10:https            
tcp   ESTAB      0      0                                  172.30.249.175:57876     160.79.104.10:https            
tcp   ESTAB      0      0                                  172.30.249.175:47144     160.79.104.10:https            
tcp   FIN-WAIT-2 0      0                                  172.30.249.175:3400       172.30.240.1:50737            
tcp   ESTAB      0      0                                       127.0.0.1:37353         127.0.0.1:45882            
tcp   ESTAB      0      0                                       127.0.0.1:37353         127.0.0.1:59952            
tcp   ESTAB      0      0                                  172.30.249.175:57884     160.79.104.10:https            
v_str ESTAB      0      0                                               *:633275402             2:50000            
v_str ESTAB      0      0                                               *:633275403             2:50000            
v_str ESTAB      0      0                                               *:633275404             2:50000            
v_str ESTAB      0      0                                               *:633275405             2:50000            
v_str ESTAB      0      0                                               *:633275406             2:50000            
v_str ESTAB      0      0                                               *:633275408             2:50001            
v_str ESTAB      0      0                                               *:633275409             2:50001            
v_str ESTAB      0      0                                               *:633275410             2:50001            
v_str ESTAB      0      0                                               *:633275424             2:50000            
v_str ESTAB      0      0                                               *:633275425             2:50000            
v_str ESTAB      0      0                                               *:633275426             2:50002            
v_str ESTAB      0      0                                               *:633275427             2:50002            
v_str ESTAB      0      0                                               *:633275428             2:50002            
v_str ESTAB      0      0                                               *:633275431             2:50002            
v_str ESTAB      0      0                                               *:633275432             2:50002            
v_str ESTAB      0      0                                               *:633275433             2:50002            
v_str ESTAB      0      0                                               *:1                     2:4102841729       
v_str ESTAB      0      0                                               *:633275411             2:4102841364       
v_str ESTAB      0      0                                               *:633275671             2:342791897        
v_str ESTAB      0      0                                               *:633275674             2:342791913        
v_str ESTAB      0      0                                               *:633275674             2:342791912        
v_str ESTAB      0      0                                               *:633275674             2:342791911        
v_str ESTAB      0      0                                               *:633275674             2:342791910        
v_str ESTAB      0      0                                               *:633275674             2:342791909        
v_str ESTAB      0      0                                               *:633275675             2:342791919        
v_str ESTAB      0      0                                               *:633275672             2:342791902        
v_str ESTAB      0      0                                               *:633275672             2:342791901        
v_str ESTAB      0      0                                               *:633275672             2:342791900        
v_str ESTAB      0      0                                               *:633275672             2:342791899        
v_str ESTAB      0      0                                               *:633275672             2:342791898        
v_str ESTAB      0      0                                               *:633275673             2:342791908        
v_str ESTAB      0      0                                               *:633275676             2:342791924        
v_str ESTAB      0      0                                               *:633275676             2:342791923        
v_str ESTAB      0      0                                               *:633275676             2:342791922        
v_str ESTAB      0      0                                               *:633275676             2:342791921        
v_str ESTAB      0      0                                               *:633275676             2:342791920        
v_str ESTAB      0      0                                               *:633275430             2:4102841703       
v_str ESTAB      0      0                                               *:633275430             2:4102841702       
v_str ESTAB      0      0                                               *:633275430             2:4102841701       
v_str CLOSING    0      0                                               *:633275430             2:4102841700       
v_str ESTAB      0      0                                               *:633275429             2:4102841697       
v_str ESTAB      0      0                                               *:633275435             2:4102841707       
v_str ESTAB      0      0                                               *:633275691             2:342792670        
v_str ESTAB      0      0                                               *:633275694             2:342792686        
v_str ESTAB      0      0                                               *:633275694             2:342792685        
v_str ESTAB      0      0                                               *:633275694             2:342792684        
v_str ESTAB      0      0                                               *:633275694             2:342792683        
v_str ESTAB      0      0                                               *:633275694             2:342792682        
v_str ESTAB      0      0                                               *:633275692             2:342792675        
v_str ESTAB      0      0                                               *:633275692             2:342792674        
v_str ESTAB      0      0                                               *:633275692             2:342792673        
v_str ESTAB      0      0                                               *:633275692             2:342792672        
v_str ESTAB      0      0                                               *:633275692             2:342792671        
v_str ESTAB      0      0                                               *:633275693             2:342792681        
v_str ESTAB      0      0                                               *:633275703             2:342792823        
v_str ESTAB      0      0                                               *:633275706             2:342792839        
v_str ESTAB      0      0                                               *:633275706             2:342792838        
v_str ESTAB      0      0                                               *:633275706             2:342792837        
v_str ESTAB      0      0                                               *:633275706             2:342792836        
v_str ESTAB      0      0                                               *:633275706             2:342792835        
v_str ESTAB      0      0                                               *:633275704             2:342792828        
v_str ESTAB      0      0                                               *:633275704             2:342792827        
v_str ESTAB      0      0                                               *:633275704             2:342792826        
v_str ESTAB      0      0                                               *:633275704             2:342792825        
v_str ESTAB      0      0                                               *:633275704             2:342792824        
v_str ESTAB      0      0                                               *:633275705             2:342792834        
v_str ESTAB      0      0                                               *:633275458             2:4102841830       
v_str ESTAB      0      0                                               *:633275458             2:4102841829       
v_str CLOSING    0      0                                               *:633275458             2:4102841828       
v_str CLOSING    0      0                                               *:633275458             2:4102841827       
v_str ESTAB      0      0                                               *:633275458             2:4102841826       
v_str ESTAB      0      0                                               *:633275711             2:342793593        
v_str ESTAB      0      0                                               *:633275457             2:4102841825       
v_str ESTAB      0      0                                               *:633275462             2:4102842074       
v_str ESTAB      0      0                                               *:633275462             2:4102842073       
v_str ESTAB      0      0                                               *:633275462             2:4102842072       
v_str ESTAB      0      0                                               *:633275462             2:4102842071       
v_str ESTAB      0      0                                               *:633275462             2:4102842070       
v_str ESTAB      0      0                                               *:633275714             2:342793609        
v_str ESTAB      0      0                                               *:633275714             2:342793608        
v_str ESTAB      0      0                                               *:633275714             2:342793607        
v_str ESTAB      0      0                                               *:633275714             2:342793606        
v_str ESTAB      0      0                                               *:633275714             2:342793605        
v_str ESTAB      0      0                                               *:633275463             2:4102842086       
v_str ESTAB      0      0                                               *:633275712             2:342793598        
v_str ESTAB      0      0                                               *:633275712             2:342793597        
v_str ESTAB      0      0                                               *:633275712             2:342793596        
v_str ESTAB      0      0                                               *:633275712             2:342793595        
v_str ESTAB      0      0                                               *:633275712             2:342793594        
v_str ESTAB      0      0                                               *:633275461             2:4102842069       
v_str ESTAB      0      0                                               *:633275713             2:342793604        
v_str ESTAB      0      0                                               *:633275466             2:4102842133       
v_str ESTAB      0      0                                               *:633275466             2:4102842132       
v_str ESTAB      0      0                                               *:633275466             2:4102842131       
v_str ESTAB      0      0                                               *:633275466             2:4102842130       
v_str ESTAB      0      0                                               *:633275466             2:4102842129       
v_str ESTAB      0      0                                               *:633275467             2:4102842156       
v_str ESTAB      0      0                                               *:633275464             2:4102842091       
v_str ESTAB      0      0                                               *:633275464             2:4102842090       
v_str ESTAB      0      0                                               *:633275464             2:4102842089       
v_str ESTAB      0      0                                               *:633275464             2:4102842088       
v_str ESTAB      0      0                                               *:633275464             2:4102842087       
v_str ESTAB      0      0                                               *:633275465             2:4102842128       
v_str ESTAB      0      0                                               *:633275470             2:4102842564       
v_str ESTAB      0      0                                               *:633275470             2:4102842563       
v_str ESTAB      0      0                                               *:633275470             2:4102842562       
v_str ESTAB      0      0                                               *:633275470             2:4102842561       
v_str ESTAB      0      0                                               *:633275470             2:4102842560       
v_str ESTAB      0      0                                               *:633275471             2:4102843465       
v_str ESTAB      0      0                                               *:633275468             2:4102842161       
v_str ESTAB      0      0                                               *:633275468             2:4102842160       
v_str ESTAB      0      0                                               *:633275468             2:4102842159       
v_str ESTAB      0      0                                               *:633275468             2:4102842158       
v_str ESTAB      0      0                                               *:633275468             2:4102842157       
v_str ESTAB      0      0                                               *:633275469             2:4102842559       
v_str ESTAB      0      0                                               *:633275472             2:4102843470       
v_str ESTAB      0      0                                               *:633275472             2:4102843469       
v_str ESTAB      0      0                                               *:633275472             2:4102843468       
v_str ESTAB      0      0                                               *:633275472             2:4102843467       
v_str ESTAB      0      0                                               *:633275472             2:4102843466       : Base ScanState structure containing common scan state information
- : Expression state for recheck conditions that must be evaluated on retrieved tuples
- : TIDBitmap containing the set of tuple identifiers to scan from the heap
- : Iterator for scanning through the current pages in the bitmap
- : Current-page data structure containing results from the current iteration
- : Buffer for visibility-map lookups when prefetching pages
- : Statistics counter for total number of exact pages retrieved
- : Statistics counter for total number of lossy pages retrieved
- : Iterator for prefetching pages ahead of the current scan position
- : Number of pages the prefetch iterator is ahead of the current iterator
- : Current target prefetch distance for optimal I/O performance
- : Maximum allowed value for prefetch_target
- : Boolean flag indicating if the node is ready to iterate
- : Shared iterator for parallel bitmap scans
- : Shared iterator for coordinated prefetching in parallel scans
- : Pointer to ParallelBitmapHeapState for parallel execution coordination

## Dependencies
- Functions called/Symbols referenced:
  - [ScanState](../S/ScanState.md)
  - [TIDBitmap](../T/TIDBitmap.md)
  - [TBMIterator](../T/TBMIterator.md)
  - TBMIterateResult
  - [TBMSharedIterator](../T/TBMSharedIterator.md)
  - [ParallelBitmapHeapState](../P/ParallelBitmapHeapState.md)
- Called from (representative examples):
  - [ExecBitmapHeapScan](../E/ExecBitmapHeapScan.md)
  - [ExecInitBitmapHeapScan](../E/ExecInitBitmapHeapScan.md)
  - [ExecEndBitmapHeapScan](../E/ExecEndBitmapHeapScan.md)
  - [ExecReScanBitmapHeapScan](../E/ExecReScanBitmapHeapScan.md)
  - [BitmapHeapNext](BitmapHeapNext.md)
  - [BitmapHeapRecheck](BitmapHeapRecheck.md)

## Notes and Other Information
- Critical component of PostgreSQL's bitmap scan strategy, complementing bitmap index scans
- Supports both exact pages (containing specific TIDs) and lossy pages (requiring full page scan)
- Prefetching mechanism significantly improves I/O performance by reading ahead of current scan position
- Parallel execution allows multiple workers to efficiently share the work of scanning large bitmaps
- The visibility map is used during prefetching to avoid unnecessary I/O for all-visible pages
- Dynamic prefetch distance adjustment adapts to system I/O characteristics and workload patterns
- Statistics on exact vs lossy pages provide insight into bitmap scan efficiency for query optimization
- Recheck qualification handles cases where index conditions need re-evaluation after heap access
- Seamlessly integrates with the broader bitmap scan ecosystem including BitmapOr and BitmapAnd nodes