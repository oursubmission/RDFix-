# RDFix: An Efficient Indexing Architecture with Persistent Memory for RDF DATA
Welcome! This repository contains the code to our submission. 


## Using RDFix
RDFix is a specialized architecture for indexing Web KGs (Knowledge Graphs) from the ground-up in which all data persist on Persistent Memory (PM). You can download it and include it in your application (without using CMake, etc.).
Here is a short example of RDFix's interface (this is the content of `playground.c`).

```cpp
#include "RDFix/rdfix.h"

int main(int argc, char **argv) {

        //checking that the input is correct (pmem and RDF filename )
        
        if (argc != 3 ) {
                printf("usage: %s file-name\n", argv[0]);
                return 1;
        }
        

        // seed for random generator 
        srand(time(0));

        init_RDFix(argv[1]);
        read_triples(argv[2]); //insert operation
        finalize_RDFix();




        /// *****************  READ
        
        
        range = 0; //for range scan operation
        ss_c = 100000000 - range; // max(v1)
        long repeat = 10; // 1M, 10M, 100M in the paper

        read_from_pool();
        lookup_sc1(repeat);
        dict_destroy();

        
        return 0;
}


```
## Compile
1) Please note that `PMDK` is needed.
2) We used the following flags: `gcc -o3 -c playground.c -o RDFix.o -lpmemobj -Werror=vla -Wextra -Wall -Wshadow -Wswitch-default -DDEBUG=10 -g3  && gcc -o3 kvs.o RDFix.o -o w3.o -lpmemobj -g3  && sudo perf stat -d  nice -n 5 taskset -c 20-39 ./w3.o path_2_POOL path_2_dataset`

# Benchmark Datasets

| Benchmark Datasets   | Link |
| -------------        | ------------- |
|  datasets.tar.xz     | https://zenodo.org/record/5044233/files/datasets.tar.xz?download=1|
|  YAGO                | https://yago-knowledge.org/downloads/yago-4                       |
|  DBpedia             | https://databus.dbpedia.org/dbpedia/collections/latest-core       |     





