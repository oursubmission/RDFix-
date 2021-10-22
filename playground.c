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
        ss_c = 17175 - range; // max(v1)
        long repeat = 10; // 1M, 10M, 100M in the paper

        read_from_pool();
        lookup_sc1(repeat);
        dict_destroy();

        
        return 0;
}

