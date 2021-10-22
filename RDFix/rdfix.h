#include <stdio.h>
#include <string.h>
#include <libpmemobj.h>
#include "kvs.h"
#include <stdbool.h>
#include <time.h>       // for clock_t, clock(), CLOCKS_PER_SEC
#include <inttypes.h>


#define die(...) do {fprintf(stderr, __VA_ARGS__); exit(1);} while(0)
#define LAYOUT_NAME "RDFix"

#define SUBJECT_NUMBER   17500 
#define PREDICATE_NUMBER 18   
#define OBJECT_NUMBER    14000

#define ST_OBJ_SIZE  1910
#define ST_PRE_SIZE  200
#define PRE_SIZE     50
#define OBJ_SIZE     100
#define POOL_G_SIZE ((size_t)(1024 * 1024 * 1024)) //Upto 1G seems OK as well

#define KEY_1  17500 
#define KEY_2  18 
#define SHARD_FACTOR 4000000

#define POOL_SIZE  (sizeof(oid_ram_subject_array) + (OBJECT_NUMBER * sizeof(long))) 
#define NUMBER_OF_POOLS (( (KEY_1 * KEY_2 ) / SHARD_FACTOR ) + 1)


typedef PMEMoid  Pmem_spo_oid [PREDICATE_NUMBER];
Pmem_spo_oid * oid_ram_subject_array[NUMBER_OF_POOLS] ;

uint64_t size_total =0;

PMEMobjpool *pop_w[NUMBER_OF_POOLS];
PMEMoid root_w[NUMBER_OF_POOLS];

struct oo_link_list {

	long oo_value;
	PMEMoid next;

};

int attach_node (struct link_list *head, long v, long shrd_idx) {

	PMEMoid node1;

	if(pmemobj_alloc(pop_w[shrd_idx], &node1, sizeof(struct oo_link_list ),0,NULL,0)) {
                printf("attach_node pallloc problem\n");
                return 1;		
        }

        struct oo_link_list *n1 = pmemobj_direct(node1);
        n1->oo_value = v;	
        n1->next = OID_NULL;

	struct oo_link_list* read = head;

	while(!OID_IS_NULL(read->next)) {
                read = pmemobj_direct(read->next);
        }

	read->next = node1;
        pmemobj_persist(pop_w[shrd_idx], &n1->oo_value, sizeof(n1->oo_value));
        pmemobj_persist(pop_w[shrd_idx], &n1->next, sizeof(n1->next));

	return 0;
}





KVSstore *s_store_str_id;
KVSstore *s_store_id_str;

KVSstore *p_store_str_id;
KVSstore *p_store_id_str;

KVSstore *o_store_str_id;
KVSstore *o_store_id_str;

long s_counter=1, p_counter=1, o_counter=1;


void create_index_for_subject_block(char _ss[ST_PRE_SIZE], char _pp[PRE_SIZE][ST_PRE_SIZE], char _oo[OBJ_SIZE][ST_OBJ_SIZE], int idx)
{


	#if (DEBUG == 1)
	{
		printf("\n subject:%s\n", _ss);

		for (int i=0; i <= idx; i++)
			printf("p:%s \t \t o:%s \n ", _pp[i], _oo[i]);
		//sleep(1);
		printf("\n************************\n");

	}
        #endif



	if ( kvs_get(s_store_str_id, _ss) == NULL)
	{
		char* ssdup = strdup(_ss);	         
		kvs_put(s_store_str_id, ssdup,  s_counter);
		char* sdup = strdup(_ss);
		long * ints = malloc(sizeof(long));
		ints = s_counter;
		kvs_put(s_store_id_str, ints ,sdup);
		s_counter++;
	}


	long slk = kvs_get(s_store_str_id, _ss);
	long last_pp = 0;
	PMEMoid oo_head;
	struct oo_link_list *n1;
	long long_shard;



	for (int i=0; i <= idx; i++)
	{
		if (kvs_get(p_store_str_id, _pp[i]) == NULL )
		{
			char* ppdup = strdup(_pp[i]);
			kvs_put(p_store_str_id, ppdup, p_counter);
			char* pdup = strdup(_pp[i]);
			long * intp = malloc(sizeof(long));
			intp = p_counter;
			kvs_put(p_store_id_str, intp ,pdup);
	
			p_counter++;
		}

		if (kvs_get(o_store_str_id, _oo[i]) == NULL)
		{
			char* oodup = strdup(_oo[i]);
			kvs_put(o_store_str_id, oodup, o_counter);
			char* odup = strdup(_oo[i]);
			long * into = malloc(sizeof(long));
			into = o_counter;
			kvs_put(o_store_id_str, into ,odup);
	
			o_counter++;
		}


		long plk = kvs_get(p_store_str_id, _pp[i]);
		long olk = kvs_get(o_store_str_id, _oo[i]);
		if(last_pp != plk) { //new predicate
			last_pp = plk;

			//long_shard = ((slk * KEY_2) / SHARD_FACTOR) + 1;
			long_shard = ((slk * KEY_2) / SHARD_FACTOR) ;

			if(pmemobj_alloc(pop_w[long_shard], &oo_head, sizeof(struct oo_link_list),0,NULL,0)) {
				printf("pallloc problem\n");
			        printf("size_total: %" PRIu64 "", size_total);
				getchar();
				return 1;
			}
			//size_total += 24;
			n1 = pmemobj_direct(oo_head);
			n1->oo_value = olk;;
			n1->next = OID_NULL;
			pmemobj_persist(pop_w[long_shard], &n1->oo_value, sizeof(n1->oo_value));
			pmemobj_persist(pop_w[long_shard], &n1->next, sizeof(n1->next));
			oid_ram_subject_array[long_shard] [slk][plk] = oo_head;

		} else { //multi-valued predicate
			attach_node(n1, olk, long_shard);
		}



	}




}//create indexes
	

void read_triples (char *filename)
{


	char subject[ST_PRE_SIZE], tr_predicate[PRE_SIZE][ST_PRE_SIZE], object[OBJ_SIZE][ST_OBJ_SIZE];
	char tmp_subject[ST_PRE_SIZE];
	char *check_ws;
	char gt = '>';
	bool flag_bnode = false;
	char first_line = 'y';
	long fpos;
	int readfile = 0;
	int arr_index=0;
	FILE *fp = NULL;

	fp = fopen(filename, "r");
        if (fp == NULL)	{
		printf ("--read_triples-- \t Error opening the file\n\n");
		exit(EXIT_FAILURE);
	}


	while (readfile != EOF)	{
		fpos = ftell(fp);
		readfile = fscanf(fp, "%s %s %[^\n]\n", subject, tr_predicate[arr_index], object[arr_index]);
		
		if (subject[0] == '_' && subject[1]==':')
		      	flag_bnode = true;
	      	else
		     	check_ws = strstr(subject,">");

	      	if (!flag_bnode && check_ws == NULL) {
		      	fseek(fp, fpos, SEEK_SET);
		      	readfile = fscanf(fp, "%[^>]> %s %[^\n]\n", subject, tr_predicate[arr_index], object[arr_index]);
			#pragma GCC diagnostic push
			#pragma GCC diagnostic ignored "-Wstringop-truncation"
			strncat(subject, &gt, 1);
			#pragma GCC diagnostic pop
	     	}

		if (first_line == 'y') {
			strcpy(tmp_subject, subject);
			first_line = 'n';
		}

		flag_bnode = false;
		arr_index++;

		if (strcmp(tmp_subject,subject) != 0) {
			first_line = 'y';
			fseek(fp, fpos, SEEK_SET);
			arr_index-=2;
			create_index_for_subject_block(tmp_subject,tr_predicate,object,arr_index);
			fseek(fp, fpos, SEEK_SET);
			tmp_subject[0] = '\0';
			arr_index=0;
		}

	}//while

	if(arr_index > 0)
	{
		arr_index-=2;
		create_index_for_subject_block(tmp_subject,tr_predicate,object,arr_index);
	}


	fclose(fp);
        


}//read_triples

long getRand(long lw, long up) {
	return ((rand() % (up - lw + 1)) + lw); 	
} 

char *filenames[50][NUMBER_OF_POOLS];
void init_RDFix(char * path) {


      	//file name
        for (int i = 0; i < NUMBER_OF_POOLS; i++) {
                snprintf(filenames[i], 50, "%s%d_shard", path,i); // puts string into buffer
                printf("%s\n", filenames[i]); // outputs so you can see it
        }

        s_store_str_id = kvs_create(strcmp);
        p_store_str_id = kvs_create(strcmp);
        o_store_str_id = kvs_create(strcmp);

        s_store_id_str = kvs_create(NULL);
        p_store_id_str = kvs_create(NULL);
        o_store_id_str = kvs_create(NULL);


	for(int i=0; i<NUMBER_OF_POOLS; i++) {

		printf("output file name%s\n", filenames[i]);
		pop_w[i] = pmemobj_create(filenames[i], LAYOUT_NAME, POOL_G_SIZE , 0666);
                if (pop_w[i] == NULL) {
                        perror("pmemobj_create");
    			return 1;
 		}
	}


        //root object
        for(int i=0; i<NUMBER_OF_POOLS; i++) {
                root_w[i] = pmemobj_root(pop_w[i], (sizeof(PMEMoid)* SUBJECT_NUMBER * PREDICATE_NUMBER) );
                oid_ram_subject_array[i] = (Pmem_spo_oid *) pmemobj_direct(root_w[i]);
        }


	for(int k=0; k<NUMBER_OF_POOLS; k++) {
		for(long i=0; i<SUBJECT_NUMBER; i++){
			for(long j=0; j<PREDICATE_NUMBER; j++){
				oid_ram_subject_array[k][i][j]= OID_NULL;
                        }
                }
 	}



}


void finalize_RDFix(){

	for(int i=0; i<NUMBER_OF_POOLS; i++) {
		pmemobj_persist(pop_w[i], oid_ram_subject_array[i], sizeof(oid_ram_subject_array[i]));
 		pmemobj_close(pop_w[i]);
	}
}


PMEMobjpool * pop_r1[NUMBER_OF_POOLS];
PMEMoid root_r1[NUMBER_OF_POOLS];
Pmem_spo_oid * read_oid_ram_subject_array[NUMBER_OF_POOLS];

void read_from_pool() {

	for(int i=0; i<NUMBER_OF_POOLS; i++) {
		printf("\n file-name: %s \n", filenames[i]);
                pop_r1[i]  = pmemobj_open(filenames[i], LAYOUT_NAME);
                root_r1[i] = pmemobj_root(pop_r1[i], (sizeof(PMEMoid)* SUBJECT_NUMBER * PREDICATE_NUMBER)  );
                read_oid_ram_subject_array[i] = (Pmem_spo_oid *)  pmemobj_direct(root_r1[i]);
        }

}

int range = 0; //for range scan operation
long ss_c = 0;

void lookup_sc1(long rep) {

	
        // All values associated with Random v1 and Random v2
         for (int i=0; i < rep; i++) {

                long sub = getRand(1,ss_c);
                long long_shard = (( sub  * KEY_2) / SHARD_FACTOR) ;
                //printf("slk: %ld\n", sub );
                long pre_rand = getRand(1, PREDICATE_NUMBER);

                struct oo_link_list *read =
                                pmemobj_direct(read_oid_ram_subject_array[long_shard][sub][pre_rand]);

                while(read != NULL) {
                                //printf("key = %ld, value = %ld \n",pre_rand , read->oo_value);
                                read = pmemobj_direct(read->next);
                }

        }

        


}

void lookup_sc2(long rep) {


 	// All values associated with Random K1 and all K2
        for (int i=0; i < rep; i++) {

                long sub = getRand(1,ss_c);
                long long_shard = (( sub  * KEY_2) / SHARD_FACTOR) ;
                //printf("slk: %ld\n", sub );

                for(long j=0; j<PREDICATE_NUMBER; j++) {

                        struct oo_link_list *read =
                                pmemobj_direct(read_oid_ram_subject_array[long_shard][sub][j]);

                //if (read != NULL) {
                                //printf("\n******************************\n");
                               // printf("slk: %ld\n", sub );
                //}


                while(read != NULL) {
                                printf("key = %ld, value = %ld \n",j, read->oo_value);
                                read = pmemobj_direct(read->next);
                }

                //if (read != NULL) printf("\n******************************\n");

                }

        }

}


void range_scans(long rep, long range) {
	
   
         // Range Q. (All values associated with Random K1 and all K2)
        for (int i=0; i < rep ; i++) {
        
                long sub = getRand(1,ss_c);             
                long long_shard = (( sub  * KEY_2) / SHARD_FACTOR) ;
                //printf("slk: %ld\n", sub );

                for (long k=0; k < range; k++) { //range is a var \in {10, 100, 1000}
        
                        for(long j=0; j<PREDICATE_NUMBER; j++) {
                                struct oo_link_list *read = 
                                        pmemobj_direct(read_oid_ram_subject_array[long_shard][sub][j]);

                                

                                while (read != NULL) {
                                        printf("key = %ld, value = %ld \n",j, read->oo_value);
                                        read = pmemobj_direct(read->next);
                                }

                //if (read != NULL) printf("\n******************************\n");
                
                        }//pre
                        
                        sub++;                  
                        //long_shard = (( sub  * KEY_2) / SHARD_FACTOR) ;
                }//range
        }//repeat


}

void del(long rep) {

       	// Del all first nodes related to rand K1 and all associated k2
        /*
	 * Note: if we repeated will del all nodes
	 * since the second node (if any) will be replaced
	 * and become the new first node each time
	*/

        for (int i=0; i < rep ; i++) {

                long sub = getRand(1,ss_c);
                long long_shard = (( sub  * KEY_2) / SHARD_FACTOR) ;
                
                for(long j=0; j<PREDICATE_NUMBER; j++) {

                        struct oo_link_list *read =
                                pmemobj_direct(read_oid_ram_subject_array[long_shard][sub][j]);

                
			if (read != NULL) {

                        PMEMoid del_node = read_oid_ram_subject_array[long_shard][sub][j];
                        pmemobj_free(&del_node);
                        read_oid_ram_subject_array[long_shard][sub][j] = read->next;
                        //printf("del done!");
			//If you are sure you can persist it by calling finalize_RDFix() 
			
			}
		}

        }
	
}

void dict_destroy() {

	kvs_destroy(s_store_str_id);
        kvs_destroy(p_store_str_id);
        kvs_destroy(o_store_str_id);

        kvs_destroy(s_store_id_str);
        kvs_destroy(p_store_id_str);
        kvs_destroy(o_store_id_str);
	
}
