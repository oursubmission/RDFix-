#include <stdio.h>
#include <stdlib.h>
#include <libpmemobj.h>
#include "map_hashmap_atomic.h"
#include "map.h"
#include "hashmap.h"
#include <sys/stat.h>   
#include <stdbool.h> 
#include <fcntl.h>
#include <sys/types.h>
#include <inttypes.h>
#include "kvs.h"



#define KEY_1 5878750 
#define SHARD_FACTOR 1614696666
#define PM_HASHSET_POOL_SIZE	(990 * 1024 * 1024)

#define NUMBER_OF_POOLS (( (KEY_1 * 824 ) / SHARD_FACTOR ) + 1)

#define PREDICATE_NUMBER 160

#define ST_OBJ_SIZE 1910
#define ST_PRE_SIZE 200
#define PRE_SIZE    50
#define OBJ_SIZE 100

KVSstore *s_store_str_id;
KVSstore *s_store_id_str;

KVSstore *p_store_str_id;
KVSstore *p_store_id_str;

KVSstore *o_store_str_id;
KVSstore *o_store_id_str;
long s_counter=1, p_counter=1, o_counter=1;


bool file_exists (char *filename) {
      	struct stat   buffer;
      	return (stat (filename, &buffer) != 0);
}


POBJ_LAYOUT_BEGIN(map);
POBJ_LAYOUT_ROOT(map, struct root);
POBJ_LAYOUT_END(map);

struct root {
	TOID(struct map) map;
};

static PMEMobjpool *pop;
static struct map_ctx *mapc[KEY_1];
static TOID(struct root) root[KEY_1];
static TOID(struct map) map[KEY_1];


struct oo_link_list {

	long oo_value;
	PMEMoid next;

};


int attach_node (struct link_list *head, long v, long shrd_idx) {

	PMEMoid node1;


        if(pmemobj_alloc(pop, &node1, sizeof(struct oo_link_list ),0,NULL,0)) {
                printf("attach_node pallloc problem\n");
                return 1;
		getchar();
        }
	//size_total += 24;

        struct oo_link_list *n1 = pmemobj_direct(node1);
        n1->oo_value = v;	
        n1->next = OID_NULL;

	struct oo_link_list* read = head;

	while(!OID_IS_NULL(read->next))
        {
                //printf("attach_node value: %d\n", read->int_value);
                read = pmemobj_direct(read->next);
        }
	read->next = node1;

        //pmemobj_persist(pop, &head->int_value, sizeof(head->int_value));
        //pmemobj_persist(pop, &head->next, sizeof(head->next));
        pmemobj_persist(pop, &n1->oo_value, sizeof(n1->oo_value));
        pmemobj_persist(pop, &n1->next, sizeof(n1->next));


	//printf("inside attach size_total: %" PRIu64 "\n", size_total);

	return 0;
}



void create_index_for_subject_block(char _ss[ST_PRE_SIZE], char _pp[PRE_SIZE][ST_PRE_SIZE], char _oo[OBJ_SIZE][ST_OBJ_SIZE], int idx)
{



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
	//printf("slk: %ld \n", slk);
	long last_pp = 0;
	PMEMoid oo_head;
	struct oo_link_list *n1;
	long long_shard;
	long key_c ;


	


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

                //printf("p:%s \t \t o:%s \n ", _pp[i], _oo[i]);
		//printf("c_p: %lu \t c_o: %lu \n", p_counter, o_counter);

		long plk = kvs_get(p_store_str_id, _pp[i]);
		long olk = kvs_get(o_store_str_id, _oo[i]);
		//printf("plk: %ld \t olk: %ld \n", plk, olk);
		//key_c = concatenate_keys(slk, plk);
		//printf("Key_c: %ld\n", key_c);


	
		

		if(last_pp != plk) { //new predicate
			last_pp = plk;

			long_shard = ((slk * KEY_1) / SHARD_FACTOR) ;

			if(pmemobj_alloc(pop, &oo_head, sizeof(struct oo_link_list),0,NULL,0)) {
				printf("pallloc problem\n");
			        //printf("size_total: %" PRIu64 "", size_total);
				getchar();
				return 1;
			}
			//size_total += 24;
			n1 = pmemobj_direct(oo_head);
			n1->oo_value = olk;;
			n1->next = OID_NULL;
			pmemobj_persist(pop, &n1->oo_value, sizeof(n1->oo_value));
			pmemobj_persist(pop, &n1->next, sizeof(n1->next));
			map_insert(mapc[slk], map[slk], plk, oo_head);

			//oid_ram_subject_array[long_shard] [slk][plk] = oo_head;

		} else { //multi-valued predicate
			attach_node(n1, olk, long_shard);
		}


	        //skiplist_insert(list, plk, olk);

	}
	

	//skiplist_dump(subject_keys[slk]);
	/*
         printf("\n **sizes**\n");
         printf("MAX-POOL: %ld\n", POOL_MAX_SIZE);
         printf("ROOT-SIZE: %ld\n", (sizeof(PMEMoid)* SUBJECT_NUMBER * PREDICATE_NUMBER));
         printf("SIZE of Pmem_spo_oid: %ld \n", sizeof(Pmem_spo_oid));
         printf("SIZE of Pmem_spo_oid*: %ld \n", sizeof(Pmem_spo_oid*));
         printf("size of oo_link_list: %ld\n",sizeof(struct oo_link_list));
         printf("size of oo_link_list*:%ld\n",sizeof(struct oo_link_list*));
         printf("size of oid_ram_subject_array: %ld\n", sizeof(oid_ram_subject_array));
	 printf("size_total: %" PRIu64 "\n", size_total);
	 */



}//create indexes


void read_triples (char *filename)
{


	char subject[ST_PRE_SIZE], tr_predicate[PRE_SIZE][ST_PRE_SIZE], object[OBJ_SIZE][ST_OBJ_SIZE];
	char tmp_subject[ST_PRE_SIZE];
	//char * tmp_subject;
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

int main(int argc, char ** argv) {



	struct hashmap_args args;
	args.seed = (uint32_t)time(NULL);
	srand(args.seed);



	s_store_str_id = kvs_create(strcmp);
	p_store_str_id = kvs_create(strcmp);
	o_store_str_id = kvs_create(strcmp);

	s_store_id_str = kvs_create(NULL);
	p_store_id_str = kvs_create(NULL);
	o_store_id_str = kvs_create(NULL);


        if (file_exists(argv[1]) != 0) {

		printf("file does not exists, let's create: %s \n", argv[1]);

		pop = pmemobj_create(argv[1], POBJ_LAYOUT_NAME(map), PM_HASHSET_POOL_SIZE, 0666);	

		if (pop == NULL) {
			fprintf(stderr, "failed to create pool: %s\n", pmemobj_errormsg());
			return 1;
		}

		//init hashmaps
		for (int i=0; i<KEY_1; i++){
			mapc[i] = map_ctx_init(MAP_HASHMAP_ATOMIC, pop);
			if (!mapc[i]) {
				pmemobj_close(pop);
				perror("map_ctx_init");
				return 1;
			}
		}


		for (int i=0; i<KEY_1; i++){
			root[i] = POBJ_ROOT(pop, struct root);
			map_create(mapc[i], &D_RW(root[i])->map, &args);
			map[i] = D_RO(root[i])->map;

		}//for



                read_triples(argv[2]);


	}else {
		printf("file exists, let's read: %s \n", argv[1]);
		pop = pmemobj_open(argv[1], POBJ_LAYOUT_NAME(map));
		if (pop == NULL) {
			fprintf(stderr, "failed to open pool: %s\n", pmemobj_errormsg());
			return 1;
		}

		//init hashmaps
                for (int i=0; i<KEY_1; i++){
                        mapc[i] = map_ctx_init(MAP_HASHMAP_ATOMIC, pop);
                        if (!mapc[i]) {
                                pmemobj_close(pop);
                                perror("map_ctx_init");
                                return 1;
                        }
                }

		
                for (int i=0; i<KEY_1; i++){
                        root[i] = POBJ_ROOT(pop, struct root);
                        map[i] = D_RO(root[i])->map;

                }//for

		
		
		
		
	}


        int range = 3;

        long ss_c = KEY_1 - range ; //number of subjects
	long rep_no = strtol(argv[3], NULL, 10);


	
	
	// All values associated with Random K1 and all K2
	  for (int i=0; i<rep_no ; i++) {
                long sub = getRand(1,ss_c);
                for(long j=0; j < PREDICATE_NUMBER; j++) {
			//long key_c = concatenate_keys(sub, j);
                //long long_shard = (( sub  * KEY_2) / SHARD_FACTOR) ;
                //printf("slk: %ld\n", sub );
		 struct oo_link_list *read = pmemobj_direct(map_get(mapc[sub], map[sub], j));
		 //if (read != NULL){
	 	//	 printf("***********\n slk: %ld \n", sub);
		 //}
	 	 while(read != NULL) {
	 		 printf("pre:%ld \t value = %ld \n",j, read->oo_value);
	 		 read = pmemobj_direct(read->next);
	 	 }
	 	// if (read != NULL)
	 	//	 printf("***********\n");
		}
                
        }
	
	
	






}



