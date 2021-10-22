#include <stdio.h>
#include <stdlib.h>
#include <libpmemobj.h>
#include "map_btree.h"
#include "map.h"
#include <sys/stat.h>   
#include <stdbool.h> 
#include <fcntl.h>
#include <sys/types.h>
#include <inttypes.h>
#include "kvs.h"




#define LAYOUT_NAME	"Btree"
#define POOL_MAX_SIZE ((size_t)(1024 * 1024 * 1024)) //Upto 1G seems OK as well

#define KEY_1 17500
#define KEY_2 18
#define SHARD_FACTOR 4000000



#define POOL_SIZE  (sizeof(oid_ram_subject_array) + (OBJECT_NUMBER * sizeof(long))) 
#define NUMBER_OF_POOLS (( (KEY_1 * KEY_2 ) / SHARD_FACTOR ) + 1)


POBJ_LAYOUT_BEGIN(map);
POBJ_LAYOUT_ROOT(map, struct root);
POBJ_LAYOUT_END(map);


bool file_exists (char *filename) {
      	struct stat   buffer;
      	return (stat (filename, &buffer) != 0);
}

struct root {
	TOID(struct map) map;
};

static PMEMobjpool *pop;
static struct map_ctx *mapc;
static TOID(struct root) root;
static TOID(struct map) map;


#define INPUT_BUF_LEN 1000
#define SUBJECT_NUMBER 17500
#define PREDICATE_NUMBER 18 
#define OBJECT_NUMBER 14000

#define ST_OBJ_SIZE 1910
#define ST_PRE_SIZE 200
#define PRE_SIZE    50
#define OBJ_SIZE 100


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

        struct oo_link_list *n1 = pmemobj_direct(node1);
        n1->oo_value = v;	
        n1->next = OID_NULL;

	struct oo_link_list* read = head;

	while(!OID_IS_NULL(read->next))
                read = pmemobj_direct(read->next);
	read->next = node1;

        pmemobj_persist(pop, &n1->oo_value, sizeof(n1->oo_value));
        pmemobj_persist(pop, &n1->next, sizeof(n1->next));

	return 0;
}



KVSstore *s_store_str_id;
KVSstore *s_store_id_str;

KVSstore *p_store_str_id;
KVSstore *p_store_id_str;

KVSstore *o_store_str_id;
KVSstore *o_store_id_str;
long s_counter=1, p_counter=1, o_counter=1;

long concatenate_keys(long a, long b) {

	char s1[200]; 
	char s2[200]; 

	 
	sprintf(s1, "%ld", a); 
	sprintf(s2, "%ld", b); 

	 
	strcat(s1, s2); 
	 
	char *err;
	long c = strtol(s1,&err,10); 

	return c; 
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


		long plk = kvs_get(p_store_str_id, _pp[i]);
		long olk = kvs_get(o_store_str_id, _oo[i]);
		key_c = concatenate_keys(slk, plk);
	
		

		if(last_pp != plk) { //new predicate
			last_pp = plk;

			long_shard = ((slk * KEY_2) / SHARD_FACTOR) ;

			if(pmemobj_alloc(pop, &oo_head, sizeof(struct oo_link_list),0,NULL,0)) {
				printf("pallloc problem\n");
				return 1;
			}
			n1 = pmemobj_direct(oo_head);
			n1->oo_value = olk;;
			n1->next = OID_NULL;
			pmemobj_persist(pop, &n1->oo_value, sizeof(n1->oo_value));
			pmemobj_persist(pop, &n1->next, sizeof(n1->next));

			map_insert(mapc, map, key_c, oo_head);


		} else { //multi-valued predicate
			attach_node(n1, olk, long_shard);
		}



	}
	
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


static int hashmap_print(uint64_t key, PMEMoid value, void *arg) {
	//printf("%s" , key);
	printf("%" PRIu64 " ", key);
	struct oo_link_list *read = pmemobj_direct(value);
	while(read != NULL) {
		printf("value = %ld \n", read->oo_value);
		read = pmemobj_direct(read->next);
	}
	printf("***********\n");
	return 0;
}

static void print_all(void) {
	map_foreach(mapc, map, hashmap_print, NULL);
	printf("\n");
}

long getRand(long lw, long up) {
        return ((rand() % (up - lw + 1)) + lw);
}


int main(int argc, char ** argv) {


	// seed for random generator 
        srand(time(0));





	s_store_str_id = kvs_create(strcmp);
	p_store_str_id = kvs_create(strcmp);
	o_store_str_id = kvs_create(strcmp);

	s_store_id_str = kvs_create(NULL);
	p_store_id_str = kvs_create(NULL);
	o_store_id_str = kvs_create(NULL);


	double time_spent = 0.0;
        clock_t begin;
        time_t wc_begin;


	time_t wc_end;
        clock_t end;

	if (file_exists(argv[1]) != 0) {

		printf("file does not exists, let's create: %s \n", argv[1]);

		pop = pmemobj_create(argv[1], POBJ_LAYOUT_NAME(map), PM_HASHSET_POOL_SIZE, 0666);	

		if (pop == NULL) {
			fprintf(stderr, "failed to create pool: %s\n", pmemobj_errormsg());
			return 1;
		}

		
		mapc = map_ctx_init(MAP_BTREE, pop);
		if (!mapc) {
			pmemobj_close(pop);
			perror("map_ctx_init");
			return 1;
		}
		
		
		root = POBJ_ROOT(pop, struct root);
		map_create(mapc, &D_RW(root)->map, NULL);
		map = D_RO(root)->map;


		time_spent = 0.0;
        	begin = clock();
        	wc_begin = time(NULL);


		read_triples(argv[2]);




		
	}else {
		printf("file exists, let's read: %s \n", argv[1]);
		pop = pmemobj_open(argv[1], POBJ_LAYOUT_NAME(map));
		if (pop == NULL) {
			fprintf(stderr, "failed to open pool: %s\n", pmemobj_errormsg());
			return 1;
		}
                mapc = map_ctx_init(MAP_BTREE, pop);



		if (!mapc) {
			pmemobj_close(pop);
			perror("map_ctx_init");
			return 1;
		}
		root = POBJ_ROOT(pop, struct root);
		map = D_RO(root)->map;
		
	}

	int range = 0;

        long ss_c = 17175 - range; 



/*
	
        // Del first nodes in the list values All values associated with Random K1 and all K2
          for (int i=0; i<10000 ; i++) {

                long sub = getRand(1,ss_c);

	
                for(long j=0; j < PREDICATE_NUMBER; j++) {
                        long key_c = concatenate_keys(sub, j);

		
                 struct oo_link_list *read = pmemobj_direct(map_get(mapc, map, key_c));

 
                 if (read != NULL) {

                         PMEMoid val =  map_remove(mapc, map, key_c);
                         pmemobj_free(&val);
                         map_insert(mapc, map, key_c, read->next);
                         printf("del done!\n");

                 }
		 
		 



	  }


        }
*/

/*
	

	      // Range Q. (associated with Random K1 and Random K2)
         for (int i=0; i<100000 ; i++) {

                long sub = getRand(1,ss_c);

                for (long k=0; k < range; k++) { //range is a var \in {10, 100, 1000}
 
			for(long j=0; j<PREDICATE_NUMBER; j++) {
			
		 		long key_c = concatenate_keys(sub, j);
				struct oo_link_list *read = pmemobj_direct(map_get(mapc, map, key_c));
		 
				if (read != NULL){
	       				printf("***********\n slk: %ld \n", sub);
	       			}
	       			while(read != NULL) {
	       				printf("value = %ld \n", read->oo_value);
	       				read = pmemobj_direct(read->next);
	       			}
	       			if (read != NULL)
	       				printf("***********\n");
        
			}
		sub++;	
		}
	 }

	 
*/


	
	
	// All values associated with Random K1 and all K2
	  for (int i=0; i<100000 ; i++) {

                long sub = getRand(1,ss_c);

                for(long j=0; j < PREDICATE_NUMBER; j++) {
			long key_c = concatenate_keys(sub, j);

		 struct oo_link_list *read = pmemobj_direct(map_get(mapc, map, key_c));
		 //if (read != NULL){
	 	//	 printf("***********\n slk: %ld \n", sub);
		// }
	 	 while(read != NULL) {
	 		 //printf("value = %ld \n", read->oo_value);
	 		 read = pmemobj_direct(read->next);
	 	 }
	 	 //if (read != NULL)
	 	//	 printf("***********\n");
		}


                

        }


	

	

/*
     
	     // All values associated with Random K1 and Random K2
         for (int i=0; i<1000000 ; i++) {

                long sub = getRand(1,ss_c);
                long pre_rand = getRand(1, PREDICATE_NUMBER);
		long key_c = concatenate_keys(sub, pre_rand);
		struct oo_link_list *read = pmemobj_direct(map_get(mapc, map, key_c));
                //if (read != NULL){
                  //       printf("***********\n slk: %ld \n", sub);
                // }
                 while(read != NULL) {
                         //printf("value = %ld \n", read->oo_value);
                         read = pmemobj_direct(read->next);
                 }
                // if (read != NULL)
                  //       printf("***********\n");
                }

	
	
*/


	map_ctx_free(mapc);
	pmemobj_close(pop);




	return 0;
}



