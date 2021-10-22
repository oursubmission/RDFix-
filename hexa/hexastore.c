#include <stdio.h>
#include <string.h>
#include <libpmemobj.h>
#include "kvs.h"
#include <stdbool.h>
#include <time.h>       // for clock_t, clock(), CLOCKS_PER_SEC
#include <inttypes.h>


#define die(...) do {fprintf(stderr, __VA_ARGS__); exit(1);} while(0)
#define LAYOUT_NAME "Hexastore"

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






PMEMobjpool *pop_w;
PMEMoid root_w;

KVSstore *s_store_str_id;
KVSstore *s_store_id_str;

KVSstore *p_store_str_id;
KVSstore *p_store_id_str;

KVSstore *o_store_str_id;
KVSstore *o_store_id_str;

struct oo_link_list {

        long oo_value;
        PMEMoid next;

};

struct v2_node {

        long pp_value;
        PMEMoid oo_header;

};

struct v2_node ** v1_arr;


int attach_node (struct link_list *head, long v) {

        PMEMoid node1;

        if(pmemobj_alloc(pop_w, &node1, sizeof(struct oo_link_list ),0,NULL,0)) {
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
        pmemobj_persist(pop_w, &n1->oo_value, sizeof(n1->oo_value));
        pmemobj_persist(pop_w, &n1->next, sizeof(n1->next));

        return 0;
}


long s_counter=1, p_counter=1, o_counter=1;


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
	int counter_v2_alloc = 1;
	int ar_idx = -1 ;

        PMEMoid tmp_v2;
      	if(pmemobj_alloc(pop_w, &tmp_v2, sizeof(struct v2_node) ,0,NULL,0)) {
		printf("pallloc problem\n");
		getchar();
		return 1;
	}



	struct v2_node * v2_arr = (struct v2_node *)pmemobj_direct(tmp_v2);

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

                        if(pmemobj_alloc(pop_w, &oo_head, sizeof(struct oo_link_list),0,NULL,0)) {
                                printf("pallloc problem\n");
                                getchar();
                                return 1;
                        }
                        n1 = pmemobj_direct(oo_head);
                        n1->oo_value = olk;;
                        n1->next = OID_NULL;
                        pmemobj_persist(pop_w, &n1->oo_value, sizeof(n1->oo_value));
                        pmemobj_persist(pop_w, &n1->next, sizeof(n1->next));

			if(pmemobj_realloc(pop_w, &tmp_v2, sizeof(struct v2_node) * (++counter_v2_alloc) ,0)) {
				printf("pallloc problem\n");
				getchar();
				return 1;
			}
			

			
		        


			ar_idx++;
			v2_arr[ar_idx].oo_header = oo_head;
			v2_arr[ar_idx].pp_value  = plk;

			


                } else { //multi-valued predicate
                        attach_node(n1, olk);
                }

        }

	struct v2_node  tmp_v22;
	
	for(int i=0; i<ar_idx ; i++){
		for(int j=0; j< ar_idx - i; j++){
			if ( v2_arr[j].pp_value > v2_arr[j+1].pp_value){
				tmp_v22 = v2_arr[j+1];
				v2_arr[j+1] = v2_arr[j];
				v2_arr[j] = tmp_v22;
			}		
		}
	}

	ar_idx++;
        v2_arr[ar_idx].pp_value = -9;
        pmemobj_persist(pop_w, &v2_arr[ar_idx], sizeof(v2_arr[ar_idx]));


	for(int i=0; i<ar_idx ; i++)
		pmemobj_persist(pop_w, &v2_arr[i], sizeof(v2_arr[i]));
	

	v1_arr[slk] = v2_arr;
        pmemobj_persist(pop_w, &v1_arr[slk], sizeof(v1_arr[slk]));



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
        if (fp == NULL) {
                printf ("--read_triples-- \t Error opening the file\n\n");
                exit(EXIT_FAILURE);
        }


        while (readfile != EOF) {
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

void init_hexa(char * path) {

        s_store_str_id = kvs_create(strcmp);
        p_store_str_id = kvs_create(strcmp);
        o_store_str_id = kvs_create(strcmp);

        s_store_id_str = kvs_create(NULL);
        p_store_id_str = kvs_create(NULL);
        o_store_id_str = kvs_create(NULL);

        pop_w = pmemobj_create(path, LAYOUT_NAME, POOL_G_SIZE , 0666);
	if (pop_w == NULL) {
		perror("pmemobj_create");
		return 1;
	}

	
        root_w = pmemobj_root(pop_w, sizeof(struct v2_node **) * SUBJECT_NUMBER );
	v1_arr = (struct v2_node **) pmemobj_direct(root_w);

}

void dict_destroy() {

        kvs_destroy(s_store_str_id);
        kvs_destroy(p_store_str_id);
        kvs_destroy(o_store_str_id);

        kvs_destroy(s_store_id_str);
        kvs_destroy(p_store_id_str);
        kvs_destroy(o_store_id_str);

}

void finalize_hexa(){
	pmemobj_close(pop_w);
}

PMEMobjpool * pop_r1;
PMEMoid root_r1;
struct v2_node ** read_v1_arr;


void read_from_pool(char * path) {

                     pop_r1 = pmemobj_open(path, LAYOUT_NAME);
                    root_r1 = pmemobj_root(pop_r1, (sizeof(struct v2_node **) * SUBJECT_NUMBER));
                read_v1_arr = (struct v2_node **)  pmemobj_direct(root_r1);
        

}

int range = 0; //for range scan operation
long ss_c = 0;


long get_arr_size (struct v2_node ** v_v1_arr, long v_s){


	long res = 0;
	for(int i=0; i<PREDICATE_NUMBER; i++){
		if (v_v1_arr[v_s][i].pp_value == -9){
			res = i;
			break;
		}
	}
	return res; //0 if not found
				
}


long binary_search(struct v2_node ** v_v1_arr , long sub_v, long j_v)
{
        size_t left = 0;
        size_t right = (get_arr_size(v_v1_arr, sub_v));



        while(left <= right)
        {

                size_t mid = (left + right) >> 1;
		//v_v1_arr[sub_v][mid].pp_value




                if (v_v1_arr[sub_v][mid].pp_value == j_v)
                {
                        return mid;
                }
                if(mid >= j_v)
                        right = mid - 1;
                else
                        left  = mid + 1;
        }
	
        return -1; // not found    
}




void lookup_sc1(long rep) {


        // All values associated with Random v1 and Random v2
         for (int i=0; i < rep; i++) {

                long sub = getRand(1,ss_c);
                //printf("slk: %ld\n", sub );
                long pre_rand = getRand(1, PREDICATE_NUMBER);
		long pre_inx = binary_search(read_v1_arr, sub, pre_rand);
		if ( pre_inx != -1){
	 		struct oo_link_list *read =
		       pmemobj_direct(read_v1_arr[sub][pre_inx].oo_header);
			while(read != NULL) {
				printf("key = %ld, value = %ld \n",pre_rand , read->oo_value);
				read = pmemobj_direct(read->next);
			}
		}

	 }

}


void lookup_sc2(long rep) {


        // All values associated with Random v1 and Random v2
         for (int i=0; i < rep; i++) {

                long sub = getRand(1,ss_c);

		for(int j=0; j<PREDICATE_NUMBER; j++){               
			long pre_inx = binary_search(read_v1_arr, sub, j);
			if ( pre_inx != -1){
				struct oo_link_list *read =
		 			pmemobj_direct(read_v1_arr[sub][pre_inx].oo_header);
				while(read != NULL) {
					printf("key = %ld, value = %ld \n",j , read->oo_value);
					read = pmemobj_direct(read->next);
				}
			}
       		}
	 }
}

void range_scans(long rep, long range) {


        // All values associated with Random v1 and Random v2
         for (int i=0; i < rep; i++) {

                long sub = getRand(1,ss_c);
               for (long k=0; k < range; k++) { //range is a var \in {10, 100, 1000}
       		       for(int j=0; j<PREDICATE_NUMBER; j++){
       			       long pre_inx = binary_search(read_v1_arr, sub, j);
       			       if ( pre_inx != -1){
       				       struct oo_link_list *read =
       					       pmemobj_direct(read_v1_arr[sub][pre_inx].oo_header);
       				       while(read != NULL) {
       					       printf("key = %ld, value = %ld \n",j , read->oo_value);
       					       read = pmemobj_direct(read->next);
       				       }
       			       }
       		       }
		       sub++;
      	       }
	 }
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

		for(long j=0; j<PREDICATE_NUMBER; j++) {

			for(int j=0; j<PREDICATE_NUMBER; j++){
                               long pre_inx = binary_search(read_v1_arr, sub, j);
                               if ( pre_inx != -1){
                                       struct oo_link_list *read =
                                               pmemobj_direct(read_v1_arr[sub][pre_inx].oo_header);                                                  if(read != NULL) {
						       PMEMoid del_node = read_v1_arr[sub][pre_inx].oo_header;
						       pmemobj_free(&del_node);
						       read_v1_arr[sub][pre_inx].oo_header = read->next;
						       //pmemobj_persist(pop_w, &read_v1_arr[sub][pre_inx], sizeof(v2_arr[ar_idx][pre_inx]));
                                       }
			       }
 			}
		}   
	} 
}


int main(int argc, char **argv) {

	//checking that the input is correct (pmem and RDF filename )
        if (argc != 3 ) {
                printf("usage: %s file-name\n", argv[0]);
                return 1;
        }


        // seed for random generator
        srand(time(0));

        init_hexa(argv[1]);
 	read_triples(argv[2]); //insert operation
	finalize_hexa();


       /// *****************  READ


        range = 10; //for range scan operation
        ss_c = 17175 - range; // max(v1)
        long repeat = 10; // 1M, 10M, 100M in the paper

        read_from_pool(argv[1]);
        //lookup_sc2(repeat);
	//range_scans(repeat,range);
        dict_destroy();



        return 0;
}















