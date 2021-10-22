#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <unistd.h>
#include "lp_lib.h"

#define ty_size 256 
#define st_size 1910 
#define pr_size 1400 
#define lp_size 10000
#define lp_row 10

struct coherency {

	char           type_list[ty_size][st_size];
	size_t                type_index;
	size_t           instance_number;
	size_t          predicate_number;
	size_t          predicate_coverage_number;
	size_t       multi_valued_number;
	size_t repeated_predicate_number;
	char     predicate_list[pr_size][st_size];
	size_t  predicate_lp_var[pr_size];
	size_t  predicate_frequency_list[pr_size];
	size_t  predicate_unique_list[pr_size];
	char      first_subject[st_size];
	char       last_subject[st_size];
	struct coherency           *next;
};


	const char *w3ctype = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>";
	const char *w3csubclass = "<http://www.w3.org/2000/01/rdf-schema#subClassOf>";
	            size_t triples = 1;
           char type[ty_size][st_size];
	         size_t type_index = 0;
	    size_t predicate_index = 0;
      char predicate[pr_size][st_size];
   size_t predicate_frequency[pr_size];
		      size_t multi  =0;
		      size_t repeat =0;
         struct coherency *head = NULL;
void write_after_update_type_system(char type_sys[ty_size][st_size], size_t t_index, char p_list[pr_size][st_size], size_t p_index, char e_subject[st_size], size_t p_frequency[pr_size], char o_list[pr_size][st_size], size_t o_index);

size_t predicate_exist(char p[])
{
	int index = -1;
	if (predicate_index > 0)
	{
		for (size_t i=0; i < predicate_index; i++)
		{
			if (strcmp(predicate[i],p)==0)
			{
				index = i;
				break;

			}
		}
	}


	return index;
}	






void update_type_system(char type_sys[ty_size][st_size], size_t t_index, char p_list[pr_size][st_size], size_t p_index, size_t m_number, size_t r_number, char e_subject[st_size], size_t p_fre_list[pr_size])
{

	struct coherency *tmp = NULL;
	struct coherency   *p = NULL;
	bool type_found = true;
	char merge_list[pr_size][st_size];
	char dup_free_list[pr_size][st_size];
	size_t final_frequency[pr_size];
	size_t dup_free_p_frequency[pr_size];
	size_t final_unique[pr_size];
	size_t final_unique_dup_free[pr_size];


	//#####################
	// These are supposed to be sorted, but just in case
	//#####################
	char temp[st_size];
	
	for(size_t i=0; i < t_index-1 ;i++)
		for(size_t j=0; j <  t_index-i-1 ;j++)
		{
			if(strcmp(type_sys[j],type_sys[j+1]) > 0)
			{
				strcpy(temp,type_sys[j]);
				strcpy(type_sys[j],type_sys[j+1]);
				strcpy(type_sys[j+1],temp);
			}
		}		



	size_t tmp_frequency;
	if (p_index > 0)
	{		
		for(size_t i=0;i< p_index-1;i++)	
			for(size_t j=0;j< p_index-i-1;j++)
			{				
				if(strcmp(p_list[j],p_list[j+1]) > 0)			
				{				
					strcpy(temp,p_list[j]);
					tmp_frequency = p_fre_list[j];

					strcpy(p_list[j],p_list[j+1]);	
					p_fre_list[j] = p_fre_list[j+1];


					strcpy(p_list[j+1],temp);
					p_fre_list[j+1] = tmp_frequency;

				}		
			}
	}	
	//#################
	//
	//#################

	

	if (head != NULL)
	{ //at least one node exists
		p = head;
		do//while(p->next != NULL)
		{
			if (p->type_index == t_index)
			{
				for(size_t i=0; i<t_index; i++)
				{
					if (strcmp(p->type_list[i], type_sys[i]) != 0)
					{
						type_found = false;
					
					}//if
				}//for

			}
			else
			{
				type_found = false;
			}

			if(type_found)
			{
				//TO DO: merging predicates and updating predicate index

				p->instance_number++;
				//#############
				// Merge two predicate lists (remove duplicates)
				//#############

				size_t ii = 0, jj = 0, kk = 0;
			
				
  			      	while ( ii < p->predicate_number && jj < p_index)
			    	{

					if (strcmp(p->predicate_list[ii], p_list[jj]) < 0 )
					{
				    		strcpy(merge_list[kk++],p->predicate_list[ii++]);
						final_frequency[(kk-1)] = p->predicate_frequency_list[(ii-1)];
						final_unique[(kk-1)] = p->predicate_unique_list[(ii-1)];
						
					}
					else //if (strcmp(p->predicate_list[ii], p_list[jj]) > 0 )
					{
						strcpy(merge_list[kk++], p_list[jj++]);
						final_frequency[(kk-1)] = p_fre_list[(jj-1)];
						final_unique[(kk-1)] = 1;
					}
					//else
					//{
					//	strcpy(merge_list[kk++],p->predicate_list[ii++]);
                                        //       final_frequency[(kk-1)] = p->predicate_frequency_list[(ii-1)] + p_fre_list[(jj)] ;

					//}
				
				}// while merge
	
				while (ii < p->predicate_number)
				{
					strcpy(merge_list[kk++],p->predicate_list[ii++]);
					final_frequency[(kk-1)] = p->predicate_frequency_list[(ii-1)] ;
					final_unique[(kk-1)] = p->predicate_unique_list[(ii-1)];
				}
    
				while (jj < p_index)
				{
					strcpy(merge_list[kk++], p_list[jj++]);
					final_frequency[(kk-1)] = p_fre_list[(jj-1)] ;
					final_unique[(kk-1)] = 1;
				}

				//##########
				// Duplication removal, if any
				//##########
				jj =0;
				if (kk > 1)
				{
					for (size_t i=0; i < kk-1 ; i++)
					{
						if (strcmp(merge_list[i],merge_list[i+1]) != 0)
						{
							strcpy(dup_free_list[jj++], merge_list[i]);
							dup_free_p_frequency[jj-1] = final_frequency[i];
							final_unique_dup_free[jj-1]= final_unique[i];
						}
						else
						{
							final_frequency[i+1] = final_frequency[i] + final_frequency[i+1]; //dup_free_p_frequency[jj] += final_frequency[i];
							final_unique[i+1] = final_unique[i] + final_unique[i+1];
						}



					}
	
					strcpy(dup_free_list[jj++], merge_list[kk-1]);
					dup_free_p_frequency[jj-1] = final_frequency[kk-1];
					final_unique_dup_free[jj-1]= final_unique[kk-1];
		    	
				
				}

				
			
				for (size_t i =0; i < jj; i++)
				{
					strcpy(p->predicate_list[i],dup_free_list[i]);
					p->predicate_frequency_list[i] =  dup_free_p_frequency[i];
					p->predicate_unique_list[i] = final_unique_dup_free[i];
				}
				
		 		if (kk == 1) jj=1;
				p->predicate_number = jj;
		    		p->multi_valued_number += m_number;
			  	p->repeated_predicate_number += r_number;
				p->predicate_coverage_number += p_index;
				strcpy(p->last_subject,e_subject);
	



				type_found = false; //NOTE: it is not intuitive
				break;
			}else
			{
				type_found = true;
			}//if-else




			if(p->next != NULL)
				p = p->next;
			else
				break;

		}while(true);//while

	}//else head not NULL

	if(type_found)
	{
		tmp = (struct coherency*) malloc ( sizeof(struct coherency));
		tmp->next = NULL;
		
		for (size_t i=0; i < t_index; i++)
			strcpy(tmp->type_list[i],type_sys[i]);

		tmp->type_index = t_index;

		for (size_t i =0; i < p_index; i++)
		{
			strcpy(tmp->predicate_list[i],p_list[i]);
			tmp->predicate_frequency_list[i] = p_fre_list[i];
			tmp->predicate_unique_list[i]=1;
		}

		tmp->instance_number = 1;
               tmp->predicate_number = p_index;
            tmp->multi_valued_number = m_number;
      tmp->repeated_predicate_number = r_number;
      tmp->predicate_coverage_number = p_index;
      strcpy(tmp->first_subject, e_subject);

		if(head == NULL)
			head = tmp;
		else
			p->next = tmp;
	
	
	
			
			

	}//else	{  found and already updated	}










}
size_t line = 0;


float coh_just_triples_without_types = 0;
size_t size_just_triples_without_types = 0;

size_t multiple_type_triples;
float multiple_fixed_values;
size_t lp_number_of_variables = 0;
size_t coin_index_global = 0;
float coin_list[st_size];
float coh = 0.0;
float fixed_value_type = 0;
size_t types_triples = 0 ;
float multiple_coh;
void printall()
{

	struct coherency * p;
	p = head;
	size_t total_weight = 0;
	size_t total_weight_multiple_type_index_used=0;
	if (head != NULL)
	do
	{
		total_weight +=  p->instance_number + p->predicate_number + 1;//p->type_index;
		total_weight_multiple_type_index_used +=  p->instance_number + p->predicate_number +p->type_index;


		if(p->next != NULL)
			p = p->next;
		else
			break;
	}while(true);
	


	
	p = head;
	if (head != NULL)
	do
	{
		printf("\n***********************************************************************\n");
		printf("***********************************************************************\n");
		printf("\ntype_index (number of types): %zu\n\n ", p->type_index);

		for (size_t i=0; i< p->type_index ; i++)
		{	printf("type %zu:  %s\n", i, p->type_list[i]);}



		printf("\nno instances: %zu\n\nno predicate (excluding types!): %zu\n\nno predicate_coverage_number (excluding type;): %zu\n", p->instance_number, p->predicate_number, p->predicate_coverage_number);

		//printf("\nfirst_subject: %s \nlast_subject: %s ",p->first_subject, p->last_subject);
		//
		for (size_t i =0; i< p->predicate_number ; i++)
		{
			printf("\n predicate %zu: %s   unique_repeat: %zu        frequency: %zu \n ", i, p->predicate_list[i], p->predicate_unique_list[i], p->predicate_frequency_list[i]);
		}
		
			float  tmp_t = (float)p->predicate_coverage_number + ( (float)p->instance_number);//  *  (float) p->type_index);
			float multiple_tmp_t = (float)p->predicate_coverage_number + ( (float)p->instance_number *  (float) p->type_index);



			float    cov = ( tmp_t / ((float)p->instance_number * ( (float)p->predicate_number +1)));//  (float) p->type_index ) )  );
			float    multiple_cov = ( multiple_tmp_t / ((float)p->instance_number * ( (float)p->predicate_number +  (float) p->type_index ) )  );




			float weight = ( ((float)p->instance_number + (float)p->predicate_number + 1 ) / (float)total_weight  );

			float multiple_weight = ( ((float)p->instance_number + (float)p->predicate_number + (float) p->type_index ) / (float)total_weight_multiple_type_index_used  );

			
			        coh += cov * weight; 
				multiple_coh += multiple_cov * multiple_weight;


		printf("\ncov: %f \nweight: %f\n cov * weight: %f \n", cov, weight, cov * weight);
		printf("\nmultiple_cov: %f \nmultiple_weight: %f\n", multiple_cov, multiple_weight);

		float coin = ( 1.00000 / ((float)p->instance_number * ( (float)p->predicate_number + 1 )  )) * weight;
		float multiple_coin = ( 1.00000 / ((float)p->instance_number * ( (float)p->predicate_number + (float) p->type_index )  )) * multiple_weight;


		printf("\n\ncoin: %f ", coin);
		printf("\n\nmultiple_coin: %f ", multiple_coin);
		printf("\n\nfixed_number (coin * type[s]):  %f \n\n", coin * ( (float)p->instance_number ));
		printf("\n\nMULTIPLE fixed_number (coin * type[s]):  %f \n\n", multiple_coin * ( (float)p->instance_number + (float) p->type_index   ));

		fixed_value_type += coin * ( (float)p->instance_number  );
		multiple_fixed_values += multiple_coin * ( (float)p->instance_number + (float) p->type_index) ;
		types_triples += p->instance_number  ;
		multiple_type_triples += p->instance_number  *  p->type_index;


		
		printf("\n***********************************************************************\n");

		//lp_number_of_variables += p->predicate_number + p->type_index;
		lp_number_of_variables += p->predicate_number;

		
		coin_list[coin_index_global++] =  coin ;





				
		if(p->next != NULL)
			p = p->next;
		else
			break;


	}while(true);
	
	printf("\n\n#######################\n\nCoherency: %f\n", coh);
	printf("\n\n#######################\n\nMULTIPLE Coherency: %f\n", multiple_coh);
	printf("\n\n#######################\n\nJust types (coh): %f\n", fixed_value_type);
	printf("\n\n#######################\n\nMULTIPLE Just types (multiple_coh): %f\n", multiple_fixed_values);
	printf("\n\n#######################\n\nTriples Just types(Avg.): %f\n", (float) multiple_type_triples / (float) types_triples);
	printf("\n\n#######################\n\nMULTIPLE Triples Just types: %zu\n", multiple_type_triples);

	coh_just_triples_without_types = multiple_coh - multiple_fixed_values;
	//size_just_triples_without_types = line - multiple_type_triples;
	

}

FILE *f_write_after_adjust;
char  *output_filename;
void main_coh (char *argv, bool adjust)
{

	int readfile = 0;
	type_index = 0;
	size_t object_index = 0;

	
	char subject[st_size], tr_predicate[st_size], object[st_size];//, dot[4];
	char tmp_subject[st_size];
        char object_2_write[pr_size][st_size];
	
	bool multi_flag = false;
	bool flag_bnode = false;
	char *check_ws;
	char gt = '>';
	long fpos;

	FILE *fp = NULL;

	for (size_t i=0; i < pr_size; i++)
		predicate_frequency[i]=0;

	//if(argc > 1)
		
	fp = fopen(argv, "r");


        if (fp == NULL)
	{
		printf ("Error opening the file\n\n");
		exit(EXIT_FAILURE);
	
	} else
       	{
		if (adjust)
		{
			f_write_after_adjust = fopen(output_filename, "w");
			if (f_write_after_adjust == NULL)
			{
				printf("Error opening file!\n");
				exit(1);
			}
		}

		
		//readfile = fscanf(fp, "%s %s %s %s\n", subject, tr_predicate, object, dot);

		fpos = ftell(fp);
		readfile = fscanf(fp, "%s %s %[^\n]\n", subject, tr_predicate, object);
		type_index = 0;

		if (subject[0] == '_' && subject[1]==':')
		      	flag_bnode = true;
	      	else
		     	check_ws = strstr(subject,">");

	      	if (!flag_bnode && check_ws == NULL)
	      	{
		      	fseek(fp, fpos, SEEK_SET);
		      	readfile = fscanf(fp, "%[^>]> %s %[^\n]\n", subject, tr_predicate, object);
		      	//printf("%lu",strlen(subject));
			strncat(subject, &gt, 1);
		      	//subject[strlen(subject)]='>';
		      	//subject[strlen(subject)+1]='\0';
	     	}
		
		strcpy(tmp_subject, subject);
		flag_bnode = false;
		
		

		while (readfile != EOF)
		{
			
				line++;

				//if (strcmp(subject,&"<http://www.University0.edu>")==0 )
				if (line == 63416370 && adjust)
					printf("dummy");

		
			
			{
				if (readfile == 3)
				{
					#if (DEBUG == 1)
					{
						//printf("line:%zu \n s: %s \n  p:%s \n o:%s \n \n\n\n ", line, subject, tr_predicate, object);
						//sleep(1);
						printf("line:%zu\t",line);

					}
					#endif
					
	       						
	       				

					if (strcmp(tr_predicate, w3ctype) == 0  || strcmp(tr_predicate, w3csubclass) == 0 )
					{	
						strcpy(type[type_index++], object);
					}else
					{
						int p_exist_index = predicate_exist(tr_predicate); 
						if (p_exist_index == -1)
						{
							strcpy(predicate[predicate_index],tr_predicate);
							strcpy(object_2_write[object_index++], object);
							predicate_frequency[predicate_index]++;
							predicate_index++;

							multi_flag = false;
						}
						else
						{
							predicate_frequency[p_exist_index]++;
							strcpy(object_2_write[object_index++], object);
							if (!multi_flag)
							{
								multi++;
								multi_flag = true;
								repeat++;
							}

							repeat++;

						}
					}


				}
				else
				{
					printf ("Error, line %zu in wrong format!\n\n", line);
				}
			}
			
			





	                fpos = ftell(fp);
			readfile = fscanf(fp, "%s %s %[^\n]\n", subject, tr_predicate, object);

			if (subject[0] == '_' && subject[1]==':')
				flag_bnode = true;
			else
				check_ws = strstr(subject,">");
	
			if (!flag_bnode && check_ws == NULL)
			{
				fseek(fp, fpos, SEEK_SET);
				readfile = fscanf(fp, "%[^>]> %s %[^\n]\n", subject, tr_predicate, object);
				strncat(subject, &gt, 1);
				//printf("%lu",strlen(subject));
				//subject[strlen(subject)]='>';
				//subject[strlen(subject)+1]='\0';
			}
			flag_bnode = false;


			if (strcmp(tmp_subject,subject) != 0)
			{
				//strcpy(tmp_subject,subject);
				//*************************
				if (type_index > 0)
				{

					if (!adjust)
						update_type_system(type, type_index, predicate, predicate_index, multi, repeat, tmp_subject, predicate_frequency);
					else
						write_after_update_type_system(type, type_index, predicate, predicate_index, tmp_subject, predicate_frequency, object_2_write, object_index);


				}

				for (size_t i=0; i<predicate_index; i++)
					predicate_frequency[i]=0;


	    			     type_index = 0;
		    		predicate_index = 0;
		      		         multi  = 0;
	  			         repeat = 0;
	     			   object_index = 0;
				   tmp_subject[0] = '\0';

				 strcpy(tmp_subject,subject);
		
				//*************************
				
				
			}
			
				
		}

		
		//fclose(fp);

		//for the last sets of triples
		if (type_index > 0)
		{
			if (!adjust)
				update_type_system(type, type_index, predicate, predicate_index, multi, repeat, tmp_subject, predicate_frequency);
			else
				write_after_update_type_system(type, type_index, predicate, predicate_index, tmp_subject, predicate_frequency, object_2_write, object_index);


		}

		fclose(fp);
		if(adjust)
			fclose(f_write_after_adjust);

		//printall();

		
	}//else (the file is existed and opend successfully)

}//main_coh



/*
 * * *
 */

double row_var[lp_size];

int lp_size_coh_adjust(float desired_coherency, float desired_size, float relax)
{
      lprec *lp;
      int Ncol, *colno[lp_size], j, ret = 0;
      REAL *row[lp_size+1] ;
      size_t row_index = 0;
      size_t single_var_index = 4;
      /* We will build the model row by row
       * So we start with creating a model with 0 rows and lp_number_of_variables columns */
      Ncol = lp_number_of_variables; /* there are \"lp_number_of_variables\" variables in the model */
      lp = make_lp(0, Ncol);
    
      if(lp == NULL)
	      ret = 1; /* couldn't construct a new model... */

      
      if(ret == 0) 
      {
	      /* let us name our variables. Not required, but can be useful for debugging */
	      //set_col_name(lp, 1, "x");  //later I'll name them!
	      //set_col_name(lp, 2, "y");
	     /* create space large enough for one row */

	      for(size_t i=0; i < lp_size; i++  )
	      {
		      colno[i] = (int  *) malloc(Ncol * sizeof(*colno[i]));
	
		    
		      row[i]   = (REAL *) malloc(Ncol * sizeof( *row[i] ));
	
		      if((colno[i] == NULL) || (row[i] == NULL))
			      ret = 2;

	      }

	      
	      set_add_rowmode(lp, TRUE);  /* makes building the model faster if it is done rows by row */
      }//if



      struct coherency * p;
      p = head;



      j = 0;
      size_t var_index = 1;
      size_t coin_index = 0;

      if (head != NULL)
      	      do
      	      {
		      if(ret == 0) 
		      {
			      //set_add_rowmode(lp, TRUE);
			      /* makes building the model faster if it is done rows by row */
			      /* construct first row (120 x + 210 y <= 15000) */
			      row_index=0;

			      for (size_t i=0; i < p->predicate_number; i++)
			      {
				      colno[row_index][j] = var_index; /* first row -- objective */
				      row[row_index++][j] = coin_list[coin_index];			      

				      colno[row_index][j] = var_index; /* second row -- coherency */
                                      row[row_index++][j] = coin_list[coin_index]; 

				      colno[single_var_index][0] = var_index;
				      /* third row -- average coins */
                                      row[single_var_index][0] = 1;

				      
				     if(!add_constraintex(lp, 1 , row[single_var_index], colno[single_var_index], LE,( p->predicate_frequency_list[i])))
					     ret = 3;

				      single_var_index++;


				      colno[single_var_index][0] = var_index; /* fourth row -- average coins */
                                      row[single_var_index][0] = 1;

                                      if(!add_constraintex(lp, 1 , row[single_var_index], colno[single_var_index], GE, 1 ))
                                            ret = 3;

				      single_var_index++;


				      
				      colno[row_index][j] = var_index; // second row -- coherency /
                                      row[row_index++][j] = (float)( (float)p->predicate_frequency_list[i] / (float)p->predicate_unique_list[i]) ;

				      
       				      colno[row_index][j] = var_index; // second row -- coherency /
                                      row[row_index++][j] = (float)( (float)p->predicate_frequency_list[i] / (float)p->predicate_unique_list[i]) ;

				     
		      


				      j++;
				      row_index=0;
				      var_index++;
				      


			      }//for

			      coin_index++;

			      /*

			      row_index=0;
			      
			      colno[row_index][j] = var_index; 
			      // first row -- objective /
			      row[row_index++][j] = coin_list[coin_index] ;
			      


			      colno[row_index][j] = var_index; // second row -- coherency /
			      row[row_index++][j] = coin_list[coin_index++];

			      
			      colno[single_var_index][0] = var_index; 
			      /// third row -- average coins /
			      row[single_var_index][0] = 1;

			      if(!add_constraintex(lp,1 , row[single_var_index], colno[single_var_index], EQ, (p->type_index * p->instance_number)))
			      	ret = 3;

			      single_var_index++;
                            
		      	      		      

			     
			      colno[row_index][j] = var_index; // second row -- coherency /
			      row[row_index++][j] = 1;


			      colno[row_index][j] = var_index++; // second row -- coherency /
			      row[row_index][j++] = 1;
			      


			      row_index = 0;
			      */


		      }//if

	      		     
		      if(p->next != NULL)
      			      p = p->next;
      		      else
      			      break;
      	      }while(true);
    
    
         
      if(ret == 0) 
      {
	      /* add the row to lpsolve */

	      printf("\n\n#######\n desired coherency: %f  (type+non-type: %f) \n real size: %zu \n min size: %f  (type+non-type: %f) \n max size: %f (type+non-type: %f) \n\n\n ", 
			      ( coh_just_triples_without_types * desired_coherency),
			      ( coh_just_triples_without_types * desired_coherency)+multiple_fixed_values,
			      line,
			      ((1.0 - relax) *  (desired_size * size_just_triples_without_types  )),
			      multiple_type_triples +((1.0 - relax) *  (desired_size * size_just_triples_without_types  )),
			      ((1.0 + relax) *  (desired_size * size_just_triples_without_types  )),
			      multiple_type_triples +((1.0 + relax) *  (desired_size * size_just_triples_without_types  ))
		    );


                                                            //coh_just_triples_without_types
	      if(!add_constraintex(lp, j, row[1], colno[1], LE,  ( coh_just_triples_without_types * desired_coherency)   )) // desired coherency
		      ret = 3;

	      if(!add_constraintex(lp, j, row[2], colno[2], LE, ((1.0 + relax) *  (desired_size * size_just_triples_without_types  ))    )) // desired triples -- max
                      ret = 3;

	      if(!add_constraintex(lp, j, row[3], colno[3], GE, ((1.0 - relax) *  (desired_size * size_just_triples_without_types  ))    ))   // desired triples -- min
                      ret = 3;

	      set_add_rowmode(lp, FALSE); /* rowmode should be turned off again when done building the model */


	      /* set the objective in lpsolve */
	      if(!set_obj_fnex(lp, j, row[0], colno[0]))
		      ret = 4;






      }


      if(ret == 0) 
      {
	      /* set the object direction to maximize */
	      set_maxim(lp);
	      /* just out of curioucity, now show the model in lp format on screen */
	      /* this only works if this is a console application. If not, use write_lp and a filename */
	      write_LP(lp, stdout);
	      /* write_lp(lp, "model.lp"); */
	      /* I only want to see important messages on screen while solving */
	      set_verbose(lp, IMPORTANT  );

	      //set_scaling(lp, SCALE_CURTISREID);
	      //set_epsint(lp, 1.0e-3);
	      /* Now let lpsolve calculate a solution */
	      

	      ret = solve(lp);
	      printf("%f\t", get_constr_value(lp, 0, 0, NULL, NULL));
	      
	      


	      print_constraints(lp, 1);
      
	      if(ret == OPTIMAL)
		      ret = 0;
	      else
		      ret = 5;
      }
      
      if(ret == 0) 
      {
	      /* a solution is calculated, now lets get some results */
	      /* objective value */
	      printf("Objective value: %f\n", get_objective(lp));
	      /* variable values */
	     	      get_variables(lp, row_var);

	      for(j = 0; j < Ncol; j++)
		      printf("%s: %f\t", get_col_name(lp, j + 1), row_var[j]);
	      /* we are done now */
      }
    
      /* free allocated memory */

      //printf("\n\n#######\n original: %f \n desired coherency: %f \n real size: %zu \n min size: %f \n max size: %f \n\n\n ", coh,  (coh - desired_coherency), line, ((1.0 - relax) * (line - (desired_size * line))) , ((1.0 + relax) * (line - (desired_size * line))) );
//
//printf("\n\n#######\n desired coherency: %f \n real size: %zu \n min size: %f \n max size: %f \n\n\n ", ( coh_just_triples_without_types * desired_coherency), ((1.0 - relax) * (desired_size * size_just_triples_without_types  )) , ((1.0 + relax) *  (desired_size * size_just_triples_without_types  ))  );




      if(row != NULL && colno != NULL)
	      for(size_t i=0; i < lp_size; i++  )
	      {
		      free(colno[i]);
       		      free(row[i]);
	      }


      
      if(lp != NULL) {
      	  /* clean up such that all used memory by lpsolve is freed */
	      delete_lp(lp);
      }



      return(ret);
}


/*
 * * *
 */

void update_type_system_after_lp()
{



      struct coherency * p;
      p = head;
      size_t total_remain_triple=0;

      
      size_t var_index = 0;
      //size_t coin_index = 0;

      if (head != NULL)
              do
              {
		      for (size_t i=0; i < p->predicate_number; i++)
		      {
			      p->predicate_lp_var[i] = (size_t)row_var[var_index++];
			      printf("\nvar_index: %zu   pre: %s  no: %zu\t",var_index, p->predicate_list[i],p->predicate_lp_var[i] );
			      total_remain_triple += p->predicate_lp_var[i];
		      }
		      if(p->next != NULL)
			      p = p->next;
                      else
                              break;
	      }while(true);

      printf("\ntotal pre: %zu \n",total_remain_triple );
      total_remain_triple = 0;





}













char write_line[st_size] ;
size_t written_line = 0;
void write_after_update_type_system(char type_sys[ty_size][st_size], size_t t_index, char p_list[pr_size][st_size], size_t p_index, char e_subject[st_size],size_t p_frequency[pr_size], char o_list[pr_size][st_size], size_t o_index ) 
{

	struct coherency   *p = NULL;
	bool type_found = true;
	char white_space = ' ';
	char new_line = '\n';
	bool write_2_file = true;
	size_t temp_j =0;

	


	
       	if (head != NULL)
        { //at least one node exists
                p = head;
                do//while(p->next != NULL)
                {
                        if (p->type_index == t_index)
                        {
                                for(size_t i=0; i<t_index; i++)
                                {
                                        if (strcmp(p->type_list[i], type_sys[i]) != 0)
                                        {
                                                type_found = false;

                                        }//if
                                }//for

                        }
                        else
                        {
                                type_found = false;
                        }

                        if(type_found)
                        {
				if(written_line==6)
					printf("dummy");

				for(size_t j=0; j<p_index; j++)
				{
					for(size_t i=0; i < p->predicate_number ;i++)
					{
						if (strcmp(p_list[j],p->predicate_list[i] ) == 0)
						{
							temp_j = j;
							if (p->predicate_lp_var[i] > 0)
							{
							
								
								while (p->predicate_lp_var[i] > 0 &&  p_frequency[j] > 0)
								{
									p->predicate_lp_var[i]--;
									p_frequency[j]--;									
									strncat(write_line,e_subject, (size_t)strlen(e_subject));
									strncat(write_line,&white_space, 1);
									strncat(write_line,p_list[j], (size_t)strlen(p_list[j]));
									strncat(write_line,&white_space, 1); 									
									strncat(write_line,o_list[temp_j], (size_t)strlen(o_list[temp_j]));
									strncat(write_line,&new_line, 1);
									temp_j++;
									//printf("written_line:%zu  write@file: %s",written_line++, write_line);
									fprintf(f_write_after_adjust,"%s",write_line);		
									strcpy(write_line,&"");
									/*
									if (write_2_file)
									{
										for (size_t k=0; k<t_index; k++ )
										{
											strncat(write_line,e_subject, (size_t)strlen(e_subject));
											strncat(write_line,&white_space, 1);
											strncat(write_line,w3ctype, (size_t)strlen(w3ctype));
											strncat(write_line,&white_space, 1); 									
											strncat(write_line,type_sys[k], (size_t)strlen(type_sys[k]));
											strncat(write_line,&new_line, 1);
											temp_j++;
											//printf("written_line:%zu  write@file: %s",written_line++, write_line);		
											fprintf(f_write_after_adjust,"%s",write_line);
											strcpy(write_line,&"");



										}
										
										write_2_file=false;
									}
									*/
			

									
								}
								break;

							}
						}

					}
				}
                               



                                type_found = false; //NOTE: it is not intuitive
                                break;
                        }else
                        {
                                type_found = true;
                        }//if-else




                        if(p->next != NULL)
                                p = p->next;
                        else
                                break;

                }while(true);//while

        }//else head not NULL


	
	for (size_t k=0; k<t_index; k++ )
	{
		strncat(write_line,e_subject, (size_t)strlen(e_subject));
		strncat(write_line,&white_space, 1);
		strncat(write_line,w3ctype, (size_t)strlen(w3ctype));
		strncat(write_line,&white_space, 1);                                                                    
		strncat(write_line,type_sys[k], (size_t)strlen(type_sys[k]));
		strncat(write_line,&new_line, 1);
		//printf("written_line:%zu  write@file: %s",written_line++, write_line);                
		fprintf(f_write_after_adjust,"%s",write_line);
		strcpy(write_line,&"");
	}


}

#define FLOATARG atof(optarg)
int main (int argc, char *argv[])
{

	char  *input_filename;
	bool  operation;
	float _d_coh;
	float _d_size;
	float _p_relax;
	


	int opt;
    	while ((opt = getopt(argc, argv, "i:w:o:c:s:r:")) != -1) 
	{
		
		
		switch (opt) 
		{
			case 'i':
		    		input_filename = optarg;
		    		break;
			case 'w':
				output_filename = optarg;
				break;
			case 'o':
		    		operation = optarg && strcmp(optarg,"1")==0;
		    		break;
			case 'c':
		    		_d_coh= FLOATARG;// strtof(optarg, &endptr);
		    		break;
			case 's':
				_d_size= FLOATARG;//strtof(optarg ,&endptr);
				break;
			case 'r':
				if ( strcmp(optarg, "0")!=0 || strcmp(optarg,"")!=0 )
					_p_relax = FLOATARG;//strtof(optarg, &endptr);
				else
					_p_relax = 0.0;
				break;
			default:
				printf("\n arguments are not used properly");
				break;
		}
    	}




	if (!operation) // 0 for just coherency
	{
		main_coh(input_filename, false);
		printall(); //coherency will be calculated as well as number of variables
	}
	else
	{

		main_coh(input_filename, false);
		printall(); //coherency will be calculated as well as number of variables
		printf("\n\n Optimization Started....\n");
		size_just_triples_without_types = line - multiple_type_triples;
		printf("\n\n LpSolver ret: %d \n", lp_size_coh_adjust(  _d_coh  , _d_size , _p_relax));// LUBM 100k
		update_type_system_after_lp();
		line = 0;
		main_coh(input_filename, true);
	}






	/* int lp_size_coh_adjust(float desired_coherency, float desired_size, float relax)  */
	/* (( coh_just_triples_without_types * desired_coherency))   )) // desired coherency*/ 
        /* ((1.0 + relax) * (  desired_size * size_just_triples_without_types  ))   // desired triples -- max*/
        /* ((1.0 - relax) * (  desired_size * size_just_triples_without_types  ))   // desired triples -- min*/


	//printf("\n\n Optimization Started....\n");

	//size_just_triples_without_types = line - multiple_type_triples;
	
	//printf("\n\n LpSolver ret: %d \n", lp_size_coh_adjust(  0.1  , 0.25 , 0));// LUBM 100k
	//printf("\n\n LpSolver ret: %d \n", lp_size_coh_adjust(  0.2  , 0.25 , 0));// LUBM 100k
	//printf("\n\n LpSolver ret: %d \n", lp_size_coh_adjust(  0.3  , 0.25 , 0));// LUBM 100k
	//printf("\n\n LpSolver ret: %d \n", lp_size_coh_adjust(  0.4  , 0.25 , 0));// LUBM 100k
	//printf("\n\n LpSolver ret: %d \n", lp_size_coh_adjust(  0.5  , 0.25 , 0.12));// LUBM 100k

	
	//***********IMPOSSIBLE printf("\n\n LpSolver ret: %d \n", lp_size_coh_adjust(  0.1  , 0.5 , 0));// LUBM 100k
	//printf("\n\n LpSolver ret: %d \n", lp_size_coh_adjust(  0.2  , 0.5 , 0));// LUBM 100k
	//printf("\n\n LpSolver ret: %d \n", lp_size_coh_adjust(  0.3  , 0.5 , 0));// LUBM 100k
	//printf("\n\n LpSolver ret: %d \n", lp_size_coh_adjust(  0.4  , 0.5 , 0));// LUBM 100k
	//printf("\n\n LpSolver ret: %d \n", lp_size_coh_adjust(  0.5  , 0.5 , 0));// LUBM 100k
	//printf("\n\n LpSolver ret: %d \n", lp_size_coh_adjust(  0.6  , 0.5 , 0));// LUBM 100k
	//printf("\n\n LpSolver ret: %d \n", lp_size_coh_adjust(  0.7  , 0.5 , 0));// LUBM 100k
	//printf("\n\n LpSolver ret: %d \n", lp_size_coh_adjust(  0.8  , 0.5 , 0.12));// LUBM 100k
	
	//***********IMPOSSIBLE	 printf("\n\n LpSolver ret: %d \n", lp_size_coh_adjust(  0.1  , 0.75 , 0));// LUBM 100k
	//***********IMPOSSIBLE  printf("\n\n LpSolver ret: %d \n", lp_size_coh_adjust(  0.2  , 0.75 , 0));// LUBM 100k
	//printf("\n\n LpSolver ret: %d \n", lp_size_coh_adjust(  0.3  , 0.75 , 0));// LUBM 100k
	//printf("\n\n LpSolver ret: %d \n", lp_size_coh_adjust(  0.4  , 0.75 , 0));// LUBM 100k
	//printf("\n\n LpSolver ret: %d \n", lp_size_coh_adjust(  0.5  , 0.75 , 0));// LUBM 100k
	//printf("\n\n LpSolver ret: %d \n", lp_size_coh_adjust(  0.6  , 0.75 , 0));// LUBM 100k
	//printf("\n\n LpSolver ret: %d \n", lp_size_coh_adjust(  0.7  , 0.75 , 0));// LUBM 100k
	//printf("\n\n LpSolver ret: %d \n", lp_size_coh_adjust(  0.8  , 0.75 , 0));// LUBM 100k
	//printf("\n\n LpSolver ret: %d \n", lp_size_coh_adjust(  0.9  , 0.75 , 0));// LUBM 100k

//###################################################################################################################
//###################################################################################################################

	
		//update_type_system_after_lp();
		//line = 0;
		//main_coh(argv[1], true);















	return 0;
}


