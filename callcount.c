/*
Javid Karimbayli 21002742
CS342-1
Project 2
*/

#include <sys/types.h>
#include <ctype.h>
#include <sys/stat.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <pthread.h>
#define KRED  "\x1B[31m"

/*
Global variables here
*/
int n; // number of threads
int r;
unsigned int syear, eyear; //start and end year
unsigned int startId, endId;
char out[20];


int file_exist (char *filename)
{
  struct stat   buffer;
  return (stat (filename, &buffer) == 0);
}
struct list {
  unsigned int caller_id;
  unsigned int called_id;
  int caller_count;
  int year;
  struct list *next;
};
typedef struct list LIST;

// BubleSort for Sorting A-B list
void bubleSort(struct list *tempPtr,struct list *tempNext,struct list *headS,struct list *currentS ){
	while(tempNext != NULL){
		while(tempNext != tempPtr){
			if(tempNext->caller_id < tempPtr->caller_id){
				int tmp;
				tmp = tempPtr->caller_id;
				tempPtr->caller_id = tempNext->caller_id;
				tempNext->caller_id = tmp;
			}
			tempPtr = tempPtr->next;
		}
		tempPtr = headS;
		tempNext = tempNext->next;
       }	currentS = headS;
}
void removeDuplicates(struct list *start)
{
  struct list *ptr1, *ptr2, *dup;
  ptr1 = start;

  /* Pick elements one by one */
  while(ptr1 != NULL && ptr1->next != NULL)
  {
     ptr2 = ptr1;

     /* Compare the picked element with rest of the elements */
     while(ptr2->next != NULL)
     {
       /* If duplicate then delete it */
       if(ptr1->caller_id == ptr2->caller_id)
       {
          /* sequence of steps is important here */
          dup = ptr2->next;
          ptr2->next = ptr2->next->next;
          free(dup);
          ptr1->caller_id=ptr1->caller_id+1;
       }
       else /* This is tricky */
       {
          ptr2 = ptr2->next;
       }
     }
     ptr1 = ptr1->next;
  }
}

struct param{
  int r;
  int j;
  char fname[20];
  int thread_number;
  int count_inputs;
  LIST *origin;
  char final_file[10];
};
void *mapper_worker(void *arg){
  struct param *p;
  p =(struct param *) arg;
  int i;
  int thread_index = p->j;
  char in_file_name[20];
  strcpy(in_file_name, p->fname);

  char buff[30];
  char tempName[10];
  char tempI[2];
  char tempJ[2];

  printf ("\033[32;1m\n......starting thread[%d] ....\033[0m\n",thread_index);

  FILE *fp = fopen(in_file_name, "r");
  while (fscanf(fp,"%s",buff)>0) {
    int caller_id = atoi(buff);
    i = caller_id %r;// CallerID mod R
    sprintf(tempJ,"%d",p->j);
    sprintf(tempI,"-%d",i);
    strcpy(tempName,"temp");
    strcat(tempName,tempJ);
    strcat(tempName,tempI);

    printf("string is: %s\n", tempName);
    //char buffer[30];
    FILE *fptemp = fopen(tempName, "a");
    fputs(buff,fptemp);
    fputs(" ", fptemp);
    fscanf(fp,"%s",buff);
    fputs(buff,fptemp);
    fputs(" ", fptemp);
    fscanf(fp,"%s",buff);
    fputs(buff,fptemp);
    fputs("\n", fptemp);
    fclose(fptemp);
  }
  fclose(fp);
  printf("\033[32;1mend of working in thread[%d] for file[%s]\033[0m  \n",p->j, p->fname);
}
void *reducer_worker(void *arg){
  LIST *current, *head, *mainH;
  LIST *head_distinct, *temp_distinct,*tempS_distinct;
  head_distinct = temp_distinct = tempS_distinct = NULL;
  head = current = NULL;
  //int distinct;
  temp_distinct = head_distinct;
  tempS_distinct = temp_distinct;
  current = head;
  mainH = head;


  struct param *p;
  p = (struct param *) arg;
  p->origin = mainH;
  printf("\033[32;1m=========== REDUCER_THREAD[%d] working==========\033[0m  \n",p->thread_number );
  char rfileName[20];
  char indexI[4];
  char indexJ[2];
  int inputfile_count = p->count_inputs;
  int choice=0;
  char buff1[30];
  //int choose=0;
  char ltemp[10];
  char lth[3];


  for(int i=0;i< inputfile_count;i++){
    sprintf(indexJ,"%d",i);
    sprintf(indexI,"-%d",p->thread_number);
    strcpy (rfileName,"temp");
    strcat(rfileName,indexJ);
    strcat (rfileName,indexI);


    //FILE *fpt;// = fopen(rfileName, "r");
    char bufft[30];
    unsigned int buff_caller_id;
    unsigned int buff_called_id;
    unsigned int buff_year;

    if (file_exist(rfileName))
    {
      printf("temp is: %s\n", rfileName);
      FILE *fpt = fopen(rfileName, "r");
      while (fscanf(fpt,"%s", bufft)>0){
        choice=1;
        buff_caller_id = atoi(bufft);
        fscanf(fpt,"%s", bufft);
        buff_called_id = atoi(bufft);
        fscanf(fpt,"%s", bufft);
        buff_year = atoi(bufft);


        while (tempS_distinct!=NULL) {

          if (tempS_distinct->caller_id == buff_caller_id && tempS_distinct->called_id == buff_called_id) {
            choice = 0;
          }
          tempS_distinct = tempS_distinct->next;
          /* code */
        }
        tempS_distinct = head_distinct;

        if (choice) {
          LIST *node = malloc(sizeof(LIST));
          node->caller_id = buff_caller_id;//note : strdup is not standard function
          node->called_id = buff_called_id;
          node->year = buff_year;
          node->next = NULL;

          if(head_distinct == NULL){
              head_distinct = node;
              temp_distinct = node;
          } else {
              temp_distinct->next = node;
            temp_distinct = temp_distinct->next;
            }
          /* code */
        }


      }
  fclose(fpt);
  struct list *tmpPtr = head_distinct;
  struct list *tmpNxt = head_distinct->next;

  bubleSort(tmpPtr,tmpNxt,head_distinct,head_distinct->next);
}


      //printf("\n-----%d--------\n", current->caller_id);
      printf("\n\033[32;1mContent of temp%d-%d.txt :\033[0m\n",i,p->thread_number);
      for(LIST *curr = head_distinct; curr ; curr=curr->next){
          choice = 0;
          printf("\033[32;1mcaller id: %d called id:%d \033[0m\n", curr->caller_id,curr->called_id);

          for (LIST *c=head; c; c=c->next) {
            if( curr->caller_id==c->caller_id){
              choice = 1;
              //printf("same\n" );
              /*countOfCaller++;
              currentS->caller_count=countOfCaller;*/
              c->caller_count= c->caller_count +1;
            }            /* code */
          }


          if (choice==0){
            LIST *node = malloc(sizeof(LIST));
            node->caller_id = curr->caller_id;//note : strdup is not standard function
            node->called_id = curr->called_id;
            node->year=curr->called_id;
            node->caller_count =1;
            node->next =NULL;

            if(head == NULL){
                head = node;
                current = head;
            } else {
                current->next = node;
                current = current->next;
            }
          }

      }
      head=head->next;
  }//for br



  for(current = head; current ; current=current->next){
      printf(" thread sum caller id: %d  count: %d\n", current->caller_id,current->caller_count);
      strcpy(ltemp, "temp");
      sprintf(lth,"%d",p->thread_number);
      strcat(ltemp,lth);
      FILE *fptemp = fopen(ltemp, "a");
      sprintf(buff1, "%d", current->caller_id);
      fputs(buff1, fptemp);
      fputs(" ", fptemp);
      sprintf(buff1, "%d", current->caller_count);
      fputs(buff1, fptemp);
      fputs("\n", fptemp);
      fclose(fptemp);

  }




}
void *merger_worker(void *arg){
  printf("\n\033[32;1m MERGER THREAD\033[0m\n");
  char  index[3], readfil[10], buff[20];
  struct param *p;
  p = (struct param *) arg;
  LIST *mhead=NULL, *mtemp=NULL;


  FILE *merger = fopen(p->final_file, "w");
printf("out is:%s \n",p->final_file);

  for(int i=0; i< p->r ;i++){
    strcpy(readfil,"temp");
    sprintf(index, "%d", i);
    strcat(readfil, index);
    printf("read file :%s  \n",readfil);

    if (file_exist(readfil)){
      FILE *rp = fopen(readfil, "r");
      while (fscanf(rp, "%s", buff) > 0){
        LIST *node = malloc(sizeof(LIST));
        node->caller_id = atoi(buff);
        fscanf(rp, "%s", buff);
        node->caller_count = atoi(buff);
        node->next = NULL;

        if(mhead == NULL){
            mhead = node;
            mtemp =mhead;
        } else {
                        mtemp->next = node;
            mtemp  = mtemp->next;
        }
      }
      fclose(rp);
    }
  }
  mtemp = mhead;
  struct list *tmpPtr = mhead;
  struct list *tmpNxt = mhead->next;

  bubleSort(tmpPtr,tmpNxt,mhead ,mhead->next);
  //removeDuplicates(mhead);

  for(LIST *current = mhead; current ; current=current->next){
      printf("caller id: %d  count: %d\n", current->caller_id,current->caller_count);
  }
  while (mhead!=NULL) {
    sprintf(buff, "%d", mhead->caller_id);
    fputs(buff, merger);
    fputs(" ", merger);
    sprintf(buff, "%d", mhead->caller_count);
    fputs(buff, merger);
    fputs("\n", merger);
    mhead = mhead->next;
  }

  fclose(merger);

}

int main(int argc, char *argv[])
{
  clock_t start = clock();
  printf("Number of arguments: %d \n",argc);
   if( argc < 8) { printf("at least 8 argument expected  - callcount<N> <R> <infile1> ... <infileN> inalfile> <syear> <eyear> <sID> <eID>\n");exit(0);}

   n = atoi( argv[1]); //# of threads
   r =  atoi( argv[2]); //# of temp files

   if(n>10 || n<1 ){printf("Number of files should be at range[1-10]\n"); exit(0);}
   if(r>50){printf("R should be <=50\n"); exit(0);}

   int indexSaver=0;
   char *filename[n]; // for input files
   int ret;
   struct param par;
   //char test;


   //this for realize index of input files
   for(int i=0;i<n;i++){
     filename[i]=argv[3+i];
     indexSaver = 3+i;
     printf("filename%d: %s\t\n",i, filename[i]);
   }

   strcpy(par.final_file, argv[indexSaver+1]);
   strcpy(out, argv[indexSaver+1]);
   syear = atoi(argv[indexSaver+2]);
   eyear = atoi(argv[indexSaver+3]);
   startId = atoi(argv[indexSaver+4]);
   endId = atoi (argv[indexSaver+5]);
   printf("indexSaver: %d\tsyear: %d\teyear: %d\tstartId: %d\tendId: %d\n",indexSaver,syear,eyear,startId,endId);


   pthread_t tid[n];
   for(int i =0; i < n; i++){
     strcpy(par.fname, filename[i]);
     par.j=i;
     ret = pthread_create(&tid[i],NULL,&mapper_worker,(void *) &par);
     if (ret != 0) { printf("mapper_thread create failed \n");exit(1);}
     ret = pthread_join(tid[i], NULL);
   }

   par.r=r;

   par.count_inputs=n;
   pthread_t reducer_tid[n];
   for(int k =0; k < r; k++){

     //strcpy(par.fname, filename[k]);
     par.thread_number=k;
     ret = pthread_create(&reducer_tid[k],NULL,&reducer_worker,(void *) &par);
     if (ret != 0) { printf("reducer_thread create failed \n");exit(1);}
     ret = pthread_join(reducer_tid[k], NULL);
   }

   pthread_t merger_tid;
   ret = pthread_create(&merger_tid,NULL,&merger_worker,(void *) &par);
   if (ret != 0) { printf("merger_thread create failed \n");exit(1);}
   ret = pthread_join(merger_tid, NULL);

   clock_t end = clock();
   float seconds = (float)(end - start) / CLOCKS_PER_SEC;
   printf("\033[32;1m It takes: %f seconds\033[0m\n",seconds);

  return 0;
}
