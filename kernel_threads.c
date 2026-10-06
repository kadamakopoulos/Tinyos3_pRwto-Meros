
#include "tinyos.h"
#include "kernel_sched.h"
#include "kernel_proc.h"
#include "kernel_threads.h"
#inlcude "kernel_streams.h"
#include "kernel_cc.h"


PTCB*aquire(){
  PTCB*ptcb=(PTCB*)xmalloc(sizeof(PTCB));//desmeuei xwro gia ena ptcb me xmalloc
  assert(ptcb!=NULL);//an den uparxei mnimi tote panic
  return ptcb;//epistrefoume to neo ptcb
}


PTCB*intialize_PTCB(PTCB*ptcb,Task task,int argl,void* args){
  //apothikeusi basikwn stoixeiwn tis diergasias
  ptcb->task=task;
  ptcb->argl=argl;
  ptcb->args=args;

  ptcb->detached=0;//den einai detached
  ptcb->exited=0;//den exei termatisei akoma
  ptcb->exit_cv=COND_INIT;//arxikopoiisi condition variable gia join
  ptcb->refcount=0;  //kanena den exei kanei join


  return ptcb;
}


void start_ptcb_thread(){
 int exitval;//timi epistrofis tis task

 PTCB* ptcb = cur_thread()->ptcb;//pairnei to PTCB tou trexontos thread

 //antigrafei ta stoixeia apo to PTCB
 Task call = ptcb->task;
 int argl = ptcb->argl;
 void* args = ptcb->args;

 exitval = call(argl,args);//ektelei tin pragmatiki sinartisi
 ThreadExit(exitval);//termatizei to thread me tin timi epistrofis

}

/** 
  @brief Create a new thread in the current process.
  */
Tid_t sys_CreateThread(Task task, int argl, void* args)
{
  if (task != NULL)//Elegxei an task != NULL
  {
    PTCB* ptcb = intialize_PTCB(aquire(),task,argl,args);//pairnei elefthero PTCB apo pool (acquire() = malloc-like)
    rlnode_init(&ptcb->ptcb_list_node,ptcb);  //arxikopoiei ton komvo gia tin lista ptcb_list
    rlist_push_back(&CURPROC->ptcb_list, &ptcb->ptcb_list_node); //prosthetoume to ptcb sti lista twn threads tis diergasias
    CURPROC->thread_count++;              //auxanetai afou pige stin lista

    ptcb->tcb=spawn_thread(CURPROC,start_ptcb_threads);//dimiourgei neo tcb pou tha ektelei start_sub_threads
    ptcb->tcb->ptcb=ptcb;//amfidromi sundesi tcb se ptcb

    wakeup(ptcb->tcb);

  return (Tid_t)ptcb;//epistrefi ptcb
  }
  else
  {
   return NOTHREAD;//apotuxia
  }
}

/**
  @brief Return the Tid of the current thread.
 */
Tid_t sys_ThreadSelf()
{
  return (Tid_t) cur_thread()->ptcb;//to tcb exei deikti sto ptcb
}

/**
  @brief Join the given thread.
  */
int sys_ThreadJoin(Tid_t tid, int* exitval)
{

  rlnode* node= rlist_find(&CURPROC->ptcb_list, (PTCB*)tid, NULL);//briskei to ptcb sti lista tis diergasias

  if (node==NULL)
   return -1;


  PTCB* ptcb=node->ptcb;
   //an akuro ptcb tote einai idi detached kai den mporei na kanei join ton eauto tou
   if (ptcb == NULL || ptcb->detached == 1 || tid == sys_ThreadSelf())
  return -1;

  //auxanetai to refcount otan kapoios kanei join
  ptcb->refcount++;
  //perimenei mexri to thread na termatisei i na ginei detach
  while(ptcb->exited != 1 && ptcb->detached != 1){
    kernel_wait(&ptcb->exit_cv,SCHED_USER);//koimatai me condition variable
  }

  //meionoume refcount meta tin anamoni
  ptcb->refcount--;
  
  //an to thread egine detach enw perimenoume tote apotyxia
  if(ptcb->detached == 1){
    return -1;
  }
  //epistrefei tin timi exodou an zitithike
  if (exitval != NULL)
    *exitval=ptcb->exitval;
  //an kanenas den kanei join pia tote apeleytherosi mnimis
  if (ptcb->refcount == 0)
  {
    rlist_remove(&ptcb->ptcb_list_node);//aferi apo lista
    free(ptcb);//eleftherwsi mnimis
  }



  return 0;
}

/**
  @brief Detach the given thread.
  */
int sys_ThreadDetach(Tid_t tid)
{
  //briskei to ptcb 
  rlnode* node = rlist_find(&CURPROC->ptcb_list,(PTCB*)tid,NULL);

  //elegxei an den uparxei i exei termatistei
  if (node == NULL || node->ptcb->exited == 1)
   return -1;

  node->ptcb->detached = 1;//simatodotei detached
  kernel_broadcast(&node->ptcb->exit_cv);//xupnaei osous perimenoun sto exit_cv (p.x. join)

  return 0;
}

/**
  @brief Terminate the current thread.
  */
void sys_ThreadExit(int exitval)
{
  PCB *curproc = CURPROC;               //trexousa diergasia
  PTCB* ptcb = (PTCB*) sys_ThreadSelf();//diko mas ptcb

  ptcb->exitval = exitval;  //apothikeusi timis exodou
  ptcb->exited = 1;         //simatodotisi termatismou
  curproc->thread_count--;  //meiosi metriti threads

  kernel_broadcast(&ptcb->exit_cv);//xupname osous kanoun join

  if (curproc->thread_count==0)
  {
    if (get_pid(curproc) != 1)
    {
      /*Reparent any children of the eixiting process to the
        initial task*/
      PCB* initpcb = get_pcb(1);
      while(!is_rlist_empty(& curproc ->children_list)){
        rlnode* child = rlist_pop_front(& curproc->children_list);
        child->pcb->parent = initpcb;
        rlist_push_front(& initpcb->children_list, child);
      }


      /*Add exited children to the initial task's exited list
        and signal the initial task*/
      if (!is_rlist_empty(&curproc->exited_list))
      {
        rlist_append(& initpcb->exited_list, &curproc->exited_list);
        kernel_broadcast(&initpcb->child_exit);
      }

      /*Put me into my parent's exited list*/
      rlist_push_front(&curproc->parent->exited_list, &curproc->exited_node);
      kernel_broadcast(&curproc->parent->child_exit);
    }
    assert(is_rlist_empty(&curproc->children_list));
    assert(is_rlist_empty(&curproc->exited_list));


    /*
      Do all the other cleanup we want here, close files etc.
    */

    /*Release the args data*/
    if (curproc->args)
    {
      free(curproc->args);
      curproc->args = NULL;
    }

    /*Clean up FIDT*/
    for (int i = 0; i < MAX_FILEID; ++i)
    {
      if (curproc->FIDT[i] != NULL)
      {
        FCB_decref(curproc->FIDT[i]);
        curproc->FIDT[i] = NULL;
      }
    }

    /*Disconnect my main thread*/
    curproc->main_thread = NULL;

    /*Now mark the process as exited*/
    curproc->pstate = ZOMBIE;
  }

  /*Bye bye cruel world*/
  kernel_sleep(EXITED, SCHED_USER);

}

