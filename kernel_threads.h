#ifndef __KERNEL_THREADS_H
#define __KERNEL_THREADS_H

#include "util.h"
#include "tinyos.h"
#include "kernel_proc.h"
#include "kernel_sched"

typedef struct process_thread_control_block {
  TCB* tcb;     //to TCB tou thread 
  Task task;    //i sinartisi pou ektelei 
  int argl;
  void* args;
 
  int exitval;
  int exited;
  int detached;
  CondVar exit_cv;
  int refcount;
 
  rlnode ptcb_list_node;
} PTCB;
void start_threads();

PTCB*aquire();

PTCB*intialize_PTCB(PTCB*ptcb,Task task,int argl,void* args);


#endif
 
