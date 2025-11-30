#ifndef __KERNEL_THREADS_H
#define __KERNEL_THREADS_H

#include "util.h"
#include "tinyos.h"
#include "kernel_proc.h"

void start_threads();

PTCB*aquire();

PTCB*intialize_PTCB(PTCB*ptcb,Task task,int argl,void* args);


#endif
 