#ifndef CLIENT_H
#define CLIENT_H

#include "result.h"

#ifdef __cplusplus
extern "C" {
#endif

  typedef struct clerk clerk_t;

  clerk_t* new_clerk(char* user, char* password, char* database);

  void clear_clerk(clerk_t* clerk);

  void open_connection(clerk_t* clerk);

  result_t* execute_sql(clerk_t* clerk, char* query, char** query_params, int nparams);

  void close_connection(clerk_t* clerk);

  void begin_txn(clerk_t* clerk);

  void commit_txn(clerk_t* clerk);

  void rollback_txn(clerk_t* clerk);

#ifdef __cplusplus
}
#endif

#endif
