#ifndef RESULT_H
#define RESULT_H

#ifdef __cplusplus
extern "C" {
#endif

  typedef struct result result_t;

  result_t* new_result();

  void clear_result(result_t* result);

  int nTuples(result_t* result);

  int nFields(result_t* result);

  char* getValue(result_t* result, int row, int column);

  char* getFieldName(result_t* result, int column);

#ifdef __cplusplus
}
#endif

#endif
