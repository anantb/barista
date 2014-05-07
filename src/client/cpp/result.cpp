#include "result.h"

#include "gen_cpp/Barista.h"

struct result {
  barista::ResultSet r;
};

result_t* new_result() {
  result_t* res = malloc(sizeof(*res));
  return res;
}

void clear_result(result_t* result) {
  free(result);
  result = NULL;
}

int nTuples(result_t* result) {
  return (result->r).row_count;
}

int nFields(result_t* result) {
  return (result->r).field_names.size();
}


char* getValue(result_t* result, int row, int column) {
  return (result->r).tuples[row].cells[column].c_str();
}

char* getFieldName(result_t* result, int column) {
  return (result->r).field_names[column].c_str();
}
