#include "client.h"

#include <unistd.h>
#include <cassert>
#include <boost/utility/binary.hpp>
#include <boost/shared_ptr.hpp>
#include <random>
#include <climits>
#include <ctime>
#include <iostream>
#include <sstream>
#include <mutex>

#include "gen-cpp/Barista.h"
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/protocol/TBinaryProtocol.h>

/**
 * Sample Barista C++ Client
 *
 * @author anantb
 * @date 11/07/2013
 *
 */

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

using namespace barista;

const int PORT = 9000;
const std::string ADDRS[] = {"128.52.161.243", "128.52.160.104", "128.52.161.242", "128.52.160.122", "128.52.161.24"};

class Clerk {
public:
  // constructor
  Clerk(const std::string& user = "postgres", const std::string& password = "postgres", const std::string& database = "postgres") {

    const long long max_value = LLONG_MAX;

    // random number generator seeded with time
    // http://en.cppreference.com/w/cpp/numeric/random/mersenne_twister_engine
    std::mt19937 rng( std::time(0) ) ;

    // pseudo random integers uniformly distributed in [ -max_value, max_value ]
    // http://en.cppreference.com/w/cpp/numeric/random/uniform_int_distribution
    std::uniform_int_distribution<long long> distr( -max_value, max_value ) ;

    const long long me = distr(rng);
    d_curRequest = 0;
    d_user = user;
    d_pword = password;
    d_database = database;

    std::ostringstream clientStream;
    clientStream << me;
    d_clientId = clientStream.str();

    d_con_params = ConnectionParams();
    d_con_params.__set_user(d_user);
    d_con_params.__set_password(d_pword);
    d_con_params.__set_database(d_database);
    d_con_params.__set_client_id(d_clientId);

    d_addrs.assign (ADDRS, ADDRS + 5);
  }

  void setUser(const std::string& user) {
    d_user = user;
    d_con_params.__set_user(d_user);
  }

  void setPassword(const std::string& password) {
    d_pword = password;
    d_con_params.__set_password(d_pword);
  }

  void setDatabase(const std::string& database) {
    d_database = database;
    d_con_params.__set_database(d_database);
  }

  void setClientId(const std::string& clientId) {
    d_clientId = clientId;
    d_con_params.__set_client_id(d_clientId);
  }

  const std::string & getUser() {
    return d_user;
  }

  const std::string & getPassword() {
    return d_pword;
  }

  const std::string & getDatabase() {
    return d_database;
  }

  const std::string & getClientId() {
    return d_clientId;
  }

  // open database connection
  void openConnection() {
    while(true) {
      for(std::vector<std::string>::const_iterator it = d_addrs.begin(); it != d_addrs.end(); ++it) {
	try {
	  boost::shared_ptr<TSocket> transport(new TSocket(*it, PORT));
	  boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
	  BaristaClient client(protocol);

	  transport->open();
	  std::lock_guard<std::mutex> lock(d_mu);

	  d_curRequest++;

	  std::ostringstream seqStream;
	  seqStream << d_curRequest;
	  std::string seqId = seqStream.str();

	  d_con_params.__set_seq_id(seqId);

	  client.open_connection(d_connection, d_con_params);
	  transport->close();
	  return;
	}
	catch(TException &tx) {
	  std::cout << "ERROR: " << tx.what() << std::endl;
	}
      }
    }
  }

  // execute SQL
  void execSql(const std::string &query, const std::vector<std::string> &query_params, ResultSet &res) {
    while(true) {
      for(std::vector<std::string>::const_iterator it = d_addrs.begin(); it != d_addrs.end(); ++it) {
	try {
	  boost::shared_ptr<TSocket> transport(new TSocket(*it, PORT));
	  boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
	  BaristaClient client(protocol);

	  transport->open();
	  std::lock_guard<std::mutex> lock(d_mu);

	  d_curRequest++;

	  std::ostringstream seqStream;
	  seqStream << d_curRequest;
	  std::string seqId = seqStream.str();

	  d_connection.__set_seq_id(seqId);

	  client.execute_sql(res, d_connection, query, query_params);    
	  transport->close();
	  return;
	}
	catch(TException &tx) {
	  std::cout << "ERROR: " << tx.what() << std::endl;
	}
      }
    }
  }

  // execute SQL as part of a transaction
  void execSqlTxn(const std::string &query, const std::vector<std::string> &query_params, ResultSet &res) {
    while(true) {
      for(std::vector<std::string>::const_iterator it = d_addrs.begin(); it != d_addrs.end(); ++it) {
	try {
	  boost::shared_ptr<TSocket> transport(new TSocket(*it, PORT));
	  boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
	  BaristaClient client(protocol);

	  transport->open();
	  std::lock_guard<std::mutex> lock(d_mu);

	  d_curRequest++;

	  std::ostringstream seqStream;
	  seqStream << d_curRequest;
	  std::string seqId = seqStream.str();

	  d_connection.__set_seq_id(seqId);

	  client.execute_sql_txn(res, d_connection, query, query_params);    
	  transport->close();
	  return;
	}
	catch(TException &tx) {
	  std::cout << "ERROR: " << tx.what() << std::endl;
	}
      }
    }
  }

  // begin a transaction
  void beginTxn() {
    while(true) {
      for(std::vector<std::string>::const_iterator it = d_addrs.begin(); it != d_addrs.end(); ++it) {
	try {
	  boost::shared_ptr<TSocket> transport(new TSocket(*it, PORT));
	  boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
	  BaristaClient client(protocol);

	  transport->open();
	  std::lock_guard<std::mutex> lock(d_mu);

	  d_curRequest++;

	  std::ostringstream seqStream;
	  seqStream << d_curRequest;
	  std::string seqId = seqStream.str();

	  d_connection.__set_seq_id(seqId);

	  client.begin_txn(d_connection);
	  transport->close();
	  return;
	}
	catch(TException &tx) {
	  std::cout << "ERROR: " << tx.what() << std::endl;
	}
      }
    }
  }

  // commit a transaction
  void commitTxn() {
    while(true) {
      for(std::vector<std::string>::const_iterator it = d_addrs.begin(); it != d_addrs.end(); ++it) {
	try {
	  boost::shared_ptr<TSocket> transport(new TSocket(*it, PORT));
	  boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
	  BaristaClient client(protocol);

	  transport->open();
	  std::lock_guard<std::mutex> lock(d_mu);

	  d_curRequest++;

	  std::ostringstream seqStream;
	  seqStream << d_curRequest;
	  std::string seqId = seqStream.str();

	  d_connection.__set_seq_id(seqId);

	  client.commit_txn(d_connection);
	  transport->close();
	  return;
	}
	catch(TException &tx) {
	  std::cout << "ERROR: " << tx.what() << std::endl;
	}
      }
    }
  }

  // rollback a transaction
  void rollbackTxn() {
    while(true) {
      for(std::vector<std::string>::const_iterator it = d_addrs.begin(); it != d_addrs.end(); ++it) {
	try {
	  boost::shared_ptr<TSocket> transport(new TSocket(*it, PORT));
	  boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
	  BaristaClient client(protocol);

	  transport->open();
	  std::lock_guard<std::mutex> lock(d_mu);

	  d_curRequest++;

	  std::ostringstream seqStream;
	  seqStream << d_curRequest;
	  std::string seqId = seqStream.str();

	  d_connection.__set_seq_id(seqId);

	  client.rollback_txn(d_connection);
	  transport->close();
	  return;
	}
	catch(TException &tx) {
	  std::cout << "ERROR: " << tx.what() << std::endl;
	}
      }
    }
  }

  // close database connection
  void closeConnection() {
    while(true) {
      for(std::vector<std::string>::const_iterator it = d_addrs.begin(); it != d_addrs.end(); ++it) {
	try {
	  boost::shared_ptr<TSocket> transport(new TSocket(*it, PORT));
	  boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
	  BaristaClient client(protocol);

	  transport->open();
	  std::lock_guard<std::mutex> lock(d_mu);

	  d_curRequest++;

	  std::ostringstream seqStream;
	  seqStream << d_curRequest;
	  std::string seqId = seqStream.str();

	  d_connection.__set_seq_id(seqId);

	  client.close_connection(d_connection);
	  transport->close();
	  return;
	}
	catch(TException &tx) {
	  std::cout << "ERROR: " << tx.what() << std::endl;
	}
      }
    }
  }

  void printResultSet(const ResultSet &res) {
    for(std::vector<Tuple>::const_iterator tuple_it = res.tuples.begin(); tuple_it != res.tuples.end(); ++tuple_it) {
      for(std::vector<std::string>::const_iterator cell_it = (*tuple_it).cells.begin(); cell_it != (*tuple_it).cells.end(); 
	  ++cell_it) {
	std::cout << *cell_it << "\t";
      }
      std::cout << std::endl;
    }
  }

private:
  int d_curRequest;

  std::string d_user;
  std::string d_pword;
  std::string d_database;
  std::string d_clientId;

  std::vector<std::string> d_addrs;
  Connection d_connection;
  ConnectionParams d_con_params;
  std::mutex d_mu;
};

// List of machines running on the server forming a paxos group
// 128.52.161.243:9000
// 128.52.160.104:9000
// 128.52.161.242:9000
// 128.52.160.122:9000
// 128.52.161.24:9000

int main () {
  Clerk clerk;
  clerk.openConnection();

  ResultSet res;
  clerk.execSql("SELECT 6.824 as id, 'Distributed Systems' as name", std::vector<std::string>(), res);
  clerk.printResultSet(res);

  clerk.closeConnection();
  return 0;
}


// functions to be called from C
struct result {
  ResultSet r;
};

result_t* new_result() {
  result_t* res = (result_t*) malloc(sizeof(*res));
  return res;
}

void clear_result(result_t* result) {
  free(result);
  result = NULL;
}

int num_tuples(result_t* result) {
  return (result->r).row_count;
}

int num_fields(result_t* result) {
  return (result->r).field_names.size();
}


const char* get_value(result_t* result, int row, int column) {
  return (result->r).tuples[row].cells[column].c_str();
}

const char* get_field_name(result_t* result, int column) {
  return (result->r).field_names[column].c_str();
}

struct clerk {
  Clerk* c;
};

clerk_t* new_clerk(char* user, char* password, char* database) {
  clerk_t* clerk = (clerk_t*) malloc(sizeof(*clerk));
  clerk->c = new Clerk(user, password, database);
  return clerk;
}

void clear_clerk(clerk_t* clerk) {
  delete clerk->c;
  free(clerk);
  clerk = NULL;
}

void open_connection(clerk_t* clerk) {
  (clerk->c)->openConnection();
}

result_t* execute_sql(clerk_t* clerk, char* query, char** query_params, int nparams) {
  result_t* result = new_result();
  std::vector<std::string> params;
  if(query_params != NULL) {
    params.assign(query_params, query_params + nparams);
  }
  (clerk->c)->execSqlTxn(query, params, result->r);

  return result;
}

void close_connection(clerk_t* clerk) {
  (clerk->c)->closeConnection();
}

void begin_txn(clerk_t* clerk) {
  (clerk->c)->beginTxn();
}

void commit_txn(clerk_t* clerk) {
  (clerk->c)->commitTxn();
}

void rollback_txn(clerk_t* clerk) {
  (clerk->c)->rollbackTxn();
}
