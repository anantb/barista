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
  Clerk() {

    const long long max_value = LLONG_MAX;

    // random number generator seeded with time
    // http://en.cppreference.com/w/cpp/numeric/random/mersenne_twister_engine
    std::mt19937 rng( std::time(0) ) ;

    // pseudo random integers uniformly distributed in [ -max_value, max_value ]
    // http://en.cppreference.com/w/cpp/numeric/random/uniform_int_distribution
    std::uniform_int_distribution<long long> distr( -max_value, max_value ) ;

    d_me = distr(rng);
    d_curRequest = 0;
    d_user = "postgres";
    d_pword = "postgres";
    d_database = "postgres";

    std::ostringstream clientStream;
    clientStream << d_me;
    d_clientId = clientStream.str();

    d_con_params = ConnectionParams();
    d_con_params.__set_user(d_user);
    d_con_params.__set_password(d_pword);
    d_con_params.__set_database(d_database);
    d_con_params.__set_client_id(d_clientId);
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
  void openConnection(const std::vector<std::string> &addrs, Connection &con) {
    while(true) {
      for(std::vector<std::string>::const_iterator it = addrs.begin(); it != addrs.end(); ++it) {
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

	  client.open_connection(con, d_con_params);
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
  void execSql(const std::vector<std::string> &addrs, Connection &con, 
	       const std::string &query, const std::vector<std::string> &query_params,
	       ResultSet &res) {
    while(true) {
      for(std::vector<std::string>::const_iterator it = addrs.begin(); it != addrs.end(); ++it) {
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

	  con.__set_seq_id(seqId);

	  client.execute_sql(res, con, query, query_params);    
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
  void closeConnection(const std::vector<std::string> &addrs, Connection &con) {
    while(true) {
      for(std::vector<std::string>::const_iterator it = addrs.begin(); it != addrs.end(); ++it) {
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

	  con.__set_seq_id(seqId);

	  client.close_connection(con);
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
  long long d_me; // passed as clientId
  int d_curRequest;

  std::string d_user;
  std::string d_pword;
  std::string d_database;
  std::string d_clientId;

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

  std::vector<std::string> addrs;
  addrs.assign (ADDRS, ADDRS + 5);

  Connection con;
  clerk.openConnection(addrs, con);

  ResultSet res;
  clerk.execSql(addrs, con, "SELECT 6.824 as id, 'Distributed Systems' as name", std::vector<std::string>(), res);
  clerk.printResultSet(res);

  clerk.closeConnection(addrs, con);
  return 0;
}
