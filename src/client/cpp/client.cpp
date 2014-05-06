#include <unistd.h>
#include <cassert>
#include <boost/utility/binary.hpp>
#include <boost/shared_ptr.hpp>

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

using namespace std;
using namespace boost;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

using namespace barista;

int main () {
  try {
    boost::shared_ptr<TSocket> transport(new TSocket("128.52.161.243", 9000));
    shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
    BaristaClient client(protocol);

    transport->open();
    double version = client.get_version();
    cout << version << endl;

    ConnectionParams con_params = ConnectionParams();
    con_params.__set_user("postgres");
    con_params.__set_password("postgres");
    con_params.__set_database("postgres");

    Connection con = Connection();
    client.open_connection(con, con_params);
    ResultSet res = ResultSet();
    client.execute_sql(res, con, "SELECT 6.824 as id, 'Distributed Systems' as name", vector<string>());
    for(vector<Tuple>::iterator tuple_it = res.tuples.begin(); tuple_it != res.tuples.end(); ++tuple_it) {
      for(vector<string>::iterator cell_it = (*tuple_it).cells.begin(); cell_it != (*tuple_it).cells.end(); ++cell_it) {
        cout << *cell_it << "\t";
      }
      cout << "\n";
    }
    client.close_connection(con);
    transport->close();
  } catch (TException &tx) {
    cout << "ERROR: " << tx.what();
  }

  return 0;
}
