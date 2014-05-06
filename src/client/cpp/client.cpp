#include <unistd.h>
#include <cassert>
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

    transport->close();
  } catch (TException &tx) {
    cout << "ERROR: " << tx.what();
  }

  return 0;
}
