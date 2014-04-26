import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import java.nio.ByteBuffer;
import barista.*;

/** 
 * Sample Barista Java Client
 * 
 * @author anantb
 * @date 03/24/2014
 * 
 */

public class Client {
  public static void main(String [] args) {
    try {
      TTransport transport = new TSocket("localhost", 9000);
      TProtocol protocol = new  TBinaryProtocol(transport);
      Barista.Client client = new Barista.Client(protocol);

      transport.open();
      
      ConnectionParams con_params = new ConnectionParams();
      con_params.setUser("postgres");
      con_params.setPassword("postgres");
      con_params.setDatabase("postgres");

      Connection con = client.open_connection(con_params);	     
      ResultSet res = client.execute_sql(
          con, "SELECT 6.824 as id, 'Distributed Systems' as name", null);

      for (String fielf_name : res.getField_names()) {
        System.out.print(fielf_name + "\t");
      }

      System.out.println();

      for (Tuple t : res.getTuples()) {
        for (ByteBuffer cell : t.getCells()) {
          System.out.print(new String(cell.array()) + "\t");
        }
        System.out.println();
      }

      transport.close();
    } catch(Exception e) {
      e.printStackTrace();
    } 
  }
}
