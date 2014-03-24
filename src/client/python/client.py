#!/usr/bin/python

from barista import Barista
from barista.constants import *
from thrift import Thrift
from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket
from thrift.transport import TTransport

'''
@author: anant bhardwaj
@date: Mar 24, 2014

Sample Python client for Barista
'''

try:
  transport = TSocket.TSocket('localhost', 9000)
  transport = TTransport.TBufferedTransport(transport)
  protocol = TBinaryProtocol.TBinaryProtocol(transport)
  client = Barista.Client(protocol)

  transport.open()

  con_params = ConnectionParams(
  	user="postgres", password="postgres", database="postgres")

  con = client.connect(con_params)
  res = client.execute_sql(con=con, query="SELECT 6824", query_params=None)
  
  for t in res.tuples:
  	for cell in t.cells:
  		print cell.value

  transport.close()
except Exception, e:
    print 'Something went wrong : %s' % (e)
