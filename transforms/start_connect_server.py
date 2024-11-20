from pysail.spark import SparkConnectServer


server = SparkConnectServer(port=53515)
server.start()
