### Crypto Calculator Service
Calculates current balance from transactions / exchange rates / symbol rates 



#### unit testing setup 
> import keyring
> keyring.set_password("CryptoCalculatorService","USERNAME","cryptoAdmin")
> keyring.set_password("CryptoCalculatorService","USERNAME","test")
> keyring.set_password("CryptoCalculatorService","test","test")

start kafka     
> cd <kafkadir>/bin 
> ./zookeeper-server-start.sh ../config/server.properties 
> ./kafka-server-start.sh ../config/server.properties 

start mongo 
> sudo service mongod start 
