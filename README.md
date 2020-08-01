### Crypto Calculator Service
1. Calculates balance based on transactions and symbol prices per date (BalanceService)
2. Calculates symbol price per date  (PricesService)
3. Evaluates simple expressions (A> .10 * B ) where A,B are variables from 1., 2. (evaluator)
4. Receives notifications from users Service (scedhuler) 
5. Pushes calculated notifications  to Notification Service 

####
uses mongo for storage
uses kafka for messaging 

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



