

[![CircleCI](https://circleci.com/gh/athanikos/CryptoCalculatorService.svg?style=shield&circle-token=a7ee6cc5bd4367ac7d9c05ad2a5427d8068705c5)](https://app.circleci.com/pipelines/github/athanikos/CryptoCalculatorService)


##### Crypto Calculator Service
BalanceService : Calculates balance / roi based on transactions and symbol prices per date  
Calculates symbol price per date  (PricesService)   
Receives notifications from users Service (scheduler)   
Pushes calculated notifications  to Notification Service    
Uses mongo for storage & kafka for messaging other services

###### unit testing setup 
Setup a login and password for authenticating to mongo db via keyring
this also runs in circle ci on setup (setup_dev_user.py)
> import keyring    
> keyring.set_password("CryptoCalculatorService","USERNAME","cryptoAdmin")
> keyring.set_password("CryptoCalculatorService","USERNAME","test")
> keyring.set_password("CryptoCalculatorService","test","test")


###### start kafka     
> cd <kafkadir>/bin 
> ./zookeeper-server-start.sh ../config/server.properties 
> ./kafka-server-start.sh ../config/server.properties 
###### start mongo 
> sudo service mongod start 


##### system design 

###### user notifications 
Starts Scheduler.synchronize_transactions_and_user_notifications to consume user_notifications from kafka  
schedule_user_notifications : iterates through user_notifications and adds jobs to scheduler via ScheduledJobCreator.add_job

