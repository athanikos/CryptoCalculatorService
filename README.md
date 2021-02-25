

[![CircleCI](https://circleci.com/gh/athanikos/CryptoCalculatorService.svg?style=shield&circle-token=a7ee6cc5bd4367ac7d9c05ad2a5427d8068705c5)](https://app.circleci.com/pipelines/github/athanikos/CryptoCalculatorService)


##### Crypto Calculator Service
BalanceService : Calculates balance / roi based on transactions and symbol prices per date  
Calculates symbol price per date  (PricesService)   
Receives notifications from users Service (scheduler)   
Pushes calculated notifications  to Notification Service    
Uses mongo for storage & kafka for messaging other services

##### deployment instructions 
refer to CryptoUsersService repository 

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


##### Design  
* All services use mongo db as local store.
* All services message other services via kafka. 
* An insert on some record will be produced to kafka if another service requires to read that data.

###### CDC
* Each service gets data through Kafka, APScheduler is used as a background thread to consume data.
* for every entity that needs to be sent to another service, there are two systemic columns:
    * Operation (ADDED, MODIFIED, REMOVED ) 
    * source_id keeps the id of the record in a separate column. Used when CDC is applied to link target with source db records.
    * for example:
        * A user notification is inserted in UserService local store will have id=A and source_id=None.
        * The Calculator Service will consume the record and insert to its local store a user_notification with id=B and source_id=A.
        * Upon calculation a computed_notification will be messaged via kafka and a record is  created in computed_notification central_store
        * with id = C and some computed_date   
        
        This record also holds the actual user_notification (with its source_id) stored in UserService so it is possible to link it with the user_notification using the
    * to find out:
        * if a user_notification has been calculated:
            * lookup computed_notification.user_notification.source_id (in calculator.service)
        * if a computed_notification has been sent:
            * lookup sent_notification.computed_notification.source_id (in notifications service)
                  
###### Failing cases 

* Data can be consumed but fail when saving to local store. This has the effect of loosing data (consuming without saving)
    * On exception when saving to local store data is reproduced.

###### Use Case : Balance calculation 

* UserService allows crud operations for user_notifications. The records are saved to UsersService's local mongo store 
  and also produce kafka records to be consumed by CalculatorService.  

* Consumption by CalculatorService is done via Scheduler.synchronize_transactions_and_user_notifications. 
  Data is then stored to its local mongo instance (user_notification).
  The service then calculates balance by fetching all user_notifications that are active and are not computed 
  and saves back to computed_notification.
  It also updates user_notification computed column to True.
      
* NotificationsService consumes computed_notifications from Kafka similarly to how CalculatorService consumes from UsersService.
  It keeps state via sent column in computed_notifications (on notifying a user it updates the column).
  
  
###### Deign Considerations 

*   Kafka consume & DB insert are not one atomic operation. A record can be consumed without being inserted in the local store.
    On DB insert/failed the system pushes the failed inserts to the source topic to trigger reprocessing.
    
    This can fail when the service fails in the middle (i.e consume and shut down without inserting to local store )
  


         
