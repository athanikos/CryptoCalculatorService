### Crypto Calculator Service
Calculates current balance from transactions / exchange rates / symbol rates 



#### Environment Setup 
Refer to CryptoStore repository for mongo & streamsets installation & configuration     
Use keyring to set  username and password for mongo db  
``` 
python  
import keyring 
>>> keyring.set_password("CryptoUsersService", "USERNAME", "CryptoUsersService")
>>> keyring.set_password("CryptoUsersService", "CryptoUsersService", "")


### capablities 
Transaction Managenent 
    User : Transactions/Notifications/Settings 



Compute balance     
    uses transactions, symbol_rates 
    coverts currency 
    uses exchange_rates 

insert/update/transactions 
uses transactions 



notifies some message 
    uses user_notifications
