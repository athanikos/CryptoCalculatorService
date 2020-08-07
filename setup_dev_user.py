"""
Used by config.yml to setup the username and pass for the mongo user during unit testing
"""
from keyring import set_password
set_password("CryptoCalculatorService", "USERNAME", "admin")
set_password("CryptoCalculatorService", "admin", "admin")
