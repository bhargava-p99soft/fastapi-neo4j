from dotenv import load_dotenv

load_dotenv() 
from os import getenv


db_config = {
    "user": getenv("user"),
    "password": getenv("password"),
    "account": getenv("account")
}



# SNOWFLAKE_CONFIG = {
#     'user': 'your_user',
#     'password': 'your_password',
#     'account': 'your_account',  # For example, 'xy12345.snowflakecomputing.com'
#     'warehouse': 'your_warehouse'
# }

# # Create a connection pool
# pool = PooledDB(
#     creator=snowflake.connector,
#     user=SNOWFLAKE_CONFIG['user'],
#     password=SNOWFLAKE_CONFIG['password'],
#     account=SNOWFLAKE_CONFIG['account'],
#     warehouse=SNOWFLAKE_CONFIG['warehouse'],
#     mincached=2,   # Minimum number of connections to keep in the pool
#     maxcached=5,   # Maximum number of connections in the pool
#     maxconnections=10,  # Maximum number of connections to Snowflake at once
#     blocking=True,  # Whether to block when the pool is full
# )