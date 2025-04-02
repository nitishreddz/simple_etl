# #Killing webserver and scheduler of airflow
# pkill -f "airflow scheduler"
# pkill -f "airflow webserver"

# or press Control C

# Start the Scheduler:
# airflow scheduler

# Start the Webserver:
# airflow webserver -p 8080

# Use airflow scheduler & if you want the scheduler to run in the background.
# Use airflow scheduler if you want to monitor its logs directly in the terminal.

# Postgres
# start PostgreSQL:
# sudo service postgresql start


# Log into the PostgreSQL shell:
# sudo -u postgres psql



# CREATE DATABASE etl_db;
# CREATE USER etl_user WITH ENCRYPTED PASSWORD 'etl_password';
# GRANT ALL PRIVILEGES ON DATABASE etl_db TO etl_user;

# \q