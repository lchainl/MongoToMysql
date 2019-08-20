create_database_sql = "CREATE DATABASE  %s DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;"
create_table_sql="CREATE TABLE %s(" \
                 "%s) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;"
insert_one_sql="INSERT INTO %s(%s) VALUES(%s);"
# select_one_sql="SELECT * FROM %s WHERE id=%s;"
use_database='USE %s;'