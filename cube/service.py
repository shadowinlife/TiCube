from config import dbconfig
import mysql.connector
from mysql.connector import errorcode
from utils.logger import logger as LOG


def suggest(table_name):
    try:
        cnx = mysql.connector.connect(pool_name = "mypool",
                              pool_size = 3,
                              **dbconfig)

        cursor = cnx.cursor()
        result_set = cursor.execute("desc %s", table_name)
        print(result_set)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            LOG.error("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            LOG.error("Database does not exist")
        else:
            print(err)
    else:
        cnx.close()


