import cx_Oracle
import json

def open_connection():
    connection = None
    try:
        json_file = open("./config/config.json")
        config = json.load(json_file)

        # Note: Enhance it to use secreats instead.
        connection = cx_Oracle.connect(
            config["username"],
            config["password"],
            config["dsn"],
            encoding=config["encoding"])

        return connection

    except cx_Oracle.Error as error:
        print(error)
    finally:
        if json_file:
            json_file.close()