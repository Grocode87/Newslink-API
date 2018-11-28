from flask import Flask
import MySQLdb

app = Flask(__name__)
app.config.from_object('config')

conn_conf = app.config['DATABASE_CONN_INFO']
db_connection = MySQLdb.connect(host=conn_conf["host"],
                                db=conn_conf["db"],
                                user=conn_conf["user"],
                                password=conn_conf["pass"],
                                charset=conn_conf['charset'])

from app import routes

if __name__ == "__main__":
    app.run(debug=app.config['DEBUG_MODE'])