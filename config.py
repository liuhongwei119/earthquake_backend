from urllib.parse import quote_plus as urlquote

# MySQL所在的主机名
HOSTNAME = "127.0.0.1"
# MySQL监听的端口号，默认3306
PORT = 3306
# 连接MySQL的用户名，读者用自己设置的
USERNAME = "root"
# 连接MySQL的密码，读者用自己的
PASSWORD = "123456"
# MySQL上创建的数据库名称
DATABASE = "flask_earthquake"
DB_URI = f"mysql+pymysql://{USERNAME}:{urlquote(PASSWORD)}@{HOSTNAME}:{PORT}/{DATABASE}?charset=utf8"
SQLALCHEMY_DATABASE_URI = DB_URI

