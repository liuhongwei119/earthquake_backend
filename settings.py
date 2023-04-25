from urllib.parse import quote_plus as urlquote


class BaseConfig(object):
    # 基础配置
    PROJECT_NAME = "EarthquakeBackend"


class DevelopmentConfig(BaseConfig):
    # 开发环境配置
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


class TestConfig(BaseConfig):
    # 测试环境配置
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


class ProductionConfig(BaseConfig):
    # 生产环境(上线环境)配置
    # MySQL所在的主机名
    HOSTNAME = "127.0.0.1"
    # MySQL监听的端口号，默认3306
    PORT = 3306
    # 连接MySQL的用户名，读者用自己设置的
    USERNAME = "root"
    # 连接MySQL的密码，读者用自己的
    PASSWORD = "12345678"
    # MySQL上创建的数据库名称
    DATABASE = "flask_earthquake"
    DB_URI = f"mysql+pymysql://{USERNAME}:{urlquote(PASSWORD)}@{HOSTNAME}:{PORT}/{DATABASE}?charset=utf8"
    SQLALCHEMY_DATABASE_URI = DB_URI
