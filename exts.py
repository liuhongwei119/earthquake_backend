
"""
扩展app功能
为了完成扩展的初始化操作，我们需要在实例化扩展类时传入程序实例。
但使用工厂函数时，并没有一个创建好的程序实例可以导入。
如果我们把实例化操作放到工厂函数中，那么我们就没有一个全局的扩展对象可以使用，比如表示数据库的db对象。
为了解决这个问题，大部分扩展都提供了一个init_app（）方法来支持分离扩展的实例化和初始化操作。
现在我们仍然像往常一样初始化扩展类，但是并不传入程序实例.这时扩展类实例化的工作可以抽离出来
"""

from flask_sqlalchemy import SQLAlchemy
from log_handler import Logger

db = SQLAlchemy()
logger = Logger()

