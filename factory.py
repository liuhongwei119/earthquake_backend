from flask import Flask
import settings
from exts import db, logger


# factory 工厂（factory）
# 是指创建其他对象的对象，通常是一个返回其他类的对象的函数或方法。
# 程序实例在工厂函数中创建，这个函数返回程序实例app。按照惯例，这个函数被命名为create_app（）或make_app（）。
# 我们把这个工厂函数称为程序工厂（Application Factory）——即“生产程序的工厂”，使用它可以在任何地方创建程序实例。


def create_app():
    config_class = settings.DevelopmentConfig
    app = Flask(config_class.PROJECT_NAME)
    # =========================加载配置========================
    app.config.from_object(config_class)
    # =========================注册蓝本========================

    # =========================初始化app扩展，丰富app功能=======================
    # 1.初始化SQLAlchemy
    db.init_app(app=app)
    # 2.初始化Logger
    logger.init_app(app=app)

    return app
