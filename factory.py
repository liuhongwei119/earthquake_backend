from flask import Flask
import settings
from exts import db, logger
from blueprints.curve_blueprint import bp as curve_bp
from blueprints.analysis_blueprint import bp as analysis_bp
from blueprints.websocket_blueprint import bp as websocket_bp


# factory 工厂（factory）
# 是指创建其他对象的对象，通常是一个返回其他类的对象的函数或方法。
# 程序实例在工厂函数中创建，这个函数返回程序实例app。按照惯例，这个函数被命名为create_app（）或make_app（）。
# 我们把这个工厂函数称为程序工厂（Application Factory）——即“生产程序的工厂”，使用它可以在任何地方创建程序实例。


def create_app():
    config_class = settings.DevelopmentConfig
    app = Flask(config_class.PROJECT_NAME)
    app.config['SECRET_KEY'] = 'secret_key'
    # =========================加载配置========================
    app.config.from_object(config_class)
    # =========================注册蓝本========================
    app.register_blueprint(curve_bp)
    app.register_blueprint(analysis_bp)
    app.register_blueprint(websocket_bp)
    # =========================初始化app扩展，丰富app功能=======================
    # 1.初始化SQLAlchemy
    db.init_app(app=app)
    # 2.初始化Logger
    logger.init_app(app=app)

    return app
