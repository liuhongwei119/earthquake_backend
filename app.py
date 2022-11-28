from flask import Flask
from exts import db
from flask_migrate import Migrate
import config
from blueprints.offline_mysql_curve import bp as offline_mysql_curve_bp

app = Flask(__name__)
# 绑定配置文件
app.config.from_object(config)
# flask db init：只需要执行一次
# flask db migrate：将orm模型生成迁移脚本
# flask db upgrade：将迁移脚本映射到数据库中
db.init_app(app)
migrate = Migrate(app, db)

app.register_blueprint(offline_mysql_curve_bp)


@app.route('/')
def hello_world():  # put application's code here
    return 'Hello World!'


if __name__ == '__main__':
    app.run()
