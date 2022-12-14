from flask import Flask
from exts import db
from flask_migrate import Migrate
import config
from blueprints.curve_blueprint import bp as curve_bp
from gevent import pywsgi
from flask_cors import CORS

app = Flask(__name__)
# 绑定配置文件
app.config.from_object(config)
# flask db init：只需要执行一次
# flask db migrate：将orm模型生成迁移脚本
# flask db upgrade：将迁移脚本映射到数据库中
db.init_app(app)
migrate = Migrate(app, db)

app.register_blueprint(curve_bp)


@app.route('/')
def hello_world():  # put application's code here
    return 'Hello World!'


@app.after_request
def cross_region(rst):
    rst.headers['Access-Control-Allow-Origin'] = '*'
    rst.headers['Access-Control-Allow-Method'] = 'GET,POST'  # 如果该请求是get，把POST换成GET即可
    rst.headers['Access-Control-Allow-Headers'] = 'x-requested-with,content-type'
    return rst


if __name__ == '__main__':
    app.run(debug=True)
    # 通过CORS，所有的来源都允许跨域访问
    # CORS(app, resources=r'/*')
    # server = pywsgi.WSGIServer(('0.0.0.0', 1123), app)
    # server.serve_forever()
