from factory import create_app
from flask_cors import CORS
from gevent import pywsgi

app = create_app()


@app.route('/')
def hello_world():
    app.logger.info(app.config)
    return 'Hello EarthquakeBackend!'


@app.after_request
def cross_region(response):
    """
    添加跨域功能
    :param response: 返回数据
    :return:
    """
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Method'] = 'GET,POST'  # 如果该请求是get，把POST换成GET即可
    response.headers['Access-Control-Allow-Headers'] = 'x-requested-with,content-type'
    return response


if __name__ == '__main__':
    CORS(app, supports_credentials=True)
    app.run(host='0.0.0.0', debug=True, port=5100)
