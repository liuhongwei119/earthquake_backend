from flask import Flask, render_template
from flask_socketio import SocketIO
from flask_socketio import emit

app = Flask(__name__)

socketio = SocketIO(app)
socketio.init_app(app, cors_allowed_origins='*')


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/test_send')
def test_send():
    print("test_send")
    for i in range(1,9):
        socketio.emit("test_send", "test_send", namespace="/wechat")
    return "test_send"


@socketio.on('message', namespace="/wechat")
def handle_message(message):
    print('received message: ' + message['data'])
    socketio.emit("response", {'age': 18}, namespace="/wechat")


@socketio.on('connect', namespace="/wechat")
def connect():
    print("connect..")


@socketio.on('disconnect', namespace="/wechat")
def connect():
    print("disconnect...")


if __name__ == '__main__':
    socketio.run(app, port=5000, debug=True)
