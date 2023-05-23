from flask import Blueprint, jsonify, make_response, request, current_app, render_template
from flask_socketio import emit, send
from exts import socketio
import uuid
import json
import time
import os
from flask import current_app
bp = Blueprint("realtime_earthquake_info", __name__, url_prefix="/realtime_earthquake_info")


# 定义WebSocket路由和事件处理函数
@bp.route('/')
def chat():
    return render_template('chat.html')


@socketio.on('connect', namespace='/chat')
def connect():
    print('Socket connected: {}'.format(request.sid))


@socketio.on('disconnect', namespace='/chat')
def disconnect():
    print('Socket disconnected: {}'.format(request.sid))


@socketio.on('message', namespace='/chat')
def handle_message(msg):
    print('Received message: ' + msg)
    send(msg, broadcast=True)


@socketio.on('json', namespace='/chat')
def handle_json(json):
    print('Received JSON: ' + str(json))
    send(json, broadcast=True)


@socketio.on('my event', namespace='/chat')
def handle_my_custom_event(json):
    print('Received event: ' + str(json))
    send(json, broadcast=True)
