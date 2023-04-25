from exts import db
from models import OnlineFlashInfo
from flask_socketio import Namespace
from flask import current_app

class OnlineFlashInfoDao(Namespace):
    flash_logger = current_app.logger

    """
    dump flash_info to mysql
    """
    def dump_to_mysql(self, flash_info):
        try:
            db.session.add(flash_info)
            db.session.commit()
        except Exception:
            self.flash_logger.error("存储flash_info错误")

    def on_connect(self):
        print("连接..")

    def on_disconnect(self):
        print("关闭连接")

    def on_message(self, flash_info):
        self.emit("response", flash_info)
