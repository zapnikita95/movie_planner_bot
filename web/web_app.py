"""
Flask приложение для webhook
"""
from flask import Flask, request, jsonify
import logging
import telebot
import os
import sys
import time

logger = logging.getLogger(__name__)

app = Flask(__name__)

def create_web_app(bot_instance):
    """Создает Flask приложение с webhook обработчиками"""
    
    @app.route('/webhook', methods=['POST'])
    def webhook():
        logger.info("=" * 80)
        logger.info("[WEBHOOK] ===== ПОЛУЧЕН ЗАПРОС =====")
        if request.headers.get('content-type') == 'application/json':
            json_string = request.get_data().decode('utf-8')
            logger.info(f"[WEBHOOK] Размер JSON: {len(json_string)} байт")
            # Проверяем, есть ли web_app_data в сыром JSON
            if 'web_app_data' in json_string.lower():
                logger.info("🔍 [WEBHOOK] ⚠️⚠️⚠️ В JSON ЕСТЬ 'web_app_data'! ⚠️⚠️⚠️")
            # Логируем первые 1000 символов JSON для отладки
            logger.info(f"[WEBHOOK] JSON (первые 1000 символов): {json_string[:1000]}")
            update = telebot.types.Update.de_json(json_string)
            logger.info(f"[WEBHOOK] Тип update: {type(update)}")
            logger.info(f"[WEBHOOK] Update имеет message: {hasattr(update, 'message') and update.message is not None}")
            
            # Логируем информацию о реплае для отладки
            if update.message:
                logger.info(f"[WEBHOOK] Update.message.content_type={update.message.content_type if hasattr(update.message, 'content_type') else 'НЕТ'}")
                logger.info(f"[WEBHOOK] Update.message.text='{update.message.text[:200] if update.message.text else None}'")
                logger.info(f"[WEBHOOK] Update.message.from_user.id={update.message.from_user.id if update.message.from_user else None}")
            
            bot_instance.process_new_updates([update])
            return '', 200
        else:
            logger.warning("[WEBHOOK] Неверный content-type")
            abort(400)
    
    @app.route('/', methods=['GET'])
    def root():
        logger.info("[ROOT] Root запрос получен")
        return jsonify({'status': 'ok', 'service': 'moviebot'}), 200
    
    @app.route('/health', methods=['GET'])
    def health():
        logger.info("[HEALTH] Health check запрос получен")
        return jsonify({'status': 'ok', 'bot': 'running'}), 200
    
    return app


