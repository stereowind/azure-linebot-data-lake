import logging
import os
import json
from datetime import datetime
import pytz
import azure.functions as func
from azure.storage.blob import BlockBlobService

from linebot import (
    LineBotApi, WebhookHandler
)
from linebot.exceptions import (
    LineBotApiError, InvalidSignatureError
)
from linebot.models import (
    MessageEvent, TextMessage, TextSendMessage,
)

channel_secret = os.getenv('LINE_CHANNEL_SECRET')
channel_access_token = os.getenv('LINE_CHANNEL_ACCESS_TOKEN')

line_bot_api = LineBotApi(channel_access_token)
handler = WebhookHandler(channel_secret)

@handler.add(MessageEvent, message=TextMessage)
def reply_to_message(event):
    """Reply back with received message"""
    user = event.source.user_id
    text = event.message.text
    logging.info(f"User {user} wrote: {text}")
    line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text=f"Message processed:\n{text}")
            )


def save_json_to_ADLS(event):
    """Save message data to ADLS storage in JSON format"""
    system_name = "linebot"
    user_id = event['source']['userId']
    timestamp = event['timestamp']
    date = datetime.now(tz=pytz.timezone('Japan'))
    blob_name = f"{system_name}/date={date.year}-{date.month:02d}-{date.day:02d}/{date.hour:02d}/{date.minute:02d}/{date.hour:02d}-{date.minute:02d}-{date.second:02d}_{timestamp}_{user_id}.json"
    event_content = json.dumps(event)
    
    logging.info(f"Blob name: {blob_name}")
    logging.info(f"Event body:\n{event_content}")

    try:
        blob_client = BlockBlobService(account_name=os.getenv("STORAGE_ACCOUNT_NAME"), account_key=os.getenv("STORAGE_ACCOUNT_KEY"))
        blob_client.create_blob_from_text(container_name="raw-zone", blob_name=blob_name, text=event_content)
        return "OK"
    except:
        return "ERROR"


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Processing request from AzureDataLakeBot bot...')
    
    signature = req.headers['X-Line-Signature']
    req_body = req.get_body().decode('utf-8')
    req_dict = req.get_json()
    logging.info("Request body:")
    logging.info(req_body)

    # Put events to ADLS in JSON format
    result = ""
    try:
        events = req_dict['events']
        for event in events:
            if event['type'] == 'message' and event['message']['type'] == 'text':
                    result = save_json_to_ADLS(event)
    except Exception as e:
        logging.error("Error retrieving events!")
        logging.error(e)
        result = "ERROR"

    # Reply back to user
    if result == "OK":
        try:
            handler.handle(req_body, signature)
        except LineBotApiError as e:
            logging.error(f"Got exception from LINE Messaging API: {e.message}\n")
            for m in e.error.details:
                logging.error(f"  {m.property}: {m.message}")
        except InvalidSignatureError:
            logging.error("Invalid signature.")
            return func.HttpResponse("Invalid signature.", status_code=400)
        return func.HttpResponse("OK")
    else:
        return func.HttpResponse("ERROR")
