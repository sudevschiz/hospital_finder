import telegram
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import NetworkError, Unauthorized
from google_sheet_to_json import fetch
import json
import os
from time import sleep
from util import build_menu, zones, pincodes
import pandas as pd
from datetime import datetime, timedelta

import logging

logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)

DATA_UPDATE_MIN = 1


def read_status_logs():
    """
    Check metadata file to check the freshness
    If the freshness is less than a minute, fetch the data again

    """
    try:
        with open("metadata.json", "r") as f:
            meta = json.load(f)
            last_updated_time = datetime.strptime(
                meta["last_updated_time"], "%Y-%m-%d %H:%M:%S"
            )
    except Exception as e:
        logging.error(e)
        logging.info("Will create a new metadata file")
        last_updated_time = datetime(1900, 1, 1)

    if last_updated_time < datetime.now() - timedelta(minutes=DATA_UPDATE_MIN):
        fetch_start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            fetch()
            logging.info("Data refreshed")
        except Exception as e:
            logging.error(e)
            return None
        with open("metadata.json", "w") as f:
            json.dump({"last_updated_time": fetch_start_time}, f, indent=4)

    try:
        with open("output.json", "r") as f:
            status = json.load(f)
            status = pd.DataFrame(status)
        return status
    except FileNotFoundError:
        logging.info("Output file does not exist and couldn't be fetched!")
        return None


def hosps_in_pincode(status, pincode):
    """
    Return the data of all hospitals in a pincode
    Also returns the count of hospitals
    """
    pincode = str(pincode)
    sel_status = status[status["pincode"] == pincode]

    try:
        hosp_count = sel_status.hospital.nunique()
    except KeyError:
        hosp_count = 0

    return sel_status, hosp_count


def hosps_in_zone(status, zone):
    """
    Return the data of all hospitals in a pincode
    Also returns the count of hospitals
    """
    sel_status = status[status["zone"] == zone]

    try:
        hosp_count = sel_status.hospital.nunique()
    except KeyError:
        hosp_count = 0
    return sel_status, hosp_count


def get_latest(s, n_latest=2):
    """
    For each hospital, get the `n_latest` status logs
    """
    s.sort_values("timestamp", ascending=False, inplace=True)
    result = (
        s[["timestamp", "general", "hdu", "icu", "icuwithventilator", "remarks"]]
        .head(n_latest)
        .to_dict("records")
    )

    return result


def prepare_message(logs):
    """
    Prepare the formatted message
    """
    message = ""
    for r in logs:
        status_msg = ""
        for l in r["logs"]:
            status_msg = (
                status_msg
                + f"Last updated: {l['timestamp']} \n"
                + f"General Beds: {l['general']} \n"
                + f"HDU: {l['hdu']} \n"
                + f"ICU: {l['icu']} \n"
                + f"Ventilator ICU: {l['icuwithventilator']} \n"
                + f"Remarks:  {l['remarks']} \n"
            )

        message = message + "\n*" + r["hospital"] + "*\n" + status_msg + "\n\n"

    return message


def process_pincode(pincode, n_latest=1):
    """
    Return the data of all hospitals in a pincode
    Format the response string
    """

    status = read_status_logs()

    sel_status, hosp_count = hosps_in_pincode(status, pincode)

    grp = sel_status.groupby("hospital")
    logs = []
    for hosp, s in grp:
        logs.append({"hospital": hosp, "logs": get_latest(s, n_latest=1)})

    if len(logs) == 0:
        message = "No hospitals found"
    else:
        message = prepare_message(logs)
    return message


def process_zone(zone, n_latest=1):
    """
    Return the data of all hospitals in a zone
    Format the response string
    """
    status = read_status_logs()

    sel_status, hosp_count = hosps_in_zone(status, zone)

    grp = sel_status.groupby("hospital")
    logs = []
    for hosp, s in grp:
        logs.append({"hospital": hosp, "logs": get_latest(s, n_latest=1)})

    if len(logs) == 0:
        message = "No hospitals found"
    else:
        message = prepare_message(logs)
    return message


def entry(bot, update):
    """
    Handle all actions by the bot
    """

    # CALLBACKS
    if update.callback_query:
        if update.callback_query.message.reply_to_message.text.startswith("/zone"):
            zone = update.callback_query.data
            try:
                message = process_zone(zone)
                logging.debug(message)
                bot.send_message(
                    chat_id=update.callback_query.message.chat.id,
                    text=message,
                    parse_mode=telegram.ParseMode.MARKDOWN,
                )
            except Exception as e:
                logging.error(e)
                bot.send_message(
                    chat_id=update.callback_query.message.chat.id,
                    text="Hospital fetch failed",
                )

            return

        if update.callback_query.message.reply_to_message.text.startswith("/pincode"):
            pincode = update.callback_query.data
            try:
                message = process_pincode(pincode)
                bot.send_message(
                    chat_id=update.callback_query.message.chat.id,
                    text=message,
                    parse_mode=telegram.ParseMode.MARKDOWN,
                )
            except Exception as e:
                logging.error(e)
                bot.send_message(
                    chat_id=update.callback_query.message.chat.id,
                    text="Hospital fetch failed",
                )
                logging.error(e)

            return

    if update.message:
        # ZONE
        try:
            if update.message.text.startswith("/zone"):
                bot.send_chat_action(
                    chat_id=update.message.chat.id, action=telegram.ChatAction.TYPING
                )
                button_list = []
                for zone in zones["zones"]:
                    button_list.append(InlineKeyboardButton(zone, callback_data=zone))
                reply_markup = InlineKeyboardMarkup(build_menu(button_list, n_cols=2))
                bot.send_message(
                    chat_id=update.message.chat.id,
                    text="Which zone's hospitals do you want to check?",
                    reply_to_message_id=update.message.message_id,
                    reply_markup=reply_markup,
                )
                return
        except Exception as e:
            logging.error(e)
            bot.send_message(
                chat_id=update.message.chat.id, text="Something wrong.. :/"
            )
            return

        # PINCODES
        try:
            if update.message.text.startswith("/pincode"):
                bot.send_chat_action(
                    chat_id=update.message.chat.id, action=telegram.ChatAction.TYPING
                )
                button_list = []
                for pincode in pincodes["pincodes"]:
                    button_list.append(
                        InlineKeyboardButton(pincode, callback_data=pincode)
                    )
                reply_markup = InlineKeyboardMarkup(build_menu(button_list, n_cols=4))
                bot.send_message(
                    chat_id=update.message.chat.id,
                    text="Which pincode's hospitals do you want to check?",
                    reply_to_message_id=update.message.message_id,
                    reply_markup=reply_markup,
                )
                return
        except Exception as e:
            logging.error(e)
            bot.send_message(
                chat_id=update.message.chat.id, text="Something wrong.. :/"
            )
            return

        # TODO : FUZZY MATCH ON HOSPITAL NAMES

        # TODO : NEARBY PINCODES

        # TEST
        if update.message.text.startswith("/test"):
            update.message.reply_text("200 OK!", parse_mode=telegram.ParseMode.MARKDOWN)
            return

        # START
        if update.message.text.startswith("/help") or update.message.text.startswith(
            "/start"
        ):
            help_text = f"""
            \n*Zone*
            - Send the keyword /zone
            - Pick a zone
            - Details of hospitals in that zone is listed
            \n*Pincode*
            - Send the keyword /pincode
            - Choose a pincode
            - Latest available status of hospitals in that pincode is give
            \n\n_Send `/test` for checking if the bot is online_"""

            update.message.reply_text(
                str(help_text), parse_mode=telegram.ParseMode.MARKDOWN
            )
            return


def main():
    """
    Run the bot in perpetuity
    """

    try:
        BOT_TOKEN = os.environ["BOT_TOKEN"]
    except KeyError:
        logging.error("Bot credentials not found in environment")

    bot = telegram.Bot(BOT_TOKEN)

    update_id = 0
    while True:
        try:
            for update in bot.get_updates(offset=update_id, timeout=10):
                update_id = update.update_id + 1
                logging.info(f"Update ID:{update_id}")
                entry(bot, update)
        except NetworkError:
            sleep(1)
        except Unauthorized:
            logging.error("User has blocked the bot")
            update_id = update_id + 1


if __name__ == "__main__":
    main()
