# telegram-uploader
Use Telegram messages as backup medium. Each message (file/document) limit is 2GB. No limit of total of messages (be mindful about [ToS](https://telegram.org/tos))

requires: a local Telegram Bot ([docker](https://hub.docker.com/r/aiogram/telegram-bot-api)) with following variables:
TELEGRAM_API_HASH TELEGRAM_API_ID TELEGRAM_MAX_CONNECTIONS=250 TELEGRAM_MAX_WEBHOOK_CONNECTIONS=200 TELEGRAM_MAX_THREADS=8

telegram_files_upload.py - Simple uploader with basic auto-splitting on temporary folder (with its restore pair).
When uploading files, the file name will be used as media caption, with its subfolder name used as hashtag.

telegram_files_upload_v2.py - Uploader with unlimited auto‑splitting for large files with parallel uploads and progress bars.
also local bot support, time outs for large files handling and automatic cleanup of temporary split segments.

Get API ID/Hash from [Telegram API](https://my.telegram.org/), Bot Token interacting with @BotFather and Channel ID can be obtained forwarding message to @JsonDumpBot
