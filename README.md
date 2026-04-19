# telegram-uploader
Use Telegram messages as backup medium. Message limit is 2GB.

requires: python-telegram-bot

telegram_files_upload.py - Simple uploader with basic auto-splitting on temporary folder (with its restore pair).
When uploading files, the file name will be used as media caption, with its subfolder name used as hashtag.

telegram_files_upload_v2.py - Uploader with unlimited auto‑splitting for large files with parallel uploads and progress bars.
also local bot support, time outs for large files handling and automatic cleanup of temporary split segments.

Get API ID/Hash from [Telegram API](https://my.telegram.org/) and Bot Token interacting with @BotFather 
Channel ID can be obtained forwarding message to @JsonDumpBot
