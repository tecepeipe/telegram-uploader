# telegram-uploader
Use Telegram messages as backup medium. Message limit is 2GB.

telegram_files_upload.py - Simple uploader with basic auto-splitting on temporary folder (with its restore pair).
When uploading files, the file name will be used as media caption, with its subfolder name used as hashtag.

telegram_files_upload_v2.py - Uploader with unlimited auto‑splitting for large files with parallel uploads and progress bars.

telegram_folders_upload.py - Uploader for sub-folders, 7zipping them and uploading the unlimited parts.
