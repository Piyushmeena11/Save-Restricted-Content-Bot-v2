# ---------------------------------------------------
# File Name: main.py
# Description: A Pyrogram bot for downloading files from Telegram channels or groups 
#              and uploading them back to Telegram.
# Author: Gagan
# GitHub: https://github.com/devgaganin/
# Telegram: https://t.me/team_spy_pro
# YouTube: https://youtube.com/@dev_gagan
# Created: 2025-01-11
# Last Modified: 2025-01-11
# Version: 2.0.5
# License: MIT License
# More readable 
# ---------------------------------------------------

import time
import random
import string
import asyncio
from pyrogram import filters, Client # Added Semaphore
from devgagan import app, userrbot
from config import API_ID, API_HASH, FREEMIUM_LIMIT, PREMIUM_LIMIT, OWNER_ID, DEFAULT_SESSION
from devgagan.core.get_func import get_msg, telegram_bot
from devgagan.core.func import *
from devgagan.core.mongo import db
from pyrogram.errors import FloodWait
from datetime import datetime, timedelta
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup
import subprocess
from devgagan.modules.shrink import is_user_verified
async def generate_random_name(length=8):
    return ''.join(random.choices(string.ascii_lowercase, k=length))



users_loop = {}
interval_set = {}
batch_mode = {}

async def process_and_upload_link(userbot, user_id, msg_id, link, retry_count, message):
    try:
        res = await get_msg(userbot, user_id, msg_id, link, retry_count, message)
        if res is False:
            return False
        # await asyncio.sleep(15) # Removed fixed delay for adaptive rate limiting
        return True
    except Exception:
        return False
    finally:
        try:
            await app.delete_messages(user_id, msg_id)
        except Exception:
            pass

# Function to check if the user can proceed
async def check_interval(user_id, freecheck):
    if freecheck != 1 or await is_user_verified(user_id):  # Premium or owner users can always proceed
        return True, None

    now = datetime.now()

    # Check if the user is on cooldown
    if user_id in interval_set:
        cooldown_end = interval_set[user_id]
        if now < cooldown_end:
            remaining_time = (cooldown_end - now).seconds
            return False, f"Please wait {remaining_time} seconds(s) before sending another link. Alternatively, purchase premium for instant access.\n\n> Hey 👋 You can use /token to use the bot free for 3 hours without any time limit."
        else:
            del interval_set[user_id]  # Cooldown expired, remove user from interval set

    return True, None

async def set_interval(user_id, interval_minutes=45):
    now = datetime.now()
    # Set the cooldown interval for the user
    interval_set[user_id] = now + timedelta(seconds=interval_minutes)
    

@app.on_message(
    filters.regex(r'https?://(?:www\.)?t\.me/[^\s]+|tg://openmessage\?user_id=\w+&message_id=\d+')
    & filters.private
)
async def single_link(_, message):
    user_id = message.chat.id

    # Check subscription and batch mode
    if await subscribe(_, message) == 1 or user_id in batch_mode:
        return

    # Check if user is already in a loop
    if users_loop.get(user_id, False):
        await message.reply(
            "You already have an ongoing process. Please wait for it to finish or cancel it with /cancel."
        )
        return

    # Check freemium limits
    if await chk_user(message, user_id) == 1 and FREEMIUM_LIMIT == 0 and user_id not in OWNER_ID and not await is_user_verified(user_id):
        await message.reply("Freemium service is currently not available. Upgrade to premium for access.")
        return

    # Check cooldown
    can_proceed, response_message = await check_interval(user_id, await chk_user(message, user_id))
    if not can_proceed:
        await message.reply(response_message)
        return

    # Add user to the loop
    users_loop[user_id] = True

    link = message.text if "tg://openmessage" in message.text else get_link(message.text)
    msg = await message.reply("Processing...")
    userbot = await initialize_userbot(user_id)
    try:
        if await is_normal_tg_link(link):
            await process_and_upload_link(userbot, user_id, msg.id, link, 0, message)
            await set_interval(user_id, interval_minutes=45)
        else:
            await process_special_links(userbot, user_id, msg, link)
            
    except FloodWait as fw:
        await msg.edit_text(f'Try again after {fw.x} seconds due to floodwait from Telegram.')
    except Exception as e:
        await msg.edit_text(f"Link: `{link}`\n\n**Error:** {str(e)}")
    finally:
        users_loop[user_id] = False
        try:
            await msg.delete()
        except Exception:
            pass


async def initialize_userbot(user_id):
    data = await db.get_data(user_id)
    if data and data.get("session"):
        try:
            # Use a unique name for each client instance to avoid conflicts
            # Use in_memory=True to avoid creating session files on disk
            userbot = Client(
                name=f"userbot_{user_id}",
                api_id=API_ID,
                api_hash=API_HASH,
                device_model='iPhone 16 Pro',
                session_string=data.get("session"),
                in_memory=True
            )
            await userbot.start()
            return userbot
        except Exception as e:
            await app.send_message(user_id, f"Login Expired or invalid. Please /login again.\n\n**Error:** `{e}`")
            return None
    else:
        if DEFAULT_SESSION:
            # Ensure the default userbot is started if it's not already connected
            if userrbot and not userrbot.is_connected:
                try:
                    await userrbot.start()
                except Exception as e:
                    print(f"CRITICAL: Default session failed to start: {e}")
                    return None
            return userrbot
        else:
            return None


async def is_normal_tg_link(link: str) -> bool:
    """Check if the link is a standard Telegram link."""
    special_identifiers = ['t.me/+', 't.me/c/', 't.me/b/', 'tg://openmessage']
    return 't.me/' in link and not any(x in link for x in special_identifiers)
    
async def process_special_links(userbot, user_id, msg, link):
    if userbot is None:
        return await msg.edit_text("Try logging in to the bot and try again.")
    if 't.me/+' in link:
        result = await userbot_join(userbot, link)
        await msg.edit_text(result)
        return
    special_patterns = ['t.me/c/', 't.me/b/', '/s/', 'tg://openmessage']
    if any(sub in link for sub in special_patterns):
        await process_and_upload_link(userbot, user_id, msg.id, link, 0, msg)
        await set_interval(user_id, interval_minutes=45)
        return
    await msg.edit_text("Invalid link...")


@app.on_message(filters.command("batch") & filters.private)
async def batch_link(_, message):
    join = await subscribe(_, message)
    if join == 1:
        return
    user_id = message.chat.id
    # Check if a batch process is already running
    if users_loop.get(user_id, False):
        await app.send_message(
            message.chat.id,
            "You already have a batch process running. Please wait for it to complete."
        )
        return

    freecheck = await chk_user(message, user_id)
    if freecheck == 1 and FREEMIUM_LIMIT == 0 and user_id not in OWNER_ID and not await is_user_verified(user_id):
        await message.reply("Freemium service is currently not available. Upgrade to premium for access.")
        return

    max_batch_size = FREEMIUM_LIMIT if freecheck == 1 else PREMIUM_LIMIT

    # Start link input
    for attempt in range(3):
        start = await app.ask(message.chat.id, "Please send the start link.\n\n> Maximum tries 3")
        start_id = start.text.strip()
        s = start_id.split("/")[-1]
        if s.isdigit():
            cs = int(s)
            break
        await app.send_message(message.chat.id, "Invalid link. Please send again ...")
    else:
        await app.send_message(message.chat.id, "Maximum attempts exceeded. Try later.")
        return

    # End link input
    for attempt in range(3):
        end = await app.ask(message.chat.id, "Please send the end link.\n\n> Maximum tries 3")
        end_id = end.text.strip()
        e = end_id.split("/")[-1]
        if e.isdigit():
            ce = int(e)
            if ce < cs:
                await app.send_message(message.chat.id, "End link must be after start link.")
                continue
            cl = ce - cs + 1
            if cl <= max_batch_size:
                break
            else:
                await app.send_message(message.chat.id, f"Range exceeds limit of {max_batch_size}. Please try a smaller range.")
        else:
            await app.send_message(message.chat.id, "Invalid link. Please send again ...")
    else:
        await app.send_message(message.chat.id, "Maximum attempts exceeded. Try later.")
        return

    # Validate and interval check
    can_proceed, response_message = await check_interval(user_id, freecheck)
    if not can_proceed:
        await message.reply(response_message)
        return
        
    join_button = InlineKeyboardButton("Join Channel", url="https://t.me/team_spy_pro")
    keyboard = InlineKeyboardMarkup([[join_button]])
    pin_msg = await app.send_message(
        user_id,
        f"Batch process started ⚡\nProcessing: 0/{cl}\n\n**Powered by Team SPY**",
        reply_markup=keyboard
    )
    await pin_msg.pin(both_sides=True)

    users_loop[user_id] = True
    processed_count = 0
    try:
        normal_links_handled = False
        userbot = await initialize_userbot(user_id)
        # Handle normal links first
        for i in range(cs, ce + 1):
            if user_id in users_loop and users_loop[user_id]:
                try:
                    url = f"{'/'.join(start_id.split('/')[:-1])}/{i}"
                    link = get_link(url)
                    # Process t.me links (normal) without userbot
                    if link and 't.me/' in link and not any(x in link for x in ['t.me/b/', 't.me/c/', 'tg://openmessage']):
                        msg = await app.send_message(message.chat.id, f"Processing...")
                        if await process_and_upload_link(userbot, user_id, msg.id, link, 0, message):
                            processed_count += 1
                            await pin_msg.edit_text(
                                f"Batch process started ⚡\nProcessing: {processed_count}/{cl}\n\n**__Powered by Team SPY__**",
                                reply_markup=keyboard
                            )
                        normal_links_handled = True
                except Exception:
                    pass
            else:
                break

        if normal_links_handled:
            await set_interval(user_id, interval_minutes=300)
            await pin_msg.edit_text(
                f"Batch completed successfully for {processed_count} messages 🎉\n\n**__Powered by Team SPY__**",
                reply_markup=keyboard
            )
            await app.send_message(message.chat.id, "Batch completed successfully! 🎉")
            return
            
        # Handle special links with userbot
        for i in range(cs, ce + 1):
            if not userbot:
                await app.send_message(message.chat.id, "Login in bot first ...")
                users_loop[user_id] = False
                return
            if user_id in users_loop and users_loop[user_id]:
                try:
                    url = f"{'/'.join(start_id.split('/')[:-1])}/{i}"
                    link = get_link(url)
                    if link and any(x in link for x in ['t.me/b/', 't.me/c/']):
                        msg = await app.send_message(message.chat.id, f"Processing...")
                        if await process_and_upload_link(userbot, user_id, msg.id, link, 0, message):
                            processed_count += 1
                            await pin_msg.edit_text(
                                f"Batch process started ⚡\nProcessing: {processed_count}/{cl}\n\n**__Powered by Team SPY__**",
                                reply_markup=keyboard
                            )
                except Exception:
                    pass
            else:
                break

        await set_interval(user_id, interval_minutes=300)
        await pin_msg.edit_text(
            f"Batch completed successfully for {processed_count} messages 🎉\n\n**__Powered by Team SPY__**",
            reply_markup=keyboard
        )
        await app.send_message(message.chat.id, "Batch completed successfully! 🎉")

    except Exception as e:
        await app.send_message(message.chat.id, f"Error: {e}")
    finally:
        users_loop.pop(user_id, None)

@app.on_message(filters.command("cancel"))
async def stop_batch(_, message):
    user_id = message.chat.id

    # Check if there is an active batch process for the user
    if user_id in users_loop and users_loop[user_id]:
        users_loop[user_id] = False  # Set the loop status to False
        await app.send_message(
            message.chat.id, 
            "Batch processing has been stopped successfully. You can start a new batch now if you want."
        )
    elif user_id in users_loop and not users_loop[user_id]:
        await app.send_message(
            message.chat.id, 
            "The batch process was already stopped. No active batch to cancel."
        )
    else:
        await app.send_message(
            message.chat.id, 
            "No active batch processing is running to cancel."
        )

@app.on_message(filters.command("topic") & filters.private)
async def topic_batch(_, message):
    join = await subscribe(_, message)
    if join == 1:
        return
    user_id = message.chat.id
    if users_loop.get(user_id, False):
        await app.send_message(user_id, "You already have a process running. Please wait or /cancel.")
        return

    freecheck = await chk_user(message.chat.id, user_id) # Corrected chk_user call
    if freecheck == 1 and FREEMIUM_LIMIT == 0 and user_id not in OWNER_ID and not await is_user_verified(user_id):
        await message.reply("Freemium service is currently not available. Upgrade to premium for access.")
        return

    max_batch_size = (FREEMIUM_LIMIT + 20) if await is_user_verified(user_id) else (FREEMIUM_LIMIT if freecheck == 1 else PREMIUM_LIMIT)

    try:
        start_link_msg = await app.ask(message.chat.id, "Please send the Start Message Link from the topic.", timeout=60)
        start_link = start_link_msg.text.strip()
        if "/c/" not in start_link:
            await start_link_msg.reply("❌ Invalid Link. Please send a valid message link from a private channel/supergroup topic.")
            return
    except asyncio.TimeoutError:
        await message.reply("⏰ Timed out. Please try again.")
        return

    try:
        parts = start_link.split("/")
        chat_id_str = parts[parts.index('c') + 1]
        chat_id = int(f"-100{chat_id_str}")
        start_msg_id = int(parts[-1])
        
        userbot = await initialize_userbot(user_id)
        if not userbot:
            await message.reply("❌ Userbot not initialized. Please /login first.")
            return
            
        start_message_obj = await userbot.get_messages(chat_id, start_msg_id)
        if not start_message_obj or not getattr(start_message_obj, 'message_thread_id', None):
            await message.reply("❌ This message is not part of a topic or I can't access it.")
            return
        topic_id = start_message_obj.message_thread_id
    except Exception as e:
        await message.reply(f"❌ Error parsing start link: {e}")
        return

    try:
        is_full_topic_download = False
        end_link_msg = await app.ask(message.chat.id, "Please send the End Message Link, or type 'no' to download the rest of the topic.", timeout=60)
        if end_link_msg.text.strip().lower() == 'no':
            is_full_topic_download = True
            last_message_list = [msg async for msg in userbot.get_chat_history(chat_id, limit=1)]
            end_msg_id = last_message_list[0].id if last_message_list else start_msg_id
        else:
            end_link = end_link_msg.text.strip()
            if "/c/" not in end_link or str(chat_id_str) not in end_link:
                await end_link_msg.reply("❌ Invalid Link. End link must be from the same chat.")
                return
            end_parts = end_link.split("/")
            end_msg_id = int(end_parts[-1])
    except asyncio.TimeoutError:
        await message.reply("⏰ Timed out. Please try again.")
        return
    except Exception as e:
        await message.reply(f"❌ Error parsing end link: {e}")
        return

    if end_msg_id < start_msg_id:
        await message.reply("End message must be after start message.")
        return

    total_to_check = end_msg_id - start_msg_id + 1
    if not is_full_topic_download and total_to_check > max_batch_size:
        await message.reply(f"Range exceeds limit of {max_batch_size}. Please try a smaller range.")
        return

    pin_msg = await app.send_message(user_id, f"Topic batch process started ⚡\nTotal messages to check: {total_to_check}\n\n**Powered by Team SPY**")
    users_loop[user_id] = True
    
    download_queue = asyncio.Queue()
    processed_count = 0
    failed_count = 0
    
    try:
        # --- Step 1: Efficient Message Fetching ---
        messages_to_process = []
        await pin_msg.edit("Fetching message list from topic...")
        
        # Iterate through chat history and filter by topic_id and message ID range
        # get_chat_history fetches from newest to oldest by default
        async for msg in userbot.get_chat_history(chat_id):
            if not users_loop.get(user_id):
                break
            if msg.id < start_msg_id: # Optimization: stop if we're below the start ID
                break
            if msg.id <= end_msg_id and getattr(msg, 'message_thread_id', None) == topic_id:
                messages_to_process.append(msg)

        messages_to_process.reverse() # Sort from oldest to newest for chronological processing
        total_actual_messages = len(messages_to_process)
        
        if total_actual_messages == 0:
            await pin_msg.edit("No messages found in the specified topic and range.")
            users_loop[user_id] = False
            return

        await pin_msg.edit(f"Found {total_actual_messages} messages. Starting pipeline processing...")

        # --- Step 2 & 3: Pipeline Processing (Download while Uploading) and Smart Delay ---
        async def downloader_task():
            nonlocal failed_count
            for msg_obj in messages_to_process:
                if not users_loop.get(user_id):
                    break
                try:
                    # Download the file
                    edit_msg = await app.send_message(user_id, f"📥 Downloading message `{msg_obj.id}`...")
                    file_path = await userbot.download_media(msg_obj, file_name=f"downloads/{msg_obj.id}")
                    await edit_msg.delete()
                    
                    # Put the downloaded file path into the queue for uploading
                    await download_queue.put((msg_obj, file_path))
                except FloodWait as fw:
                    failed_count += 1
                    await pin_msg.edit(f"Floodwait of {fw.value}s during download. Sleeping...")
                    await asyncio.sleep(fw.value + 5)
                    await app.send_message(LOG_GROUP, f"Download for msg {msg_obj.id} (user {user_id}) hit FloodWait & was skipped after waiting.")
                except Exception as e:
                    failed_count += 1
                    print(f"Error downloading message {msg_obj.id}: {e}")
                    await app.send_message(LOG_GROUP, f"Error downloading message {msg_obj.id} for user {user_id}: {e}")
            await download_queue.put(None) # Sentinel to signal end of downloads

        async def uploader_task():
            nonlocal processed_count, failed_count
            while True:
                if not users_loop.get(user_id):
                    break
                item = await download_queue.get()
                if item is None: # Sentinel received, no more files to upload
                    break
                
                msg_obj, file_path = item
                try:
                    edit_msg = await app.send_message(user_id, f"⬆️ Uploading message `{msg_obj.id}`...")
                    await telegram_bot._process_message(userbot, msg_obj, user_id, edit_msg, downloaded_file_path=file_path)
                    processed_count += 1
                except FloodWait as fw:
                    failed_count += 1
                    await pin_msg.edit(f"Floodwait of {fw.value}s during upload. One task is sleeping...")
                    await asyncio.sleep(fw.value + 5)
                    await app.send_message(LOG_GROUP, f"Upload for msg {msg_obj.id} (user {user_id}) hit FloodWait & was skipped after waiting.")
                except Exception as e:
                    failed_count += 1
                    print(f"Error uploading message {msg_obj.id}: {e}")
                    await app.send_message(LOG_GROUP, f"Error uploading message {msg_obj.id} for user {user_id}: {e}")
                finally:
                    # Update progress in main message
                    await pin_msg.edit(
                        f"Topic batch process running ⚡\n"
                        f"Processed: {processed_count}/{total_actual_messages}\n"
                        f"Failed: {failed_count}\n"
                        f"Current: {msg_obj.id}\n\n"
                        f"**Powered by Team SPY**"
                    )
                    if file_path and os.path.exists(file_path):
                        os.remove(file_path) # Clean up downloaded file after upload attempt
                download_queue.task_done()

        await asyncio.gather(downloader_task(), uploader_task())
        await download_queue.join() # Wait for all items in the queue to be processed

        if not users_loop.get(user_id):
            await pin_msg.edit("🛑 Batch process cancelled.")
        else:
            await pin_msg.edit(f"✅ Topic batch completed!\n\nProcessed: {processed_count}\nFailed: {failed_count}")

    except Exception as e:
        await message.reply(f"An error occurred during batch processing: {e}")
        if LOG_GROUP:
            try:
                await app.send_message(LOG_GROUP, f"Critical error in /topic for user {user_id}: {e}")
            except Exception as log_e:
                print(f"Failed to log critical error: {log_e}")
    finally:
        users_loop[user_id] = False
        # Ensure any remaining downloaded files are cleaned up if process stops prematurely
        while not download_queue.empty():
            item = await download_queue.get_nowait()
            if item and item is not None:
                msg_obj, file_path = item
                if file_path and os.path.exists(file_path):
                    os.remove(file_path)
            download_queue.task_done()
