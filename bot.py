import os
import sys
import time
import aiohttp
import logging
import random
import shutil
import re
import html
import asyncio
import json
import math
import subprocess
import uuid
import signal
import aiofiles
from pathlib import Path
import xml.etree.ElementTree as ET
from urllib.parse import urlparse
from os import path as ospath, remove as osremove, makedirs
import os  # Add this import
from typing import Optional, Dict, Callable, List, Tuple, Union
from pyrogram import Client, filters, idle
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery , InputMediaPhoto
from pyrogram.errors import FloodWait, RPCError, MessageNotModified, BadRequest
import humanize
import threading
import psutil
from functools import wraps
from datetime import timedelta
from asyncio.subprocess import PIPE
from PIL import Image, ImageDraw, ImageFont
from datetime import datetime, timezone
import pytz
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import ConnectionFailure
from collections import defaultdict
from pyrogram.enums import ParseMode
from asyncio import Lock, Semaphore
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)
# Bot configuration - Use environment variables
API_ID = int(os.getenv('API_ID', '22922577'))
API_HASH = os.getenv('API_HASH', 'ff5513f0b7e10b92a940bd107e1ac32a')
BOT_TOKEN = os.getenv('BOT_TOKEN', '1599848664:AAEEefZw_WfGPEKHqEwlYvRySXrBXcQzlhg')
MAX_CONCURRENT_TASKS = int(os.getenv('MAX_CONCURRENT_TASKS', '1'))
MAX_FILE_SIZE = int(os.getenv('MAX_FILE_SIZE', '4294967296'))
MAX_TOTAL_SIZE = int(os.getenv('MAX_TOTAL_SIZE', '32212254720'))
TORRENT_SIZE_LIMIT = 5 * 1024 * 1024 * 1024  
SINGLE_FILE_SIZE_LIMIT = 4 * 1024 * 1024 * 1024  
BOT_OWNER_ID = int(os.getenv('BOT_OWNER_ID', '1047253913'))
MONGODB_URI = os.getenv('MONGODB_URI', 'mongodb+srv://robinenca_db_user:Gm6nADrBcjgHTzy5@robinenc.sj6hua0.mongodb.net/?appName=Robinenc')
MONGODB_NAME = os.getenv('MONGODB_NAME', 'Robinenc')
START_PIC = "https://envs.sh/X6i.jpg" 
SETTINGS_SUMMARY_PIC = "https://envs.sh/X6i.jpg"
SUDO_USERS = [BOT_OWNER_ID, 1074804932,   -1002837442072]  
BOT_AUTHORIZED = False  
PREMIUM_USERS = {}  
BANNED_USERS = {}
task_id_to_user = {}  
SHOWCASE_CHANNEL = os.getenv('SHOWCASE_CHANNEL', '-1003203954408')  
SHOWCASE_ENABLED = os.getenv('SHOWCASE_ENABLED', 'False').lower() == 'true'
SHOWCASE_FORMAT = os.getenv('SHOWCASE_FORMAT', 'forward')  
QUEUE_PRIORITY_LEVELS = {
    "sudo": 1,
    "premium": 2, 
    "normal": 3
}
PRIORITY_WEIGHTS = {
    1: 0.1,  # Sudo - 10% of normal time
    2: 0.5,  # Premium - 50% of normal time
    3: 1.0   # Normal - 100% of normal time
}
START_PICS = {
    "main": "https://files.catbox.moe/so1re5.jpg",
    "encoding": "https://files.catbox.moe/3h9uqm.jpg",  # Replace with actual URLs
    "features": "https://files.catbox.moe/dp839i.jpg",
    "help": "https://files.catbox.moe/fnakf7.jpg",
    "ffmpeg": "https://files.catbox.moe/gam9bj.jpg",
    "admin": "https://files.catbox.moe/admin.jpg"
}

WATERMARK_POSITIONS = {
    "top-left": "Top Left Corner",
    "top-right": "Top Right Corner", 
    "bottom-left": "Bottom Left Corner",
    "bottom-right": "Bottom Right Corner",
    "center": "Center",
    "random": "Random Position (for piracy prevention)"
}
WATERMARK_SCALES = {
    "5%": "Very Small (5% of video width)",
    "10%": "Small (10% of video width)", 
    "15%": "Medium (15% of video width)",
    "20%": "Large (20% of video width)",
    "25%": "Very Large (25% of video width)"
}
WATERMARK_OPACITY = {
    "30%": "Light (30% opacity)",
    "50%": "Medium (50% opacity)",
    "70%": "Strong (70% opacity)",
    "90%": "Heavy (90% opacity)",
    "100%": "Full (100% opacity)"
}
storage_analytics = {
    "total_downloads": 0,
    "total_download_size": 0,
    "total_processed": 0, 
    "total_processed_size": 0,
    "total_saved_size": 0,
    "user_statistics": {},  
    "daily_statistics": {} 
}
AVAILABLE_CODECS = {
    'h264': 'H.264 (Compatible)',
    'h265': 'H.265 (HEVC)', 
    'av1': 'AV1 (Modern)'
}

AV1_SETTINGS = {
    '480p': {'crf': 32, 'preset': '6'},
    '720p': {'crf': 30, 'preset': '7'},
    '1080p': {'crf': 28, 'preset': '7'},
    '4k': {'crf': 26, 'preset': '6'}
}

PIXEL_FORMATS = {
    'yuv420p': 'yuv420p (8-bit, Maximum Compatibility)',
    'yuv420p10le': 'yuv420p10le (10-bit, HDR - H.265 Recommended)',
    'yuv422p': 'yuv422p (Progressive - Professional Use)',
    'yuv444p': 'yuv444p (Full Chroma - Large Files)'
}
CODEC_PROFILES = {
    'h264': {
        'baseline': 'Baseline (Maximum Compatibility)',
        'main': 'Main (Recommended Balance)',
        'high': 'High (Best Quality)',
        'high10': 'High 10-bit (10-bit Support)'
    },
    'h265': {
        'main': 'Main (8-bit, Best Compatibility)',
        'main10': 'Main 10 (10-bit, HDR Support)',
        'main12': 'Main 12 (12-bit, Professional)',
        'main444-8': 'Main 4:4:4 (8-bit, Full Chroma)',
        'main444-10': 'Main 4:4:4 (10-bit, Full Chroma HDR)',
        'main444-12': 'Main 4:4:4 (12-bit, Professional)'
    },
    'av1': {
        'main': 'Main (Best Compatibility)',
        'high': 'High (Better Quality)',
        'professional': 'Professional (Advanced Features)'
    }
}
COMPATIBILITY_MATRIX = {
    'h264': {
        'baseline': ['yuv420p'],
        'main': ['yuv420p'],
        'high': ['yuv420p', 'yuv422p', 'yuv444p'],
        'high10': ['yuv420p10le']
    },
    'h265': {
        'main': ['yuv420p'],
        'main10': ['yuv420p10le'],
        'main12': ['yuv420p10le'],
        'main444-8': ['yuv444p'],
        'main444-10': ['yuv444p'],
        'main444-12': ['yuv444p']
    },
    'av1': {
        'main': ['yuv420p', 'yuv420p10le'],
        'high': ['yuv420p', 'yuv420p10le', 'yuv422p', 'yuv444p'],
        'professional': ['yuv420p', 'yuv420p10le', 'yuv422p', 'yuv444p']
    }
}
try:
    mongo_client = AsyncIOMotorClient(MONGODB_URI)
    db = mongo_client[MONGODB_NAME]
    logger.info("MongoDB connected successfully")
    user_settings = db["user_settings"] if db is not None else None
    active_tasks_db = db["active_tasks"] if db is not None else None
    task_queue_db = db["task_queue"] if db is not None else None
    error_logs_db = db["error_logs"] if db is not None else None
    premium_users_db = db["premium_users"] if db is not None else None
    bot_settings_db = db["bot_settings"] if db is not None else None
    sudo_users_db = db["sudo_users"] if db is not None else None
    banned_users_db = db["banned_users"]  if db is not None else None
    showcase_settings_db = db["showcase_settings"] if db is not None else None
    task_performance_db = db["task_performance"] if db is not None else None
    storage_analytics_db = db["storage_analytics"] if db is not None else None
    logger.info(f"🔍 Collection status - bot_settings_db: {bot_settings_db is not None}, sudo_users_db: {sudo_users_db is not None}")
except Exception as e:
    logger.error(f"MongoDB connection failed: {e}")
    db = None
    user_settings = None
    active_tasks_db = None
    task_queue_db = None
    error_logs_db = None
    bot_settings_db = None
    task_performance_db = None
    storage_analytics_db = None
    banned_users_db = None  
async def test_mongodb_connection():
    """Test MongoDB connection and collections"""
    try:
        if db is not None:
            collections = await db.list_collection_names()
            logger.info(f"📁 Available collections: {collections}")
            if 'sudo_users' in collections:
                count = await sudo_users_db.count_documents({})
                logger.info(f"📊 Sudo users in DB: {count}")
            else:
                logger.info("📊 Sudo users collection doesn't exist yet")
            if 'bot_settings' in collections:
                settings = await bot_settings_db.find_one({"setting_type": "authorization"})
                logger.info(f"⚙️ Bot settings in DB: {settings is not None}")
            else:
                logger.info("⚙️ Bot settings collection doesn't exist yet")
        else:
            logger.info("❌ MongoDB not available")
    except Exception as e:
        logger.error(f"❌ MongoDB test failed: {e}")
async def load_banned_users():
    """Load banned users from MongoDB"""
    global BANNED_USERS
    try:
        logger.info("🔍 Loading banned users from MongoDB...")
        
        if banned_users_db is None:
            logger.warning("⚠️ banned_users_db is None, using empty banned users")
            BANNED_USERS = {}
            return
        count = await banned_users_db.count_documents({})
        logger.info(f"📊 Found {count} banned users in database")        
        BANNED_USERS = {}
        if count > 0:
            banned_docs = await banned_users_db.find().to_list(length=None)
            for doc in banned_docs:
                user_id = doc['user_id']
                BANNED_USERS[user_id] = {
                    "reason": doc.get('reason', 'No reason provided'),
                    "banned_by": doc.get('banned_by', 'Unknown'),
                    "banned_at": doc.get('banned_at', datetime.now().isoformat()),
                    "expires_at": doc.get('expires_at'),  
                    "last_updated": doc.get('last_updated', datetime.now().isoformat())
                }
            logger.info(f"✅ Loaded {len(BANNED_USERS)} banned users from MongoDB")
        else:
            logger.info("ℹ️ No banned users found in database")
            BANNED_USERS = {}            
    except Exception as e:
        logger.error(f"❌ Failed to load banned users from MongoDB: {str(e)}")
        BANNED_USERS = {}
async def save_banned_user(user_id: int, ban_data: dict):
    """Save banned user to MongoDB"""
    try:
        if banned_users_db is None:
            logger.warning(f"⚠️ Cannot save banned user {user_id} - MongoDB not available")
            return False
        ban_data["last_updated"] = datetime.now().isoformat()
        result = await banned_users_db.update_one(
            {"user_id": user_id},
            {
                "$set": ban_data,
                "$setOnInsert": {
                    "banned_at": datetime.now().isoformat()
                }
            },
            upsert=True
        )
        if result.upserted_id:
            logger.info(f"✅ Added new banned user: {user_id}")
        else:
            logger.info(f"✅ Updated banned user: {user_id}")  
        return True
    except Exception as e:
        logger.error(f"❌ Failed to save banned user {user_id}: {str(e)}")
        return False
async def remove_banned_user(user_id: int):
    """Remove banned user from MongoDB"""
    try:
        if banned_users_db is None:
            logger.warning(f"⚠️ Cannot remove banned user {user_id} - MongoDB not available")
            return False     
        result = await banned_users_db.delete_one({"user_id": user_id})
        if result.deleted_count > 0:
            logger.info(f"✅ Removed banned user: {user_id}")
            return True
        else:
            logger.warning(f"⚠️ Banned user {user_id} not found in database")
            return False    
    except Exception as e:
        logger.error(f"❌ Failed to remove banned user {user_id}: {str(e)}")
        return False
async def is_user_banned(user_id: int) -> bool:
    """Check if user is banned"""
    try:
        if user_id in BANNED_USERS:
            ban_data = BANNED_USERS[user_id]
            expires_at = ban_data.get('expires_at')
            if expires_at:
                expiry_time = datetime.fromisoformat(expires_at).timestamp()
                if time.time() > expiry_time:
                    await unban_user(user_id)
                    return False
            return True
        return False
    except Exception as e:
        logger.error(f"Error checking ban status for {user_id}: {str(e)}")
        return False
async def ban_user(user_id: int, banned_by: int, reason: str = "No reason provided", duration: str = None):
    """Ban a user with proper timing support"""
    try:
        expires_at = None
        
        if duration:
            duration = duration.lower()
            if duration.endswith('h'):
                hours = int(duration[:-1])
                expires_at = datetime.now() + timedelta(hours=hours)
            elif duration.endswith('d'):
                days = int(duration[:-1])
                expires_at = datetime.now() + timedelta(days=days)
            elif duration.endswith('w'):
                weeks = int(duration[:-1])
                expires_at = datetime.now() + timedelta(weeks=weeks)
            elif duration.endswith('m'):
                months = int(duration[:-1])
                expires_at = datetime.now() + timedelta(days=months*30)
            elif duration.endswith('y'):
                years = int(duration[:-1])
                expires_at = datetime.now() + timedelta(days=years*365)
            else:
                try:
                    days = int(duration)
                    expires_at = datetime.now() + timedelta(days=days)
                except ValueError:
                    expires_at = None
        
        ban_data = {
            "user_id": user_id,
            "reason": reason,
            "banned_by": banned_by,
            "banned_at": datetime.now().isoformat(),
            "expires_at": expires_at.isoformat() if expires_at else None,
            "last_updated": datetime.now().isoformat()
        }
        BANNED_USERS[user_id] = ban_data
        if banned_users_db is not None:
            await banned_users_db.update_one(
                {"user_id": user_id},
                {"$set": ban_data},
                upsert=True
            )
        await cancel_user_tasks(user_id)
        return True, ban_data
    except Exception as e:
        logger.error(f"Error banning user {user_id}: {str(e)}")
        return False, None
async def unban_user(user_id: int):
    """Unban a user"""
    try:
        if user_id in BANNED_USERS:
            del BANNED_USERS[user_id]
        success = await remove_banned_user(user_id)
        return True
    except Exception as e:
        logger.error(f"Error unbanning user {user_id}: {str(e)}")
        return False
async def load_sudo_users():
    """Load sudo users from MongoDB"""
    global SUDO_USERS
    try:
        logger.info(f"🔍 Loading sudo users - sudo_users_db: {sudo_users_db is not None}")
        if sudo_users_db is not None:
            count = await sudo_users_db.count_documents({})
            logger.info(f"📊 Sudo users in DB: {count}")
            if count > 0:
                sudo_docs = await sudo_users_db.find().to_list(length=None)
                loaded_users = [doc['user_id'] for doc in sudo_docs]
                SUDO_USERS = loaded_users
                logger.info(f"✅ Loaded {len(SUDO_USERS)} sudo users from MongoDB: {SUDO_USERS}")
            else:
                default_sudo = [BOT_OWNER_ID, -1002837442072]
                SUDO_USERS = default_sudo
                await save_sudo_users()  
                logger.info(f"🔄 Initialized default sudo users: {SUDO_USERS}")
        else:
            SUDO_USERS = [BOT_OWNER_ID, -1002837442072]
            logger.info("⚠️ Using default sudo users (MongoDB not available)")
    except Exception as e:
        logger.error(f"❌ Failed to load sudo users from MongoDB: {str(e)}")
        SUDO_USERS = [BOT_OWNER_ID, -1002837442072]
        logger.info(f"🔄 Using fallback sudo users: {SUDO_USERS}")
async def save_sudo_users():
    """Save sudo users to MongoDB"""
    try:
        if sudo_users_db is not None:
            await sudo_users_db.delete_many({})
            if SUDO_USERS:
                sudo_docs = [{"user_id": user_id} for user_id in SUDO_USERS]
                await sudo_users_db.insert_many(sudo_docs)
            logger.info(f"💾 Saved {len(SUDO_USERS)} sudo users to MongoDB")
    except Exception as e:
        logger.error(f"❌ Failed to save sudo users to MongoDB: {str(e)}")
async def load_premium_users():
    """Load premium users from MongoDB"""
    global PREMIUM_USERS
    try:
        if premium_users_db is not None:
            premium_docs = await premium_users_db.find().to_list(length=None)
            for doc in premium_docs:
                PREMIUM_USERS[doc['user_id']] = {
                    "expiry": doc['expiry'],
                    "added_by": doc.get('added_by', BOT_OWNER_ID),
                    "plan": doc.get('plan', 'custom'),
                    "added_date": doc.get('added_date', datetime.now().isoformat())
                }
            logger.info(f"✅ Loaded {len(PREMIUM_USERS)} premium users from MongoDB")
    except Exception as e:
        logger.error(f"❌ Failed to load premium users: {str(e)}")
        PREMIUM_USERS = {}
async def save_premium_user(user_id: int, premium_data: dict):
    """Save premium user to MongoDB"""
    try:
        if premium_users_db is not None:
            await premium_users_db.update_one(
                {"user_id": user_id},
                {
                    "$set": {
                        **premium_data,
                        "last_updated": datetime.now().isoformat()
                    }
                },
                upsert=True
            )
    except Exception as e:
        logger.error(f"❌ Failed to save premium user: {str(e)}")
async def remove_premium_user(user_id: int):
    """Remove premium user from MongoDB"""
    try:
        if premium_users_db is not None:
            await premium_users_db.delete_one({"user_id": user_id})
    except Exception as e:
        logger.error(f"❌ Failed to remove premium user: {str(e)}")
async def load_showcase_settings():
    """Load showcase settings from MongoDB"""
    global SHOWCASE_CHANNEL, SHOWCASE_ENABLED, SHOWCASE_FORMAT
    try:
        if bot_settings_db is not None:
            settings = await bot_settings_db.find_one({"setting_type": "showcase"})
            if settings:
                SHOWCASE_CHANNEL = settings.get("channel", "")
                SHOWCASE_ENABLED = settings.get("enabled", False)
                SHOWCASE_FORMAT = settings.get("format", "forward")
                logger.info(f"✅ Loaded showcase settings: Enabled={SHOWCASE_ENABLED}, Channel={SHOWCASE_CHANNEL}, Format={SHOWCASE_FORMAT}")
    except Exception as e:
        logger.error(f"Failed to load showcase settings: {str(e)}")

async def save_showcase_settings():
    """Save showcase settings to MongoDB"""
    try:
        if bot_settings_db is not None:
            await bot_settings_db.update_one(
                {"setting_type": "showcase"},
                {
                    "$set": {
                        "channel": SHOWCASE_CHANNEL,
                        "enabled": SHOWCASE_ENABLED,
                        "format": SHOWCASE_FORMAT,
                        "last_updated": datetime.now().isoformat()
                    }
                },
                upsert=True
            )
            logger.info("💾 Saved showcase settings to MongoDB")
            return True
    except Exception as e:
        logger.error(f"Failed to save showcase settings: {str(e)}")
    return False
async def load_bot_settings():
    """Load bot settings from MongoDB"""
    global BOT_AUTHORIZED
    try:
        logger.info(f"🔍 Loading bot settings - bot_settings_db: {bot_settings_db is not None}")
        if bot_settings_db is not None:
            settings = await bot_settings_db.find_one({"setting_type": "authorization"})
            if settings:
                BOT_AUTHORIZED = settings.get("authorized", False)
                logger.info(f"✅ Loaded authorization setting from MongoDB: {BOT_AUTHORIZED}")
            else:
                # Default to False if no setting exists
                BOT_AUTHORIZED = False
                await save_bot_settings()  # Save default to DB
                logger.info("🔄 Initialized default authorization setting")
        else:
            logger.info("⚠️ Using default authorization setting (MongoDB not available)")
    except Exception as e:
        logger.error(f"❌ Failed to load bot settings from MongoDB: {str(e)}")
        BOT_AUTHORIZED = False
async def save_bot_settings():
    """Save bot settings to MongoDB"""
    try:
        if bot_settings_db is not None:
            await bot_settings_db.update_one(
                {"setting_type": "authorization"},
                {
                    "$set": {
                        "authorized": BOT_AUTHORIZED,
                        "last_updated": datetime.now().isoformat(),
                        "updated_by": "system"
                    }
                },
                upsert=True
            )
            logger.info(f"💾 Saved authorization setting to MongoDB: {BOT_AUTHORIZED}")
    except Exception as e:
        logger.error(f"❌ Failed to save bot settings to MongoDB: {str(e)}")
# Add to bot_settings_db for persistence
async def load_size_limits():
    """Load size limits from MongoDB"""
    global TORRENT_SIZE_LIMIT, SINGLE_FILE_SIZE_LIMIT
    try:
        if bot_settings_db is not None:
            limits = await bot_settings_db.find_one({"setting_type": "size_limits"})
            if limits:
                TORRENT_SIZE_LIMIT = limits.get("torrent_limit", 1 * 1024 * 1024 * 1024)
                SINGLE_FILE_SIZE_LIMIT = limits.get("single_file_limit", 4 * 1024 * 1024 * 1024)
                logger.info(f"📏 Loaded size limits: Torrent={humanize.naturalsize(TORRENT_SIZE_LIMIT)}, File={humanize.naturalsize(SINGLE_FILE_SIZE_LIMIT)}")
    except Exception as e:
        logger.error(f"Failed to load size limits: {str(e)}")
async def save_size_limits():
    """Save size limits to MongoDB"""
    try:
        if bot_settings_db is not None:
            await bot_settings_db.update_one(
                {"setting_type": "size_limits"},
                {
                    "$set": {
                        "torrent_limit": TORRENT_SIZE_LIMIT,
                        "single_file_limit": SINGLE_FILE_SIZE_LIMIT,
                        "last_updated": datetime.now().isoformat()
                    }
                },
                upsert=True
            )
            logger.info(f"💾 Saved size limits to MongoDB")
    except Exception as e:
        logger.error(f"Failed to save size limits: {str(e)}")
async def update_user_settings(chat_id: int, update_data: Dict):
    """Update user settings in MongoDB with PROPER None value handling"""
    try:
        # 🎯 FIX: Handle None values explicitly
        clean_update_data = {}
        for key, value in update_data.items():
            if value is not None:
                # Handle nested metadata updates
                if key == "metadata.title" and isinstance(value, str):
                    if "metadata" not in clean_update_data:
                        clean_update_data["metadata"] = {}
                    clean_update_data["metadata"]["title"] = value
                elif key.startswith("metadata."):
                    metadata_key = key.split(".", 1)[1]
                    if "metadata" not in clean_update_data:
                        clean_update_data["metadata"] = {}
                    clean_update_data["metadata"][metadata_key] = value
                else:
                    clean_update_data[key] = value
            else:
                # 🎯 EXPLICITLY SET None VALUES
                clean_update_data[key] = None
        # If no valid data after cleaning, return early
        if not clean_update_data:
            logger.warning(f"No valid data to update for chat_id {chat_id}")
            return
        # 🎯 FIX: Ensure MongoDB connection is working
        if user_settings is not None:
            try:
                result = await user_settings.update_one(
                    {"chat_id": chat_id},
                    {"$set": clean_update_data},
                    upsert=True
                )
                logger.info(f"💾 Saved settings to MongoDB for {chat_id}: {len(clean_update_data)} fields")
            except Exception as e:
                logger.error(f"❌ MongoDB update failed for {chat_id}: {str(e)}")
        else:
            logger.warning("⚠️ MongoDB not available, using session storage")
        # Also update session data with cleaned data
        session_data = {}
        for key, value in clean_update_data.items():
            if not key.startswith('_') and key != "chat_id":
                session_data[key] = value
        await session_manager.update_session(chat_id, {'data': session_data})
    except Exception as e:
        logger.error(f"❌ Failed to update settings for {chat_id}: {str(e)}")
        # Ensure session is still updated even if MongoDB fails
        try:
            session_data = {}
            for key, value in update_data.items():
                if value is not None and not key.startswith('_') and key != "chat_id":
                    session_data[key] = value
            await session_manager.update_session(chat_id, {'data': session_data})
        except Exception as session_error:
            logger.error(f"❌ Session update also failed: {str(session_error)}")
# Remove the global storage_analytics dictionary and replace with:
async def load_storage_analytics():
    """Load storage analytics from MongoDB"""
    try:
        if storage_analytics_db is not None:
            analytics_data = await storage_analytics_db.find_one({"type": "main_analytics"})
            if analytics_data:
                return analytics_data
        # Return default structure if no data found
        return {
            "total_downloads": 0,
            "total_download_size": 0,
            "total_processed": 0,
            "total_processed_size": 0,
            "total_saved_size": 0,
            "user_statistics": {},
            "daily_statistics": {},
            "type": "main_analytics"
        }
    except Exception as e:
        logger.error(f"Failed to load storage analytics: {str(e)}")
        return {
            "total_downloads": 0,
            "total_download_size": 0,
            "total_processed": 0,
            "total_processed_size": 0,
            "total_saved_size": 0,
            "user_statistics": {},
            "daily_statistics": {},
            "type": "main_analytics"
        }
async def save_storage_analytics(analytics_data):
    """Save storage analytics to MongoDB"""
    try:
        if storage_analytics_db is not None:
            await storage_analytics_db.update_one(
                {"type": "main_analytics"},
                {"$set": analytics_data},
                upsert=True
            )
    except Exception as e:
        logger.error(f"Failed to save storage analytics: {str(e)}")
async def update_storage_analytics(chat_id: int, original_size: int, processed_size: int, action: str = "processed"):
    """Update storage analytics for tracking with MongoDB persistence"""
    try:
        # Load current analytics
        analytics_data = await load_storage_analytics()
        current_date = datetime.now().strftime('%Y-%m-%d')
        # Initialize user stats if not exists
        if str(chat_id) not in analytics_data["user_statistics"]:
            analytics_data["user_statistics"][str(chat_id)] = {
                "downloads": 0,
                "download_size": 0,
                "processed": 0,
                "processed_size": 0,
                "saved_size": 0
            }
        # Initialize daily stats if not exists
        if current_date not in analytics_data["daily_statistics"]:
            analytics_data["daily_statistics"][current_date] = {
                "downloads": 0,
                "processed": 0,
                "total_size": 0
            }
        if action == "downloaded":
            analytics_data["total_downloads"] += 1
            analytics_data["total_download_size"] += original_size
            analytics_data["user_statistics"][str(chat_id)]["downloads"] += 1
            analytics_data["user_statistics"][str(chat_id)]["download_size"] += original_size
            analytics_data["daily_statistics"][current_date]["downloads"] += 1
            analytics_data["daily_statistics"][current_date]["total_size"] += original_size
        elif action == "processed":
            analytics_data["total_processed"] += 1
            analytics_data["total_processed_size"] += processed_size
            analytics_data["total_saved_size"] += (original_size - processed_size)
            analytics_data["user_statistics"][str(chat_id)]["processed"] += 1
            analytics_data["user_statistics"][str(chat_id)]["processed_size"] += processed_size
            analytics_data["user_statistics"][str(chat_id)]["saved_size"] += (original_size - processed_size)
            analytics_data["daily_statistics"][current_date]["processed"] += 1
        # Save updated analytics back to MongoDB
        await save_storage_analytics(analytics_data)
    except Exception as e:
        logger.error(f"Storage analytics update error: {str(e)}")
async def get_storage_analytics_summary():
    """Get comprehensive storage analytics summary from MongoDB"""
    try:
        analytics_data = await load_storage_analytics()
        total_users = len(analytics_data["user_statistics"])
        avg_saved_per_file = 0
        if analytics_data["total_processed"] > 0:
            avg_saved_per_file = analytics_data["total_saved_size"] / analytics_data["total_processed"]
        # Calculate efficiency
        efficiency = 0
        if analytics_data["total_download_size"] > 0:
            efficiency = (analytics_data["total_saved_size"] / analytics_data["total_download_size"]) * 100
        # Get top users
        top_users = sorted(
            analytics_data["user_statistics"].items(),
            key=lambda x: x[1]["processed_size"],
            reverse=True
        )[:5]
        # Get daily trends
        recent_days = sorted(analytics_data["daily_statistics"].keys(), reverse=True)[:7]
        daily_trends = {day: analytics_data["daily_statistics"][day] for day in recent_days}
        return {
            "total_users": total_users,
            "total_downloads": analytics_data["total_downloads"],
            "total_download_size": analytics_data["total_download_size"],
            "total_processed": analytics_data["total_processed"],
            "total_processed_size": analytics_data["total_processed_size"],
            "total_saved_size": analytics_data["total_saved_size"],
            "efficiency": efficiency,
            "avg_saved_per_file": avg_saved_per_file,
            "top_users": top_users,
            "daily_trends": daily_trends
        }
    except Exception as e:
        logger.error(f"Storage analytics summary error: {str(e)}")
        return None
async def remove_dump_channel(chat_id: int) -> bool:
    """Remove dump channel for user"""
    try:
        await update_user_settings(chat_id, {"dump_channel": None})
        logger.info(f"✅ Dump channel removed for {chat_id}")
        return True
    except Exception as e:
        logger.error(f"Error removing dump channel: {str(e)}")
        return False

async def get_dump_channel(chat_id: int) -> str:
    """Get dump channel for user"""
    try:
        session = await get_user_settings(chat_id)
        return session.get("dump_channel")
    except Exception as e:
        logger.error(f"Error getting dump channel: {str(e)}")
        return None
# Global task tracking
active_tasks = {}  # Track active encoding/download tasks by chat_id
task_lock = asyncio.Lock()  # Lock for thread-safe operations
cancelled_tasks = set()  # Track cancelled tasks
# Global variables
semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)
message_cleanup_queue = defaultdict(list)
user_active_messages = {}  # Track active messages per user
# Global progress message tracking
progress_message_tracker = {}
user_cancellation_events = {}  # Add this at the top with other globals

# Initialize bot with improved settings
try:
    bot = Client(
        "video_encoder_bot",
        api_id=API_ID,
        api_hash=API_HASH,
        bot_token=BOT_TOKEN,
        workers=4,
        sleep_threshold=60,
        max_concurrent_transmissions=1,
        in_memory=True,
        plugins=dict(root="bot_plugins")
    )
    logger.info("✅ Bot client initialized successfully")
except Exception as e:
    logger.error(f"❌ Failed to initialize bot client: {e}")
    sys.exit(1)
# Ensure directories exist
for folder in ["downloads", "encoded", "thumbnails", "watermarks", "tempwork", "logs", "previews", "subtitles", "audio", "samples", "screenshots"]:
    makedirs(folder, exist_ok=True)
class TaskCancelled(Exception):
    """Custom exception for cancelled tasks"""
    pass
def sudo_only(func):
    """Decorator to restrict command to sudo users only"""
    @wraps(func)
    async def wrapper(client, message):
        user_id = message.from_user.id
        if user_id not in SUDO_USERS:
            await message.reply("❌ This command is restricted to authorized users only.")
            return
        return await func(client, message)
    return wrapper
# Update the premium_required decorator
def premium_required(func):
    """Decorator to restrict commands to premium/sudo users only - FIXED VERSION"""
    @wraps(func)
    async def wrapper(client, message):
        user_id = message.from_user.id
        # Always allow sudo users and bot owner
        if user_id in SUDO_USERS or user_id == BOT_OWNER_ID:
            return await func(client, message)
        # Check if user is premium and not expired
        if await is_premium_user(user_id):
            return await func(client, message)
        # If authorization is restricted, only premium/sudo can use
        if BOT_AUTHORIZED:
            await message.reply(
                "🔒 <b>Premium Feature Required</b>\n\n"
                "This command is only available for premium users.\n\n"
                "💎 <b>Benefits:</b>\n"
                "• Access to all encoding features\n"
                "• Priority processing\n"
                "• No queue waiting\n"
                "• All quality options\n\n"
                "👑 <b>Contact Admin:</b> /premium_info",
                parse_mode=ParseMode.HTML
            )
            return
        # If public mode, allow all users
        return await func(client, message)
    return wrapper
# Update the authorized_only decorator to work with premium users
def authorized_only(func):
    """Decorator to restrict command to authorized users only - UPDATED WITH BAN CHECK"""
    @wraps(func)
    async def wrapper(client, message):
        global BOT_AUTHORIZED
        user_id = message.from_user.id
        
        # Check if user is banned
        if await is_user_banned(user_id):
            ban_data = BANNED_USERS.get(user_id, {})
            reason = ban_data.get('reason', 'No reason provided')
            expires_at = ban_data.get('expires_at')
            
            if expires_at:
                expiry_time = datetime.fromisoformat(expires_at)
                expiry_str = expiry_time.strftime('%Y-%m-%d %H:%M:%S')
                ban_message = (
                    f"🚫 <b>You are banned from using this bot</b>\n\n"
                    f"📝 <b>Reason:</b> {reason}\n"
                    f"⏰ <b>Ban expires:</b> {expiry_str}\n\n"
                    f"<i>If you believe this is a mistake, contact the admin.</i>"
                )
            else:
                ban_message = (
                    f"🚫 <b>You are permanently banned from using this bot</b>\n\n"
                    f"📝 <b>Reason:</b> {reason}\n\n"
                    f"<i>If you believe this is a mistake, contact the admin.</i>"
                )
            
            await message.reply(ban_message, parse_mode=ParseMode.HTML)
            return
        
        # Rest of existing authorization logic...
        # Always allow sudo users and bot owner
        if user_id in SUDO_USERS or user_id == BOT_OWNER_ID:
            return await func(client, message)
        
        # Check if user is premium and not expired
        if await is_premium_user(user_id):
            return await func(client, message)
        
        # Check if bot is in restricted mode
        if BOT_AUTHORIZED:
            await message.reply(
                "❌ **Bot is in restricted mode**\n\n"
                "Currently only authorized users can use this bot.\n"
                "Please contact the admin for access or get premium."
            )
            return
        
        return await func(client, message)
    return wrapper
# Fix the is_premium_user function to handle edge cases
async def is_premium_user(user_id: int) -> bool:
    """Check if user is premium and not expired - FIXED VERSION"""
    try:
        if user_id in PREMIUM_USERS:
            user_data = PREMIUM_USERS[user_id]
            expiry = user_data.get('expiry', 0)
            # Check if premium has expired
            if time.time() > expiry:
                # Remove expired premium
                await revoke_premium(user_id)
                return False
            return True
        return False
    except Exception as e:
        logger.error(f"Error checking premium status for {user_id}: {str(e)}")
        return False
# Add a function to check if user can use bot
async def can_user_use_bot(user_id: int) -> bool:
    """Check if user can use the bot (considering authorization and premium)"""
    # Always allow sudo users and bot owner
    if user_id in SUDO_USERS or user_id == BOT_OWNER_ID:
        return True
    # If bot is authorized mode, check premium status
    if BOT_AUTHORIZED:
        return await is_premium_user(user_id)
    # Public mode - all users allowed
    return True
async def revoke_premium(user_id: int):
    """Remove expired premium user"""
    if user_id in PREMIUM_USERS:
        del PREMIUM_USERS[user_id]
        # Save to your database/file if needed
async def get_premium_info(user_id: int) -> str:
    """Get premium status information for user"""
    if user_id in PREMIUM_USERS:
        user_data = PREMIUM_USERS[user_id]
        expiry_timestamp = user_data.get('expiry', 0)
        expiry_date = datetime.fromtimestamp(expiry_timestamp)
        remaining = expiry_timestamp - time.time()
        if remaining <= 0:
            return "❌ <b>Premium Expired</b>\n\nYour premium access has expired."
        days = int(remaining // 86400)
        hours = int((remaining % 86400) // 3600)
        return (
            f"💎 <b>Premium Status</b>\n\n"
            f"✅ <b>Status:</b> Active\n"
            f"📅 <b>Expires:</b> {expiry_date.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"⏳ <b>Remaining:</b> {days}d {hours}h\n"
            f"📋 <b>Plan:</b> {user_data.get('plan', 'Custom')}\n"
            f"👤 <b>Added by:</b> {user_data.get('added_by', 'Admin')}"
        )
    else:
        return (
            "🔒 <b>Premium Status</b>\n\n"
            "❌ <b>Status:</b> Not Premium\n\n"
            "💎 <b>Premium Benefits:</b>\n"
            "• Unlimited encoding access\n"
            "• Priority queue position\n"
            "• All quality options\n"
            "• No restrictions\n\n"
            "👑 <b>Contact admin for premium access</b>"
        )
async def is_sudo_user(user_id: int) -> bool:
    """Check if user is sudo user"""
    return user_id in SUDO_USERS  
async def get_user_cancellation_event(chat_id: int):
    """Get or create user-specific cancellation event"""
    if chat_id not in user_cancellation_events:
        user_cancellation_events[chat_id] = asyncio.Event()
    return user_cancellation_events[chat_id]
# Global flood protection
class FloodProtection:
    def __init__(self):
        self.last_message_time = {}
        self.min_interval = 2  # Minimum seconds between messages per chat
    async def can_send_message(self, chat_id: int) -> bool:
        current_time = time.time()
        last_time = self.last_message_time.get(chat_id, 0)
        if current_time - last_time < self.min_interval:
            return False
        self.last_message_time[chat_id] = current_time
        return True
flood_protection = FloodProtection()
class EnhancedTaskQueue:
    """Enhanced task queue with priority and accurate time estimation"""
    def __init__(self):
        self.queue = asyncio.PriorityQueue()
        self.active_tasks = {}
        self.task_history = []
        self.performance_data = {}
        self.system_specs = self._get_system_specs()
        # Add missing attributes from GlobalTaskQueue
        self.current_task = None
        self.task_lock = asyncio.Lock()
        self.current_process = None
        self.task_callbacks = {}
    def _get_system_specs(self):
        """Get system specifications for performance calculation"""
        return {
            "cpu_cores": psutil.cpu_count(),
            "cpu_freq": psutil.cpu_freq().max if psutil.cpu_freq() else 3000,
            "total_ram": psutil.virtual_memory().total,
            "available_ram": psutil.virtual_memory().available
        }
    async def calculate_task_priority(self, chat_id: int, task_type: str, file_size: int = 0) -> int:
        """Calculate task priority based on user status and task characteristics"""
        base_priority = QUEUE_PRIORITY_LEVELS["normal"]
        # Check user status
        if chat_id in SUDO_USERS:
            base_priority = QUEUE_PRIORITY_LEVELS["sudo"]
        elif await is_premium_user(chat_id):
            base_priority = QUEUE_PRIORITY_LEVELS["premium"]
        # Adjust based on task type and size (smaller tasks get slight priority)
        size_adjustment = 0
        if file_size > 500 * 1024 * 1024:  # Over 500MB
            size_adjustment = 1
        elif file_size < 100 * 1024 * 1024:  # Under 100MB
            size_adjustment = -1
        return base_priority + size_adjustment
    async def estimate_processing_time(self, task_data: dict) -> float:
        """Accurately estimate processing time based on historical data and current system state - FIXED VERSION"""
        try:
            task_type = task_data.get('type', 'encode')
            file_size = task_data.get('file_size', 0)
            quality = task_data.get('quality', '720p')
            codec = task_data.get('codec', 'h264')
            preset = task_data.get('preset', 'medium')
            priority = task_data.get('priority', QUEUE_PRIORITY_LEVELS["normal"])
            # Get base time from historical data
            base_time = await self._get_base_processing_time(task_type, file_size, quality, codec, preset)
            # Adjust for current system load
            system_load_factor = self._get_system_load_factor()
            # Apply priority weight
            priority_weight = PRIORITY_WEIGHTS.get(priority, 1.0)
            # Final estimation
            estimated_time = base_time * system_load_factor * priority_weight
            logger.info(f"⏱️ Time estimation: {base_time:.1f}s base × {system_load_factor:.2f} load × {priority_weight:.2f} priority = {estimated_time:.1f}s")
            return max(estimated_time, 60)  # Minimum 60 seconds
        except Exception as e:
            logger.error(f"Time estimation error: {str(e)}")
            return 1800  # Fallback: 30 minutes
    async def _get_base_processing_time(self, task_type: str, file_size: int, quality: str, codec: str, preset: str) -> float:
        """Get base processing time from historical data or calculations - FIXED VERSION"""
        # Try to get from historical database first
        if task_performance_db is not None:
            try:
                historical = await task_performance_db.find_one({
                    "task_type": task_type,
                    "quality": quality,
                    "codec": codec,
                    "preset": preset
                })
                if historical and historical.get('avg_duration'):
                    return historical['avg_duration']
            except Exception as e:
                logger.warning(f"Failed to get historical data: {str(e)}")
        # Calculate based on file size and encoding settings
        base_speed = self._get_encoding_speed(quality, codec, preset)
        if task_type in ['encode', 'encode_with_progress', 'start_processing', 'start_all_qualities_processing']:
            # Encoding time estimation
            if file_size > 0:
                # Convert bytes to MB and calculate time
                size_in_mb = file_size / (1024 * 1024)
                return (size_in_mb / base_speed) * 60  # Convert to seconds
            return 1800  # Default 30 minutes for encoding
        elif task_type in ['handle_torrent_download', 'handle_download_and_process']:
            # Download time estimation (assuming 10MB/s average)
            if file_size > 0:
                size_in_mb = file_size / (1024 * 1024)
                return (size_in_mb / 10) * 60  # Convert to seconds
            return 600  # Default 10 minutes for download
        else:
            return 600  # Default 10 minutes for other tasks
    def _get_encoding_speed(self, quality: str, codec: str, preset: str) -> float:
        """Get encoding speed in MB per second based on settings"""
        # Base speeds (MB processed per second)
        base_speeds = {
            '480p': 5.0, '720p': 3.0, '1080p': 1.5, '4k': 0.5
        }
        # Codec multipliers
        codec_multipliers = {
            'h264': 1.0, 'h265': 0.7
        }
        # Preset multipliers
        preset_multipliers = {
            'ultrafast': 3.0, 'superfast': 2.5, 'veryfast': 2.0,
            'faster': 1.5, 'fast': 1.2, 'medium': 1.0,
            'slow': 0.8, 'slower': 0.6, 'veryslow': 0.4
        }
        base_speed = base_speeds.get(quality, 2.0)
        codec_mult = codec_multipliers.get(codec, 1.0)
        preset_mult = preset_multipliers.get(preset, 1.0)
        return base_speed * codec_mult * preset_mult
    def _get_system_load_factor(self) -> float:
        """Calculate current system load factor (1.0 = normal, >1.0 = loaded)"""
        try:
            cpu_percent = psutil.cpu_percent(interval=1) / 100.0
            memory_percent = psutil.virtual_memory().percent / 100.0
            # Weighted load factor (CPU 70%, Memory 30%)
            load_factor = (cpu_percent * 0.7) + (memory_percent * 0.3)
            # Normalize to minimum 1.0
            return max(load_factor, 1.0)
        except Exception as e:
            logger.warning(f"System load calculation error: {str(e)}")
            return 1.0
    async def add_task(self, chat_id: int, task_func, *args, **kwargs) -> str:
        """Add task to queue with proper status tracking - PREVENT DUPLICATES"""
        
        # 🎯 FIXED: Check if user already has active tasks
        async with task_lock:
            if chat_id in active_tasks:
                existing_task = active_tasks[chat_id]
                existing_status = existing_task.get('status', 'unknown')
                existing_type = existing_task.get('type', 'unknown')
                
                # If user already has a running task, warn and proceed carefully
                if existing_status == 'running':
                    logger.warning(f"⚠️ User {chat_id} already has running task: {existing_type}")
                    # We'll still allow queuing but the user should be aware
                elif existing_status == 'queued':
                    logger.info(f"ℹ️ User {chat_id} already has queued task, adding to queue")
        
        task_id = str(uuid.uuid4())
        task_id_to_user[task_id] = chat_id
 
        # Calculate file_size for estimation if not provided
        file_size = kwargs.get('file_size', 0)
        if file_size == 0:
            # Try to get file size from file_path if available
            file_path = kwargs.get('file_path') or (args[1] if len(args) > 1 else None)
            if file_path and isinstance(file_path, str) and ospath.exists(file_path):
                try:
                    file_size = ospath.getsize(file_path)
                except:
                    file_size = 0
        
        # Get task type from function name
        task_type = task_func.__name__
        
        # Calculate priority
        priority = await self.calculate_task_priority(chat_id, task_type, file_size)
        
        # Estimate processing time
        task_data = {
            'task_id': task_id,
            'chat_id': chat_id,
            'type': task_type,
            'file_size': file_size,
            'quality': kwargs.get('quality', '720p'),
            'codec': kwargs.get('codec', 'h264'),
            'preset': kwargs.get('preset', 'medium'),
            'priority': priority,
            'added_time': time.time(),
            'all_qualities': kwargs.get('all_qualities', False),
            'total_files': kwargs.get('total_files', 1)
        }
        
        estimated_time = await self.estimate_processing_time(task_data)
        task_data['estimated_duration'] = estimated_time

        # Clean kwargs to avoid parameter conflicts
        clean_kwargs = kwargs.copy()
        if 'file_size' in clean_kwargs:
            del clean_kwargs['file_size']
        if 'chat_id' in clean_kwargs and len(args) > 0 and args[0] == chat_id:
            del clean_kwargs['chat_id']

        # Store the actual function call with cleaned arguments
        task_callable = {
            'func': task_func,
            'args': args,
            'kwargs': clean_kwargs
        }

        # 🎯 FIXED: Add to priority queue
        await self.queue.put((priority, task_id, task_callable, task_data))
        
        # 🎯 FIXED: Track in active tasks as queued with proper task ID
        queue_position = await self.get_queue_position(task_id)
        async with task_lock:
            # Only update if this is a new task or we're replacing a completed one
            if chat_id not in active_tasks or active_tasks[chat_id].get('status') != 'running':
                active_tasks[chat_id] = {
                    'task_id': task_id,  # 🎯 CRITICAL: Must match the queue task ID
                    'type': task_type,
                    'start_time': time.time(),
                    'status': 'queued',  # Initial status
                    'priority': priority,
                    'estimated_duration': estimated_time,
                    'queue_position': queue_position,
                    'all_qualities': kwargs.get('all_qualities', False),
                    'total_files': kwargs.get('total_files', 1)
                }
                logger.info(f"📥 Task {task_id} added to queue with status: queued, position: {queue_position}")
            else:
                logger.warning(f"🚫 Cannot add task {task_id} - user {chat_id} already has running task")
        
        # Start processing if not already running
        asyncio.create_task(self.process_queue())
        return task_id
    def get_queue_status(self):
        """Get current queue status with active tasks - for backward compatibility"""
        return {
            "queue_size": self.queue.qsize(),
            "current_task": self.current_task,
            "active_tasks": len(active_tasks)
        }
    def set_current_process(self, process):
        """Set the current subprocess for cancellation"""
        self.current_process = process
    async def cancel_current_task(self):
        """Cancel the currently running task using user-specific cancellation"""
        try:
            if self.current_process:
                try:
                    # Check if process exists and is running
                    if hasattr(self.current_process, 'returncode'):
                        if self.current_process.returncode is None:
                            self.current_process.terminate()
                            await asyncio.sleep(2)
                            if self.current_process.returncode is None:
                                self.current_process.kill()
                                await asyncio.sleep(1)
                    else:
                        # If it's a subprocess, use different approach
                        self.current_process.terminate()
                except ProcessLookupError:
                    logger.info("Process already terminated")
                except Exception as e:
                    logger.error(f"Error killing process: {str(e)}")
            # Set cancellation for the current user
            if self.current_task:
                # Find which user owns the current task
                current_chat_id = None
                for chat_id, task_info in active_tasks.items():
                    if task_info.get('task_id') == self.current_task:
                        current_chat_id = chat_id
                        break
                if current_chat_id:
                    user_event = await get_user_cancellation_event(current_chat_id)
                    user_event.set()
                cancelled_tasks.add(self.current_task)
                self.current_task = None
        except Exception as e:
            logger.error(f"Error in cancel_current_task: {str(e)}")
    async def process_queue(self):
        """Process queue with proper status updates - FIXED TASK ID MANAGEMENT"""
        async with self.task_lock:
            if self.current_task is not None:
                return
                
            while not self.queue.empty():
                try:
                    # Get highest priority task
                    priority, task_id, task_callable, task_data = await self.queue.get()
                    chat_id = task_data['chat_id']
                    
                    # 🎯 CRITICAL FIX: Check if user already has a running task
                    async with task_lock:
                        if chat_id in active_tasks:
                            existing_task = active_tasks[chat_id]
                            existing_status = existing_task.get('status', 'unknown')
                            existing_task_id = existing_task.get('task_id')
                            
                            # If user already has a running task, skip this one for now
                            if existing_status == 'running' and existing_task_id != task_id:
                                logger.warning(f"🚫 User {chat_id} already has running task {existing_task_id}, skipping {task_id}")
                                # Put the task back in queue
                                await self.queue.put((priority, task_id, task_callable, task_data))
                                await asyncio.sleep(2)  # Wait before retrying
                                continue
                    
                    # 🎯 FIXED: Update task status to 'running' with proper task ID
                    self.current_task = task_id
                    async with task_lock:
                        # Remove any existing task for this user to avoid conflicts
                        if chat_id in active_tasks:
                            old_task_id = active_tasks[chat_id].get('task_id')
                            if old_task_id != task_id:
                                logger.info(f"🔄 Replacing old task {old_task_id} with new task {task_id} for user {chat_id}")
                        
                        # Set the new task as running
                        active_tasks[chat_id] = {
                            'task_id': task_id,  # 🎯 MUST match the current task ID
                            'type': task_data.get('type', 'unknown'),
                            'status': 'running',
                            'start_time': time.time(),
                            'estimated_duration': task_data['estimated_duration'],
                            'priority': priority,
                            'all_qualities': task_data.get('all_qualities', False),
                            'total_files': task_data.get('total_files', 1)
                        }
                    
                    logger.info(f"🎯 Processing task {task_id} (status: RUNNING) for user {chat_id}")
                    
                    try:
                        # Execute task with all stored arguments
                        task_func = task_callable['func']
                        args = task_callable['args']
                        kwargs = task_callable['kwargs']
                        
                        # 🎯 FIX: Ensure chat_id is properly passed
                        if 'chat_id' not in kwargs and len(args) == 0:
                            kwargs['chat_id'] = chat_id
                        
                        # Add task_id to kwargs if function expects it
                        if 'task_id' not in kwargs:
                            kwargs['task_id'] = task_id
                        
                        # Log the actual call for debugging
                        logger.info(f"🔧 Calling {task_func.__name__} for user {chat_id} with task_id {task_id}")
                        
                        # Execute the task
                        await task_func(*args, **kwargs)
                        
                        # Record successful completion
                        await self.record_task_performance(task_data, True)
                        
                    except TaskCancelled:
                        logger.info(f"Task {task_id} was cancelled")
                        await self.record_task_performance(task_data, False)
                    except Exception as e:
                        logger.error(f"Task {task_id} failed: {str(e)}")
                        await self.record_task_performance(task_data, False)
                        await send_error_report(chat_id, e, f"Task execution: {task_func.__name__}")
                    finally:
                        # 🎯 FIXED: Clean up task when done - ONLY if it's the same task
                        self.current_task = None
                        self.current_process = None
                        
                        # Remove from active tasks when completed - with careful checking
                        async with task_lock:
                            if chat_id in active_tasks:
                                current_task_info = active_tasks[chat_id]
                                current_task_id = current_task_info.get('task_id')
                                
                                if current_task_id == task_id:
                                    # This is the correct task to remove
                                    del active_tasks[chat_id]
                                    logger.info(f"✅ Task {task_id} completed and removed from active_tasks")
                                else:
                                    # Different task is now active for this user
                                    logger.warning(f"⚠️ Task ID mismatch: completed {task_id} but active is {current_task_id} for user {chat_id}")
                                    logger.warning(f"⚠️ Keeping current task {current_task_id} in active_tasks")
                            else:
                                logger.info(f"ℹ️ User {chat_id} not in active_tasks during cleanup")
                        
                        self.queue.task_done()
                        logger.info(f"✅ Task {task_id} completed and queue task_done called")
                        
                except Exception as e:
                    logger.error(f"Queue processing error: {str(e)}")
                    await asyncio.sleep(1)
    async def get_queue_position(self, task_id: str) -> int:
        """Get position of task in queue"""
        position = 1
        for priority, t_id, task_callable, task_data in self.queue._queue:
            if t_id == task_id:
                return position
            position += 1
        return 0
    async def get_queue_status_with_estimates(self) -> dict:
        """Get detailed queue status with accurate time estimates - FIXED VERSION"""
        queue_list = []
        current_time = time.time()
        
        # Convert priority queue to list safely
        try:
            temp_queue = list(self.queue._queue)
        except:
            temp_queue = []
        
        # Track real-time progress for accurate estimations
        real_time_progress = {}
        
        # First, check active tasks for real progress
        active_task_estimates = {}
        running_tasks_count = 0
        
        for chat_id, task_info in active_tasks.items():
            task_status = task_info.get('status', 'unknown')
            
            if task_status == 'running':
                running_tasks_count += 1
                task_type = task_info.get('type', 'unknown')
                start_time = task_info.get('start_time', current_time)
                elapsed = current_time - start_time
                
                # For download tasks, use actual download progress
                if task_type == 'handle_torrent_download' and 'downloader' in task_info:
                    downloader = task_info['downloader']
                    progress = downloader.progress
                    if progress > 0:
                        total_estimated = (elapsed / progress) * 100 if progress > 0 else 0
                        remaining = max(0, total_estimated - elapsed)
                        active_task_estimates[chat_id] = remaining
                    else:
                        # Fallback to original estimation
                        active_task_estimates[chat_id] = task_info.get('estimated_duration', 1800) - elapsed
                else:
                    # For encoding tasks, use elapsed time vs estimated
                    estimated_total = task_info.get('estimated_duration', 1800)
                    progress = task_info.get('real_progress', 0)
                    if progress > 0:
                        # Use real progress if available
                        total_estimated = (elapsed / progress) * 100 if progress > 0 else estimated_total
                        remaining = max(0, total_estimated - elapsed)
                    else:
                        # Fallback to estimated time
                        remaining = max(0, estimated_total - elapsed)
                    active_task_estimates[chat_id] = remaining
        
        # Process queue items
        for idx, (priority, task_id, task_callable, task_data) in enumerate(temp_queue):
            chat_id = task_data.get('chat_id', 0)
            user_status = "👑 Sudo" if chat_id in SUDO_USERS else "💎 Premium" if await is_premium_user(chat_id) else "👤 Normal"
            
            # Use REAL progress if available, otherwise use estimation
            estimated_duration = task_data.get('estimated_duration', 0)
            
            # If this task is currently active, use real progress
            if chat_id in active_task_estimates:
                estimated_duration = active_task_estimates[chat_id]
            
            queue_list.append({
                'position': idx + 1,
                'task_id': task_id,
                'chat_id': chat_id,
                'user_status': user_status,
                'priority': priority,
                'estimated_duration': estimated_duration,
                'task_type': task_data.get('type', 'unknown')
            })
        
        # Calculate wait times ACCURATELY
        current_task_remaining = 0
        
        # For current running task, use REAL progress
        if self.current_task:
            # Find which user owns the current task
            current_chat_id = None
            for chat_id, task_info in active_tasks.items():
                if task_info.get('status') == 'running' and task_info.get('task_id') == self.current_task:
                    current_chat_id = chat_id
                    break
            
            if current_chat_id and current_chat_id in active_task_estimates:
                current_task_remaining = active_task_estimates[current_chat_id]
            else:
                # Fallback to estimated time
                for chat_id, task_info in active_tasks.items():
                    if task_info.get('status') == 'running':
                        elapsed = current_time - task_info.get('start_time', current_time)
                        estimated_total = task_info.get('estimated_duration', 1800)
                        current_task_remaining = max(0, estimated_total - elapsed)
                        break
        
        # Calculate queue wait time based on ACTUAL remaining times
        queue_wait_time = current_task_remaining
        
        for task in queue_list:
            # Use real progress if available, otherwise use estimation
            chat_id = task['chat_id']
            if chat_id in active_task_estimates:
                queue_wait_time += active_task_estimates[chat_id]
            else:
                queue_wait_time += task['estimated_duration']
        
        return {
            'current_task': self.current_task,
            'current_task_remaining': current_task_remaining,
            'queue_size': self.queue.qsize(),
            'queue_wait_time': queue_wait_time,
            'queue_list': queue_list,
            'active_tasks_count': running_tasks_count,  # Only count running tasks
            'all_tasks_count': len(active_tasks),       # Count all tasks
            'active_task_estimates': active_task_estimates
        }
    async def record_task_performance(self, task_data: dict, success: bool):
        """Record task performance for future estimations"""
        if not success or task_performance_db is None:
            return
        try:
            duration = time.time() - task_data.get('added_time', time.time())
            performance_record = {
                'task_type': task_data.get('type'),
                'quality': task_data.get('quality'),
                'codec': task_data.get('codec'),
                'preset': task_data.get('preset'),
                'file_size': task_data.get('file_size', 0),
                'duration': duration,
                'success': success,
                'timestamp': datetime.now().isoformat(),
                'system_specs': self.system_specs
            }
            await task_performance_db.insert_one(performance_record)
            # Update local performance data
            key = f"{task_data.get('type')}_{task_data.get('quality')}_{task_data.get('codec')}_{task_data.get('preset')}"
            if key not in self.performance_data:
                self.performance_data[key] = []
            self.performance_data[key].append(duration)
            # Keep only last 10 records
            if len(self.performance_data[key]) > 10:
                self.performance_data[key] = self.performance_data[key][-10:]
        except Exception as e:
            logger.warning(f"Failed to record task performance: {str(e)}")
global_queue_manager = EnhancedTaskQueue()
class EnhancedTaskQueueWrapper:
    """Wrapper for backward compatibility with existing function names - FIXED VERSION"""
    def __init__(self, max_concurrent=1):
        self.max_concurrent = max_concurrent
        self.active_tasks = set()
    async def add_task(self, chat_id, task_func, *args, **kwargs):
        """Add task with enhanced priority system - FIXED VERSION with proper argument handling"""
        # Calculate file_size for estimation if not provided
        file_size = kwargs.get('file_size', 0)
        if file_size == 0:
            # Try to get file size from file_path if available
            file_path = kwargs.get('file_path') or (args[1] if len(args) > 1 else None)
            if file_path and isinstance(file_path, str) and ospath.exists(file_path):
                try:
                    file_size = ospath.getsize(file_path)
                except:
                    file_size = 0
        # Add file_size to kwargs for queue estimation (will be removed before calling function)
        kwargs['file_size'] = file_size
        # 🎯 FIX: Ensure proper arguments for specific functions
        if task_func.__name__ == 'handle_download_and_process':
            # For handle_download_and_process, ensure magnet_link is properly passed
            if 'magnet_link' not in kwargs and len(args) < 2:
                # Try to get magnet_link from session if not provided
                session = await get_user_settings(chat_id)
                magnet_link = session.get('magnet_link')
                if magnet_link:
                    # Add magnet_link as second positional argument
                    args = (chat_id, magnet_link) + args[1:] if args else (chat_id, magnet_link)
        # Add task_id to kwargs if not present
        if 'task_id' not in kwargs:
            kwargs['task_id'] = str(uuid.uuid4())[:8]
        # Remove chat_id from kwargs to avoid duplication (it's already the first parameter)
        if 'chat_id' in kwargs:
            del kwargs['chat_id']
        # Call the global queue manager
        return await global_queue_manager.add_task(chat_id, task_func, *args, **kwargs)
    async def cancel_task(self, task_id):
        """Cancel a specific task"""
        cancelled_tasks.add(task_id)
        logger.info(f"Task {task_id} marked for cancellation")
# Initialize task queue
task_queue = EnhancedTaskQueueWrapper(max_concurrent=1)
class UserSessionManager:
    """Enhanced user session management with expiration"""
    def __init__(self):
        self.sessions = {}
        self.lock = asyncio.Lock()
    async def get_session(self, chat_id):
        async with self.lock:
            if chat_id not in self.sessions:
                self.sessions[chat_id] = {
                    'created_at': time.time(),
                    'last_activity': time.time(),
                    'state': 'idle',
                    'data': {},
                    'message_history': []
                }
            self.sessions[chat_id]['last_activity'] = time.time()
            return self.sessions[chat_id]
    async def update_session(self, chat_id, updates):
        async with self.lock:
            if chat_id in self.sessions:
                self.sessions[chat_id].update(updates)
                self.sessions[chat_id]['last_activity'] = time.time()
    async def cleanup_sessions(self, max_age=3600):
        """Cleanup old sessions"""
        async with self.lock:
            current_time = time.time()
            expired_sessions = [
                chat_id for chat_id, session in self.sessions.items()
                if current_time - session['last_activity'] > max_age
            ]
            for chat_id in expired_sessions:
                del self.sessions[chat_id]
async def cleanup_old_cancellation_events():
    """Clean up cancellation events for users with no active tasks"""
    while True:
        await asyncio.sleep(3600)  # Run every hour
        try:
            current_time = time.time()
            users_to_remove = []
            for chat_id, event in user_cancellation_events.items():
                # Remove events for users with no active tasks
                if (chat_id not in active_tasks and 
                    not event.is_set()):  # Only remove if not currently cancelling
                    users_to_remove.append(chat_id)
            for chat_id in users_to_remove:
                del user_cancellation_events[chat_id]
                logger.debug(f"🧹 Cleaned up cancellation event for user {chat_id}")
        except Exception as e:
            logger.error(f"Error cleaning up cancellation events: {str(e)}")
# Start this in your main() function:
    async def add_message_to_history(self, chat_id, message_id):
        """Add message to user's message history"""
        async with self.lock:
            if chat_id in self.sessions:
                self.sessions[chat_id]['message_history'].append(message_id)
                # Keep only last 20 messages
                if len(self.sessions[chat_id]['message_history']) > 20:
                    self.sessions[chat_id]['message_history'] = self.sessions[chat_id]['message_history'][-20:]
    async def cleanup_message_history(self, chat_id):
        """Cleanup message history for a user"""
        async with self.lock:
            if chat_id in self.sessions:
                try:
                    for msg_id in self.sessions[chat_id]['message_history']:
                        try:
                            await bot.delete_messages(chat_id, msg_id)
                        except Exception:
                            pass
                    self.sessions[chat_id]['message_history'] = []
                except Exception as e:
                    logger.error(f"Failed to cleanup message history: {str(e)}")
# Initialize session manager
session_manager = UserSessionManager()
async def send_error_report(chat_id, error, context=None):
    """Send detailed error reports - FIXED: Handle unsupported types"""
    error_id = str(uuid.uuid4())[:8]
    error_time = datetime.now().isoformat()
    error_message = (
        f"⚠️ **Error Report** `[{error_id}]`\n\n"
        f"**Error:** `{type(error).__name__}`\n"
        f"**Message:** {str(error)[:200]}...\n"
        f"**Time:** {error_time}\n"
    )
    if context:
        error_message += f"**Context:** {context}\n"
    logger.error(f"Error {error_id}: {str(error)}", exc_info=True)
    # Store error in database with proper error handling
    try:
        if error_logs_db is not None:
            # 🎯 FIX: Convert all values to strings to avoid encoding issues
            await error_logs_db.insert_one({
                "error_id": error_id,
                "chat_id": str(chat_id),  # Convert to string
                "error_type": str(type(error).__name__),
                "error_message": str(error)[:500],  # Limit length
                "context": str(context) if context else "None",
                "timestamp": error_time,
                "resolved": False
            })
    except Exception as e:
        logger.error(f"Failed to log error to database: {str(e)}")
    try:
        await bot.send_message(chat_id, error_message)
    except Exception as e:
        logger.error(f"Failed to send error message: {str(e)}")
def handle_floodwait(func=None, *, max_retries=3):
    def decorator(inner_func):
        @wraps(inner_func)
        async def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return await inner_func(*args, **kwargs)
                except FloodWait as e:
                    logger.warning(f"FloodWait attempt {attempt + 1}/{max_retries}: Sleeping for {e.value} seconds")
                    await asyncio.sleep(e.value)
                    last_exception = e
                except MessageNotModified:
                    return None
                except Exception as e:
                    logger.error(f"Error in {inner_func.__name__}: {str(e)}")
                    last_exception = e
                    break
            chat_id = kwargs.get("chat_id", args[0] if args else None)
            if last_exception and chat_id:
                await send_error_report(chat_id, last_exception, f"{inner_func.__name__} after {max_retries} attempts")
            raise last_exception
        return wrapper
    if func:
        return decorator(func)
    return decorator
async def get_user_settings(chat_id: int) -> Dict:
    """Get user settings from MongoDB or fallback to memory - UPDATED"""
    try:
        if user_settings is not None:
            settings = await user_settings.find_one({"chat_id": chat_id})
            if settings:
                return settings
    except Exception as e:
        logger.error(f"Failed to get user settings from DB: {str(e)}")
    # Fallback to session manager
    session = await session_manager.get_session(chat_id)
    return {
        "chat_id": chat_id,
        "quality": "all_qualities",  # 🎯 Force All Qualities
        "all_qualities": True,      # 🎯 Enable All Qualities flag
        "selected_qualities": ["480p"],  # 🎯 Default qualities        "codec": "h264",
        "upload_mode": "video",
        "metadata": {"title": ""},
        "status": "idle",
        "created_at": time.time(),
        "last_update": time.time(),
        "subtitle_mode": "keep",
        "audio_mode": "keep",
        "samples_enabled": False,
        "screenshots_enabled": False,
        "custom_crf": None,
        "watermark_enabled": False,
        "preset": "medium",
        "video_tune": "none",
        "cancelled": False,
        **session.get('data', {})
    }
async def cleanup_settings_messages(chat_id: int):
    """Clean only settings messages, keeping progress updates"""
    try:
        if chat_id in user_active_messages:
            try:
                await bot.delete_messages(chat_id, user_active_messages[chat_id])
                del user_active_messages[chat_id]
                logger.info(f"✅ Settings menu closed for {chat_id}")
            except Exception as e:
                logger.warning(f"Failed to delete settings message: {str(e)}")
    except Exception as e:
        logger.error(f"Cleanup settings error: {str(e)}")
async def cleanup_user_messages(chat_id: int):
    """Cleanup all messages in the queue for a user"""
    if chat_id not in message_cleanup_queue:
        return
    try:
        for msg_id in message_cleanup_queue[chat_id]:
            try:
                await bot.delete_messages(chat_id, msg_id)
            except Exception as e:
                logger.warning(f"Failed to delete message {msg_id}: {str(e)}")
        message_cleanup_queue[chat_id] = []
    except Exception as e:
        logger.error(f"Cleanup messages failed: {str(e)}")
async def add_to_cleanup_queue(chat_id: int, message_id: int):
    """Add a message to the cleanup queue"""
    message_cleanup_queue[chat_id].append(message_id)
# Progress bar system
def get_random_progress_bar(progress: float, length: int = 12) -> str:
    """Get random progress bar style"""
    filled_length = int(progress / 100 * length)
    empty_length = length - filled_length
    # Your favorite styles
    styles = [
        ("█", "░"),    # Classic Blocks
        ("●", "○"),    # Animated Dots  
       # ("▰", "▱"),    # Percentage Bar
        ("▓", "▒"),    # Gradient Blocks
        ("▣", "☐"),    # Creative Unicode
        ("◆", "◇"),    # Geometric Shapes
       # ("|", "."),    # Battery Style
       # ("█", "─"),    # Music Equalizer
    ]
    filled_char, empty_char = random.choice(styles)
    bar = filled_char * filled_length + empty_char * empty_length
    return f"[{bar}]"
async def handle_crf_input(chat_id: int, text: str, message: Message):
    """Handle CRF value input from user - FIXED DATA TYPE"""
    try:
        # Clear awaiting state first
        await update_user_settings(chat_id, {"awaiting": None})
        
        # Handle skip/cancel
        if text.lower() in ['skip', 'cancel', 'auto', 'default']:
            await update_user_settings(chat_id, {"custom_crf": None})
            await message.reply("✅ Using Auto CRF (recommended)")
            await collect_settings(chat_id)
            return
            
        # Validate CRF value
        try:
            crf_value = int(text)  # Store as integer
            if crf_value < 0 or crf_value > 51:
                await message.reply(
                    "❌ CRF value must be between 0-51\n\n"
                    "🔹 0 = Lossless (huge files)\n"
                    "🔹 18-22 = High Quality\n" 
                    "🔹 23-26 = Good Balance (Recommended)\n"
                    "🔹 27-30 = Smaller Files\n"
                    "🔹 31-51 = Low Quality\n\n"
                    "Please enter a valid CRF value (0-51) or type 'skip' for Auto CRF:"
                )
                # Reset awaiting state
                await update_user_settings(chat_id, {"awaiting": "crf"})
                return
                
        except ValueError:
            await message.reply(
                "❌ Please enter a valid number for CRF (0-51)\n\n"
                "Examples:\n"
                "• 23 - Good quality\n"
                "• 26 - Balanced\n" 
                "• 28 - Smaller files\n\n"
                "Or type 'skip' for Auto CRF:"
            )
            # Reset awaiting state
            await update_user_settings(chat_id, {"awaiting": "crf"})
            return
        
        # Valid CRF value - store as integer
        await update_user_settings(chat_id, {"custom_crf": crf_value})
        await message.reply(f"✅ CRF set to {crf_value}")
        await collect_settings(chat_id)
        
    except Exception as e:
        logger.error(f"CRF input handler error: {str(e)}")
        await message.reply("❌ Error setting CRF. Please try again.")
        await update_user_settings(chat_id, {"awaiting": None})
async def handle_thumbnail(chat_id: int, message: Message) -> Optional[str]:
    """Handle thumbnail upload and processing"""
    try:
        folder = "thumbnails"
        makedirs(folder, exist_ok=True)
        if message.photo:
            file_path = ospath.join(folder, f"thumb_{chat_id}.jpg")
            await message.download(file_path)
            # Convert to JPEG if needed and resize
            try:
                with Image.open(file_path) as img:
                    img = img.convert("RGB")
                    img.thumbnail((320, 320))
                    img.save(file_path, "JPEG", quality=85)
            except Exception as e:
                logger.warning(f"Thumbnail processing error: {str(e)}")
            await update_user_settings(chat_id, {"thumbnail": file_path})
            return file_path
    except Exception as e:
        logger.error(f"Thumbnail handling failed: {str(e)}")
    return None
async def handle_watermark(chat_id: int, message: Message) -> Optional[str]:
    """Handle watermark upload and processing"""
    try:
        folder = "watermarks"
        makedirs(folder, exist_ok=True)
        file_path = ospath.join(folder, f"wm_{chat_id}.png")
        if message.photo or (message.document and message.document.mime_type == "image/png"):
            await message.download(file_path)
            # Ensure it's a transparent PNG
            try:
                with Image.open(file_path) as img:
                    if img.mode != 'RGBA':
                        img = img.convert("RGBA")
                    img.save(file_path, "PNG")
            except Exception as e:
                logger.warning(f"Watermark processing error: {str(e)}")
            await update_user_settings(chat_id, {"watermark": file_path, "watermark_enabled": True})
            return file_path
    except Exception as e:
        logger.error(f"Watermark handling failed: {str(e)}")
    return None
async def handle_subtitle(chat_id: int, message: Message) -> Optional[str]:
    """Handle subtitle file upload with better validation"""
    try:
        folder = "subtitles"
        makedirs(folder, exist_ok=True)
        if message.document and message.document.mime_type in ["application/x-subrip", "text/plain", "text/x-ssa", "application/x-ass"]:
            file_path = ospath.join(folder, f"sub_{chat_id}_{int(time.time())}")
            await message.download(file_path)
            # Validate subtitle file by checking if it's readable
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read(2048)  # Read more content for better detection
                # Check for common subtitle formats
                is_valid = any([
                    '[Script Info]' in content,  # ASS/SSA
                    'WEBVTT' in content,        # WebVTT
                    '-->' in content,           # SRT and similar
                    'Dialogue:' in content      # ASS
                ])
                if not is_valid:
                    osremove(file_path)
                    await message.reply("❌ Invalid subtitle file format. Please upload SRT, ASS, or VTT format.")
                    return None
            except UnicodeDecodeError:
                # Try different encodings
                try:
                    with open(file_path, 'r', encoding='latin-1') as f:
                        content = f.read(2048)
                    # Same validation checks...
                    is_valid = any([
                        '[Script Info]' in content,
                        'WEBVTT' in content,
                        '-->' in content,
                        'Dialogue:' in content
                    ])
                    if not is_valid:
                        osremove(file_path)
                        await message.reply("❌ Invalid subtitle file format or encoding.")
                        return None
                except:
                    osremove(file_path)
                    await message.reply("❌ Could not read subtitle file. Please check the encoding.")
                    return None
            except Exception as e:
                osremove(file_path)
                await message.reply(f"❌ Error validating subtitle file: {str(e)}")
                return None
            await update_user_settings(chat_id, {"subtitle_file": file_path})
            await message.reply("✅ Subtitle file saved successfully!")
            return file_path
    except Exception as e:
        logger.error(f"Subtitle handling failed: {str(e)}")
        await message.reply("❌ Failed to process subtitle file.")
    return None
async def handle_audio(chat_id: int, message: Message) -> Optional[str]:
    """Handle audio file upload with better validation"""
    try:
        folder = "audio"
        makedirs(folder, exist_ok=True)
        valid_audio_types = ['audio/mpeg', 'audio/mp3', 'audio/wav', 'audio/flac', 'audio/aac', 'audio/x-wav']
        if message.document and (message.document.mime_type.startswith('audio/') or message.document.mime_type in valid_audio_types):
            file_path = ospath.join(folder, f"audio_{chat_id}_{int(time.time())}")
            await message.download(file_path)
            # Basic file validation
            if not ospath.exists(file_path) or ospath.getsize(file_path) == 0:
                osremove(file_path)
                await message.reply("❌ Downloaded audio file is empty or corrupted.")
                return None
            # Try to validate with ffprobe
            try:
                cmd = ['ffprobe', '-v', 'error', '-select_streams', 'a:0', '-show_entries', 'stream=codec_name', '-of', 'csv=p=0', file_path]
                process = await asyncio.create_subprocess_exec(*cmd, stdout=PIPE, stderr=PIPE)
                stdout, stderr = await process.communicate()
                if process.returncode != 0:
                    osremove(file_path)
                    await message.reply("❌ Invalid audio file format. Please upload MP3, WAV, FLAC, or AAC.")
                    return None
            except Exception as e:
                logger.warning(f"Audio validation warning: {str(e)}")
                # Continue anyway, as the file might still be valid
            await update_user_settings(chat_id, {"audio_file": file_path})
            await message.reply("✅ Audio file saved successfully!")
            return file_path
    except Exception as e:
        logger.error(f"Audio handling failed: {str(e)}")
        await message.reply("❌ Failed to process audio file.")
    return None
async def safe_edit_message(chat_id: int, message_id: int, text: str, reply_markup=None) -> bool:
    """Safely edit a message with error handling"""
    try:
        if message_id and chat_id:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=text,
                reply_markup=reply_markup
            )
            return True
    except MessageNotModified:
        return True
    except FloodWait as e:
        await asyncio.sleep(e.value)
        return await safe_edit_message(chat_id, message_id, text, reply_markup)
    except Exception as e:
        logger.warning(f"Failed to edit message {message_id}: {str(e)}")
        return False
    return False
async def safe_delete_message(chat_id: int, message_id: int) -> bool:
    """Safely delete a message with error handling"""
    try:
        if message_id and chat_id:
            await bot.delete_messages(chat_id, message_id)
            return True
    except Exception as e:
        logger.warning(f"Failed to delete message {message_id}: {str(e)}")
        return False
    return False
async def cleanup_progress_messages(chat_id: int):
    """Clean up progress messages for a chat - FIXED VERSION"""
    try:
        # Clean up upload progress messages
        if hasattr(upload_file_with_progress, 'upload_messages'):
            if chat_id in upload_file_with_progress.upload_messages:
                try:
                    msg_obj = upload_file_with_progress.upload_messages[chat_id]
                    if hasattr(msg_obj, 'delete'):
                        await msg_obj.delete()
                    logger.info(f"🧹 Deleted upload progress message for {chat_id}")
                except Exception as e:
                    logger.warning(f"Failed to delete upload progress message: {str(e)}")
                finally:
                    # Remove from tracking
                    if chat_id in upload_file_with_progress.upload_messages:
                        del upload_file_with_progress.upload_messages[chat_id]
        # Clean up general progress messages - FIXED: Use message object
        if chat_id in progress_message_tracker:
            try:
                message_data = progress_message_tracker[chat_id]
                if 'message' in message_data and hasattr(message_data['message'], 'delete'):
                    await message_data['message'].delete()
                    logger.info(f"🧹 Deleted progress message for {chat_id}")
            except Exception as e:
                logger.warning(f"Failed to delete progress message: {str(e)}")
            finally:
                # Remove from tracking
                if chat_id in progress_message_tracker:
                    del progress_message_tracker[chat_id]
        # Clean up user active messages
        if chat_id in user_active_messages:
            try:
                msg_id = user_active_messages[chat_id]
                await bot.delete_messages(chat_id, msg_id)
                logger.info(f"🧹 Deleted user active message for {chat_id}")
            except Exception as e:
                logger.warning(f"Failed to delete user active message: {str(e)}")
            finally:
                if chat_id in user_active_messages:
                    del user_active_messages[chat_id]
        logger.info(f"🧹 Cleaned up all progress messages for chat {chat_id}")
    except Exception as e:
        logger.error(f"Error cleaning progress messages: {str(e)}")

### filename ---
# === Your Season/Episode Patterns ===
season_episode_patterns = [
    r'(?i)\bS(?:eason)?\s*0*(\d{1,2})\s*[ ._-]*EP?\.?\s*0*(\d{1,3})\b(?!v\d)',
    r'(?i)\bS0*(\d{1,2})E0*(\d{1,3})\b(?!v\d)',
    r'(?i)\b(\d{1,2})x(\d{1,3})\b',
    r'(?i)\bS0*(\d{1,2})\s*EP0*(\d{1,3})\b(?!v\d)',
    r'\[S\d+\s*[-~]\s*E(\d+)\]',
    r'\bS\d+\s*[-~]\s*E(\d+)\b'
]

episode_only_patterns = [
    r'(?i)\bEP\.?\s*0*(\d{1,3})\b',
    r'(?i)\b[Ee](?:pisode)?\.?\s*0*(\d{1,3})\b',
    r'(?i)\b[Ee]0*(\d{1,3})\b'
]

async def detect_audio_info(file_path: str):
    """Detects number/type of audio and subtitle streams using ffprobe."""
    ffprobe = shutil.which('ffprobe')
    if not ffprobe:
        raise RuntimeError("⚠️ ffprobe not found in PATH")

    cmd = [
        ffprobe, '-v', 'quiet', '-print_format', 'json', 
        '-show_streams', '-show_format', file_path
    ]

    process = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    stdout, stderr = await process.communicate()

    try:
        info = json.loads(stdout)
        streams = info.get('streams', [])
        audio_streams = [s for s in streams if s.get('codec_type') == 'audio']
        sub_streams = [s for s in streams if s.get('codec_type') == 'subtitle']

        # More comprehensive language detection
        jp_audio = 0
        en_audio = 0
        other_audio = 0
        
        for audio in audio_streams:
            lang = audio.get('tags', {}).get('language', '').lower()
            if lang in {'ja', 'jpn', 'japanese', 'jp'}:
                jp_audio += 1
            elif lang in {'en', 'eng', 'english'}:
                en_audio += 1
            else:
                other_audio += 1
        
        # Count English subtitles
        en_subs = 0
        for sub in sub_streams:
            lang = sub.get('tags', {}).get('language', '').lower()
            if lang in {'en', 'eng', 'english'}:
                en_subs += 1

        return len(audio_streams), len(sub_streams), jp_audio, en_audio, en_subs, other_audio
    except Exception as e:
        print(f"⚠️ FFprobe error: {e}")
        return 0, 0, 0, 0, 0, 0
def get_audio_label(audio_info):
    """Return audio tag label like [Sub], [Dub], [Dual], [Multi]."""
    total_audio, _, jp_audio, en_audio, en_subs, other_audio = audio_info

    # 1 audio case
    if total_audio == 1:
        if jp_audio >= 1 and en_subs >= 1:
            return "[Sub]"
        if en_audio >= 1:
            return "[Dub]"
        return None  # single unknown audio = no label

    # 2 audio case
    if total_audio == 2:
        return "[Dual]"

    # >=3 or multiple mixed languages
    if total_audio >= 3 or other_audio > 0:
        return "[Multi]"

    return None
async def ensure_quality_tag_in_filename(file_path: str, quality: str) -> str:
    """
    Clean and rename anime filenames using detected episode, season, and ffprobe audio info.
    """
    try:
        if not ospath.exists(file_path):
            logger.error(f"File not found: {file_path}")
            return file_path
            
        directory = ospath.dirname(file_path)
        filename = ospath.basename(file_path)
        name, ext = ospath.splitext(filename)

        logger.info(f"🎯 Starting filename processing: {filename}")

        # 1️⃣ Remove known uploader/source tags
        name = re.sub(r'\[[^\]]*?(EMBER|SubsPlease|Erai\-raws|@[\w\d_]+)[^\]]*?\]', '', name, flags=re.IGNORECASE)
        name = re.sub(r'@[\w\d_]+', '', name)

        # 2️⃣ Extract season + episode FIRST before cleaning
        season, episode = None, None
        for pattern in season_episode_patterns:
            match = re.search(pattern, name)
            if match:
                try:
                    season = int(match.group(1))
                    episode = int(match.group(2))
                    logger.info(f"📺 Found season {season}, episode {episode}")
                    break
                except (IndexError, ValueError):
                    continue

        if not episode:
            for pattern in episode_only_patterns:
                match = re.search(pattern, name)
                if match:
                    try:
                        episode = int(match.group(1))
                        logger.info(f"📺 Found episode {episode}")
                        break
                    except (IndexError, ValueError):
                        continue

        # 3️⃣ Now clean the name while preserving the actual title
        # Remove brackets, quality, codec tags but keep the main title
        original_name = name  # Keep original for title extraction
        
        # Remove quality tags
        name = re.sub(r'\[.*?\]', '', name)
        name = re.sub(r'\b\d{3,4}p\b', '', name, flags=re.IGNORECASE)
        name = re.sub(r'\b(HEVC|WEB\-DL|WEBRip|x265|x264|AV1|AAC|H\.?264)\b', '', name, flags=re.IGNORECASE)
        name = re.sub(r'\([^)]*(Season|Episode)[^)]*\)', '', name, flags=re.IGNORECASE)

        # 4️⃣ Extract the actual title - be more careful with this
        title = re.sub(r'[_\s]+', ' ', name).strip()
        title = re.sub(r'\s+', ' ', title)  # Remove extra spaces
        
        # If title is too short or doesn't make sense, try to extract from original
        if len(title) < 3 or any(word in title.lower() for word in ['temp', 'unknown']):
            # Try to extract from original name before cleaning
            title_match = re.search(r'([A-Za-z][A-Za-z\s\']+?)(?:\s*[\[\(]|\s*\d|$)', original_name)
            if title_match:
                title = title_match.group(1).strip()
        
        # Final title cleanup
        title = re.sub(r'^\s+|\s+$', '', title)
        if not title or title.lower().startswith('temp'):
            title = "Anime"  # Fallback title

        # 5️⃣ Detect audio streams
        audio_info = await detect_audio_info(file_path)
        audio_tag = get_audio_label(audio_info)

        # 6️⃣ Construct tags
        tags = []
        if season and episode:
            tags.append(f"[S{season:02d}E{episode:02d}]")
        elif episode:
            tags.append(f"[E{episode:02d}]")
        
        if audio_tag:
            tags.append(audio_tag)
        
        # Add quality tag (always)
        quality_tag = quality.upper() if quality != "noencode" else "NOENCODE"
        tags.append(f"[{quality_tag}]")

        # 7️⃣ Build final filename
        new_filename = f"{title} {' '.join(tags)}{ext}"
        new_filename = re.sub(r'\s+', ' ', new_filename).strip()
        new_path = ospath.join(directory, new_filename)

        # 8️⃣ Rename file if different
        if new_path != file_path:
            # Ensure we don't overwrite existing files
            counter = 1
            base_new_path = new_path
            while ospath.exists(new_path):
                name_part, ext_part = ospath.splitext(base_new_path)
                new_path = f"{name_part}_{counter:02d}{ext_part}"
                counter += 1
                
            os.rename(file_path, new_path)
            logger.info(f"✅ Renamed: {ospath.basename(file_path)} → {ospath.basename(new_path)}")
            return new_path
        
        logger.info(f"ℹ️  File name unchanged: {filename}")
        return file_path

    except Exception as e:
        logger.error(f"⚠️ Failed to rename {file_path}: {e}")
        # Return original path if renaming fails
        return file_path

class SimpleAria2Downloader:
    def __init__(self, magnet_link: str, download_dir: str):
        self.magnet_link = magnet_link
        self.download_dir = download_dir
        self._stop_event = asyncio.Event()
        self.progress = 0
        self.download_speed = 0
        self.total_size = 0
        self.downloaded_size = 0
        self.peers = 0
        self.eta = 0
        self.process = None
        self.status = "starting"
        
    async def start_download(self):
        """Start download using aria2c command line - IMPROVED ERROR HANDLING"""
        try:
            # Create download directory if it doesn't exist
            makedirs(self.download_dir, exist_ok=True)
            
            # Enhanced aria2c command with better error handling
            cmd = [
                'aria2c',
                self.magnet_link,
                '--dir=' + self.download_dir,
                '--seed-time=0',
                '--max-upload-limit=1K',
                '--max-connection-per-server=16',
                '--split=16',
                '--min-split-size=1M',
                '--file-allocation=prealloc',
                '--summary-interval=1',
                '--bt-max-peers=50',
                '--enable-dht=true',
                '--enable-peer-exchange=true',
                '--user-agent=aria2/1.0',
                '--check-certificate=false',
                '--auto-file-renaming=false',
                '--allow-overwrite=true',
                '--max-tries=5',  # Increased from 3
                '--retry-wait=15',  # Increased from 10
                '--timeout=120',  # Increased from 60
                '--max-file-not-found=0',
                '--bt-tracker-connect-timeout=60',  # Increased from 30
                '--bt-tracker-timeout=60',  # Increased from 30
                '--bt-stop-timeout=180',  # Increased from 120
                '--continue=true',  # Add continue support
                '--max-resume-failure-tries=3',  # Add resume failure handling
                '--remote-time=true',  # Get timestamp from server
                '--console-log-level=warn',  # Reduce log noise
            ]
            
            logger.info(f"Starting aria2c: {' '.join(cmd)}")
            
            self.process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
           
            self.status = "downloading"
            
            # Monitor output with better error handling
            await self._monitor_output_with_cancellation()
            
            # Wait for process to complete
            returncode = await self.process.wait()
            
            # Check cancellation after process ends
            if self._stop_event.is_set():
                self.status = "cancelled"
                raise TaskCancelled("Download stopped by user")
            elif returncode == 0:
                self.status = "completed"
                logger.info("Download completed successfully")
                return self.download_dir
            else:
                # Get stderr for better error reporting
                stderr_output = []
                if self.process.stderr:
                    stderr_data = await self.process.stderr.read()
                    stderr_output = stderr_data.decode().splitlines()
                
                error_msg = f"aria2c failed with return code: {returncode}"
                if stderr_output:
                    error_msg += f"\nError output: {' '.join(stderr_output[-3:])}"  # Last 3 lines
                
                self.status = "error"
                raise RuntimeError(error_msg)
                
        except Exception as e:
            self.status = "error"
            logger.error(f"Aria2c download error: {str(e)}")
            await self._cleanup()
            raise RuntimeError(f"Download failed: {str(e)}")
    async def _monitor_output_with_cancellation(self):
        """Monitor aria2c output with cancellation support"""
        try:
            if not self.process.stdout:
                return
            while not self._stop_event.is_set():
                # 🎯 FIXED: Use asyncio.wait_for to check cancellation frequently
                try:
                    line = await asyncio.wait_for(
                        self.process.stdout.readline(), 
                        timeout=1.0  # Check every second for cancellation
                    )
                    if not line:
                        break
                    line_str = line.decode().strip()
                    logger.debug(f"aria2c: {line_str}")
                    await self._parse_output_line(line_str)
                except asyncio.TimeoutError:
                    # Timeout is expected, just continue to check cancellation
                    continue
        except Exception as e:
            logger.warning(f"Error monitoring output: {str(e)}")
    async def _parse_output_line(self, line: str):
        """Parse aria2c output line for progress information"""
        try:
            # Parse progress from summary lines like:
            # [#1 NAME 1.2GiB/2.4GiB(50%) CN:10 SD:0 DL:1.5MiB ETA:5m30s]
            if line.startswith('[') and 'ETA:' in line:
                # Extract percentage
                percent_match = re.search(r'\((\d+)%\)', line)
                if percent_match:
                    self.progress = float(percent_match.group(1))
                    logger.debug(f"Progress: {self.progress}%")
                # Extract download speed
                dl_match = re.search(r'DL:([\d.]+)([KM]?iB)', line)
                if dl_match:
                    speed = float(dl_match.group(1))
                    unit = dl_match.group(2)
                    # Convert to bytes
                    if unit == 'KiB':
                        self.download_speed = speed * 1024
                    elif unit == 'MiB':
                        self.download_speed = speed * 1024 * 1024
                    else:
                        self.download_speed = speed
                # Extract peers
                peers_match = re.search(r'CN:(\d+)', line)
                if peers_match:
                    self.peers = int(peers_match.group(1))
                # Extract ETA
                eta_match = re.search(r'ETA:(\d+h)?(\d+m)?(\d+s)?', line)
                if eta_match:
                    hours = eta_match.group(1) or '0h'
                    minutes = eta_match.group(2) or '0m' 
                    seconds = eta_match.group(3) or '0s'
                    # Convert to seconds
                    self.eta = (int(hours[:-1]) * 3600 + 
                               int(minutes[:-1]) * 60 + 
                               int(seconds[:-1]))
                # Extract total size from pattern like: 1.2GiB/2.4GiB
                size_match = re.search(r'([\d.]+[KMGT]iB)/([\d.]+[KMGT]iB)', line)
                if size_match:
                    total_size_str = size_match.group(2)
                    # Convert size string to bytes
                    self.total_size = self._parse_size(total_size_str)
        except Exception as e:
            logger.warning(f"Error parsing output line: {str(e)}")
    def _parse_size(self, size_str: str) -> int:
        """Parse size string like '1.5GiB' to bytes"""
        try:
            size_match = re.match(r'([\d.]+)([KMGT]iB)', size_str)
            if size_match:
                size = float(size_match.group(1))
                unit = size_match.group(2)
                if unit == 'KiB':
                    return int(size * 1024)
                elif unit == 'MiB':
                    return int(size * 1024 * 1024)
                elif unit == 'GiB':
                    return int(size * 1024 * 1024 * 1024)
                elif unit == 'TiB':
                    return int(size * 1024 * 1024 * 1024 * 1024)
                else:
                    return int(size)  # Bytes
            return 0
        except:
            return 0
    async def _cleanup(self):
        """Cleanup process"""
        if self.process:
            try:
                self.process.terminate()
                await asyncio.sleep(1)
                if self.process.returncode is None:
                    self.process.kill()
                    await self.process.wait()
            except Exception as e:
                logger.warning(f"Error stopping aria2c: {str(e)}")
    def stop_download(self):
        """Stop download - FIXED"""
        self._stop_event.set()
        self.status = "cancelled"
        if self.process:
            try:
                self.process.terminate()
                logger.info("🛑 Aria2c process terminated due to cancellation")
            except Exception as e:
                logger.warning(f"Error terminating aria2c process: {str(e)}")
    def get_progress(self):
        return self.progress, self.download_speed, 0, self.eta
    def get_peers(self):
        return self.peers
    def get_total_size(self):
        return self.total_size
async def extract_subtitles(input_path: str, output_dir: str, chat_id: int) -> List[str]:
    """Extract all subtitle tracks from video file"""
    try:
        subtitle_files = []
        
        # Get subtitle stream info
        cmd_info = [
            'ffprobe', '-v', 'error',
            '-select_streams', 's',
            '-show_entries', 'stream=index,codec_name:stream_tags=language,title',
            '-of', 'json',
            input_path
        ]
        
        process = await asyncio.create_subprocess_exec(*cmd_info, stdout=PIPE, stderr=PIPE)
        stdout, stderr = await process.communicate()
        
        if process.returncode != 0:
            logger.error(f"FFprobe subtitle info error: {stderr.decode()}")
            return []
        
        stream_info = json.loads(stdout.decode())
        subtitle_streams = stream_info.get('streams', [])
        
        if not subtitle_streams:
            logger.info("No subtitle streams found to extract")
            return []
        
        # Extract each subtitle stream
        for stream in subtitle_streams:
            stream_index = stream.get('index')
            codec_name = stream.get('codec_name', 'unknown')
            language = stream.get('tags', {}).get('language', 'unknown')
            title = stream.get('tags', {}).get('title', f'Track_{stream_index}')
            
            # Determine file extension
            ext_map = {
                'subrip': 'srt',
                'ass': 'ass', 
                'ssa': 'ssa',
                'webvtt': 'vtt',
                'mov_text': 'srt'
            }
            extension = ext_map.get(codec_name, 'srt')
            
            output_filename = f"subtitle_{stream_index}_{language}_{title}.{extension}".replace(' ', '_')
            output_path = ospath.join(output_dir, output_filename)
            
            # Extract subtitle
            cmd_extract = [
                'ffmpeg', '-hide_banner', '-loglevel', 'error',
                '-i', input_path,
                '-map', f'0:{stream_index}',
                '-c', 'copy',
                '-y', output_path
            ]
            
            process_extract = await asyncio.create_subprocess_exec(*cmd_extract)
            return_code = await process_extract.wait()
            
            if return_code == 0 and ospath.exists(output_path) and ospath.getsize(output_path) > 0:
                subtitle_files.append(output_path)
                logger.info(f"✅ Extracted subtitle: {output_filename}")
            else:
                logger.warning(f"Failed to extract subtitle stream {stream_index}")
        
        return subtitle_files
        
    except Exception as e:
        logger.error(f"Subtitle extraction error: {str(e)}")
        return []
async def process_noencode_with_subtitles_audio(
    chat_id: int, 
    file_path: str, 
    output_dir: str, 
    session: dict,
    file_name: str
) -> Optional[str]:
    """Process file in noencode mode with subtitle and audio options - FIXED VERSION"""
    try:
        logger.info(f"🎯 No-encode processing started for: {file_name}")
        
        if not session:
            session = await get_user_settings(chat_id) or {}
            
        subtitle_mode = session.get("subtitle_mode", "keep")
        audio_mode = session.get("audio_mode", "keep")
        subtitle_file = session.get("subtitle_file")
        audio_file = session.get("audio_file")
        
        logger.info(f"🎯 Settings - Subtitle: {subtitle_mode}, Audio: {audio_mode}")
        logger.info(f"🎯 Files - Subtitle: {subtitle_file}, Audio: {audio_file}")

        # If no changes needed, just return original file
        if (subtitle_mode in ["keep", "hard_existing"] and 
            audio_mode == "keep" and 
            subtitle_mode != "extract"):
            logger.info("🎯 No changes needed, returning original file")
            return file_path
        
        # Create output path
        original_name = ospath.splitext(file_name)[0]
        output_path = ospath.join(output_dir, f"noencode_{original_name}.mkv")
        
        logger.info(f"🎯 Output path: {output_path}")

        # Handle subtitle extraction only
        if subtitle_mode == "extract":
            logger.info("🎯 Extracting subtitles only")
            extracted_subs = await extract_subtitles(file_path, output_dir, chat_id)
            if extracted_subs:
                for sub_file in extracted_subs:
                    try:
                        await bot.send_document(
                            chat_id=chat_id,
                            document=sub_file,
                            caption=f"📝 Extracted subtitle: {ospath.basename(sub_file)}"
                        )
                        # Cleanup after sending
                        osremove(sub_file)
                    except Exception as e:
                        logger.warning(f"Failed to send subtitle {sub_file}: {str(e)}")
            return file_path
        
        # Build FFmpeg command for processing
        cmd = ['ffmpeg', '-hide_banner', '-loglevel', 'error', '-i', file_path]
        
        # 🎯 FIX: Check if we need video re-encoding (for hard subtitles)
        needs_video_reencoding = False
        video_filters = []
        
        # Handle subtitle modes that require video re-encoding
        if subtitle_mode == "hard_existing":
            # Burn first subtitle track - REQUIRES RE-ENCODING
            video_filters.append(f'subtitles={file_path}:si=0:force_style=\'FontName=Arial,FontSize=16,PrimaryColour=&H00FFFFFF\'')
            needs_video_reencoding = True
            logger.info("🎯 Burning existing subtitles (requires video re-encoding)")
        
        elif subtitle_mode == "hard_new" and subtitle_file and ospath.exists(subtitle_file):
            # Burn external subtitle file - REQUIRES RE-ENCODING
            video_filters.append(f'subtitles={subtitle_file}:force_style=\'FontName=Arial,FontSize=16,PrimaryColour=&H00FFFFFF\'')
            needs_video_reencoding = True
            logger.info("🎯 Burning external subtitles (requires video re-encoding)")
        
        # Handle subtitle removal (can use stream copy)
        elif subtitle_mode == "remove":
            cmd.extend(['-sn'])
            logger.info("🎯 Removing subtitles (stream copy)")
        
        # Handle audio modes
        audio_needs_processing = False
        if audio_mode == "remove":
            cmd.extend(['-an'])
            logger.info("🎯 Removing audio")
        elif audio_mode == "add" and audio_file and ospath.exists(audio_file):
            cmd.extend(['-i', audio_file])
            audio_needs_processing = True
            logger.info("🎯 Adding new audio track")
        elif audio_mode == "extract":
            # Extract audio only
            audio_output = output_path.replace('.mkv', '.m4a')
            audio_cmd = [
                'ffmpeg', '-hide_banner', '-loglevel', 'error',
                '-i', file_path,
                '-vn', '-acodec', 'copy',
                '-y', audio_output
            ]
            
            logger.info(f"🎯 Extracting audio to: {audio_output}")
            process = await asyncio.create_subprocess_exec(*audio_cmd)
            return_code = await process.wait()
            
            if return_code == 0 and ospath.exists(audio_output):
                logger.info("✅ Audio extraction successful")
                return audio_output
            else:
                logger.warning("❌ Audio extraction failed, returning original")
                return file_path
        
        # 🎯 FIXED: Apply video filters if needed (this requires re-encoding)
        if video_filters:
            cmd.extend(['-vf', ','.join(video_filters)])
            # When using video filters, we MUST re-encode video
            cmd.extend([
                '-c:v', 'libx264',  # Re-encode video
                '-preset', 'fast',   # Fast preset for quick processing
                '-crf', '23',        # Good quality
                '-pix_fmt', 'yuv420p'
            ])
        else:
            # No video filters, can use stream copy
            cmd.extend(['-c:v', 'copy'])
        
        # Handle audio streams
        if audio_needs_processing:
            # When adding audio, map video from input 0, audio from input 1
            cmd.extend([
                '-map', '0:v',
                '-map', '1:a',
                '-c:a', 'copy'  # Copy audio without re-encoding
            ])
        elif audio_mode != "remove" and not needs_video_reencoding:
            # Copy audio if we're not removing it and not re-encoding video
            cmd.extend(['-c:a', 'copy'])
        elif audio_mode != "remove" and needs_video_reencoding:
            # Re-encode audio if we're re-encoding video
            cmd.extend(['-c:a', 'aac', '-b:a', '192k'])
        
        # Copy subtitles if not removing them and not burning them
        if subtitle_mode == "keep" and not needs_video_reencoding:
            cmd.extend(['-c:s', 'copy'])
        elif subtitle_mode != "remove" and needs_video_reencoding:
            # When re-encoding video, we can still copy subtitles
            cmd.extend(['-c:s', 'copy'])
        
        cmd.extend(['-y', output_path])
        
        logger.info(f"🎯 Final FFmpeg command: {' '.join(cmd)}")
        logger.info(f"🎯 Video re-encoding: {'YES' if needs_video_reencoding else 'NO'}")
        
        # Execute processing
        process = await asyncio.create_subprocess_exec(*cmd)
        return_code = await process.wait()
        
        if return_code == 0 and ospath.exists(output_path) and ospath.getsize(output_path) > 0:
            new_size = ospath.getsize(output_path)
            logger.info(f"✅ No-encode processing successful: {new_size} bytes")
            return output_path
        else:
            # Get error details
            stderr_output = []
            if hasattr(process, 'stderr') and process.stderr:
                stderr_output = await process.stderr.read()
                logger.error(f"❌ FFmpeg error: {stderr_output.decode()}")
                
            logger.warning(f"❌ No-encode processing failed, return code: {return_code}")
            logger.warning(f"❌ Output exists: {ospath.exists(output_path)}, size: {ospath.getsize(output_path) if ospath.exists(output_path) else 0}")
            return file_path  # Fallback to original file
            
    except Exception as e:
        logger.error(f"❌ No-encode subtitle/audio processing error: {str(e)}", exc_info=True)
        return file_path  # Fallback to original file
async def check_svtav1_available() -> bool:
    """Check if SVT-AV1 encoder is available in FFmpeg"""
    try:
        cmd = ['ffmpeg', '-hide_banner', '-loglevel', 'error', '-encoders']
        process = await asyncio.create_subprocess_exec(*cmd, stdout=PIPE, stderr=PIPE)
        stdout, stderr = await process.communicate()
        
        if process.returncode == 0:
            output = stdout.decode().lower()
            return 'libsvtav1' in output
        return False
    except Exception as e:
        logger.warning(f"Error checking SVT-AV1 availability: {str(e)}")
        return False
async def validate_and_repair_video(input_path: str) -> str:
    """Validate video file and attempt to repair if corrupted"""
    try:
        encoder = VideoEncoder()
        
        # First, try to get video info to check if file is valid
        try:
            video_info = await encoder.get_video_info(input_path)
            duration = video_info.get('duration', 0)
            
            if duration > 0 and video_info.get('video_streams'):
                logger.info(f"✅ Video appears valid: {ospath.basename(input_path)}")
                return input_path  # File is valid, return original path
                
        except Exception as e:
            logger.warning(f"⚠️ Video validation failed: {str(e)}")
        
        # If we get here, the file might be corrupted - try to repair
        logger.info(f"🔄 Attempting to repair video: {ospath.basename(input_path)}")
        
        # Create repaired file path
        repaired_path = input_path.replace(ospath.splitext(input_path)[1], "_repaired.mkv")
        
        # Use FFmpeg to copy streams and fix any issues
        repair_cmd = [
            'ffmpeg', '-hide_banner', '-loglevel', 'error',
            '-err_detect', 'ignore_err',          # Ignore errors
            '-i', input_path,
            '-c', 'copy',                         # Stream copy (no re-encode)
            '-map', '0',                          # Copy all streams
            '-fflags', '+genpts',                 # Generate missing PTS
            '-max_muxing_queue_size', '1024',     # Increase muxing queue
            '-y', repaired_path
        ]
        
        process = await asyncio.create_subprocess_exec(*repair_cmd)
        return_code = await process.wait()
        
        if return_code == 0 and ospath.exists(repaired_path) and ospath.getsize(repaired_path) > 0:
            # Verify the repaired file
            try:
                repaired_info = await encoder.get_video_info(repaired_path)
                if repaired_info.get('duration', 0) > 0:
                    logger.info(f"✅ Video repaired successfully: {ospath.basename(repaired_path)}")
                    # Remove original corrupted file
                    try:
                        osremove(input_path)
                    except:
                        pass
                    return repaired_path
            except:
                pass
        
        # If repair failed, try a more aggressive approach with re-encoding
        logger.info(f"🔄 Attempting aggressive repair for: {ospath.basename(input_path)}")
        aggressive_repaired_path = input_path.replace(ospath.splitext(input_path)[1], "_reencoded.mkv")
        
        aggressive_cmd = [
            'ffmpeg', '-hide_banner', '-loglevel', 'error',
            '-err_detect', 'ignore_err',
            '-i', input_path,
            '-c:v', 'libx264',                    # Re-encode video
            '-c:a', 'aac',                        # Re-encode audio
            '-preset', 'fast',
            '-crf', '23',
            '-max_muxing_queue_size', '2048',
            '-fflags', '+genpts+igndts',
            '-avoid_negative_ts', 'make_zero',
            '-y', aggressive_repaired_path
        ]
        
        process = await asyncio.create_subprocess_exec(*aggressive_cmd)
        return_code = await process.wait()
        
        if return_code == 0 and ospath.exists(aggressive_repaired_path) and ospath.getsize(aggressive_repaired_path) > 0:
            logger.info(f"✅ Video re-encoded successfully: {ospath.basename(aggressive_repaired_path)}")
            try:
                osremove(input_path)
            except:
                pass
            return aggressive_repaired_path
        
        # If all repairs failed, return original and hope for the best
        logger.warning(f"❌ All repair attempts failed for: {ospath.basename(input_path)}")
        return input_path
        
    except Exception as e:
        logger.error(f"Video repair error: {str(e)}")
        return input_path  # Return original on error
class VideoEncoder:
    async def get_video_info(self, file_path: str) -> dict:
        """Get comprehensive video information using ffprobe"""
        try:
            cmd = [
                'ffprobe', '-v', 'error',
                '-show_entries', 
                'format=duration,bit_rate,size:stream=bit_rate,width,height,duration,codec_name,avg_frame_rate,r_frame_rate,index,codec_type,tags:format_tags=title',
                '-of', 'json',
                file_path
            ]
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=PIPE,
                stderr=PIPE
            )
            stdout, stderr = await process.communicate()
            if process.returncode != 0:
                error_msg = stderr.decode('utf-8')
                logger.error(f"FFprobe error: {error_msg}")
                raise RuntimeError(f"FFprobe error: {error_msg}")
            info = json.loads(stdout.decode('utf-8'))
            return self._parse_video_info(info)
        except Exception as e:
            logger.error(f"Error getting video info: {str(e)}")
            raise ValueError(f"Could not get video info: {str(e)}")
    def _parse_video_info(self, info: dict) -> dict:
        """Parse ffprobe output into structured data with proper frame rate calculation"""
        metadata = info.get('format', {}).get('tags', {})
        streams = info.get('streams', [])
        video_streams = [s for s in streams if s.get('codec_type') == 'video']
        audio_streams = [s for s in streams if s.get('codec_type') == 'audio']
        subtitle_streams = [s for s in streams if s.get('codec_type') == 'subtitle']
        duration = float(info.get('format', {}).get('duration', 0))
        bitrate = int(info.get('format', {}).get('bit_rate', 0))
        file_size = int(info.get('format', {}).get('size', 0))
        # Calculate frame rate properly
        fps = 0
        if video_streams:
            avg_frame_rate = video_streams[0].get('avg_frame_rate', '0/0')
            r_frame_rate = video_streams[0].get('r_frame_rate', '0/0')
            # Use average frame rate if available, otherwise use r_frame_rate
            frame_rate_str = avg_frame_rate if avg_frame_rate != '0/0' else r_frame_rate
            try:
                if '/' in frame_rate_str:
                    num, den = frame_rate_str.split('/')
                    if float(den) > 0:
                        fps = float(num) / float(den)
                else:
                    fps = float(frame_rate_str)
            except (ValueError, ZeroDivisionError):
                fps = 0
        return {
            'metadata': metadata,
            'duration': duration,
            'bitrate': bitrate,
            'file_size': file_size,
            'fps': fps,
            'video_streams': video_streams,
            'audio_streams': audio_streams,
            'subtitle_streams': subtitle_streams,
            'original_info': info
        }
    def _build_ffmpeg_command(
        self,
        input_path: str,
        quality: str,
        watermark_path: Optional[str],
        watermark_enabled: bool,
        title: str,
        output_path: str,
        codec: str = 'h264',
        enable_progress: bool = False,
        subtitle_mode: str = 'keep',
        subtitle_file: Optional[str] = None,
        audio_mode: str = 'keep',
        audio_file: Optional[str] = None,
        custom_crf: Optional[int] = None,
        preset: str = 'medium',
        video_tune: str = 'none',
        watermark_position: str = "bottom-right",
        watermark_scale: str = "10%", 
        watermark_opacity: str = "70%",
        samples_enabled: bool = False,
        screenshots_enabled: bool = False,
        remname: Optional[str] = None,
        chat_id: Optional[int] = None,
        pixel_format: str = 'yuv420p',
        profile: str = 'main'
    ) -> List[str]:
        """
        Build optimized FFmpeg command with enhanced H.265/AV1 compatibility.
        FIXED: Proper H.265 parameters and AV1 fallback handling.
        """
        
        # 🎯 COMPATIBILITY VALIDATION
        def validate_compatibility(codec_name: str, profile_name: str, pix_fmt: str) -> tuple:
            """Validate and auto-correct incompatible settings"""
            compatibility_matrix = {
                'h264': {
                    'baseline': ['yuv420p'],
                    'main': ['yuv420p'],
                    'high': ['yuv420p', 'yuv422p', 'yuv444p'],
                    'high10': ['yuv420p10le']
                },
                'h265': {
                    'main': ['yuv420p'],
                    'main10': ['yuv420p10le'],
                    'main12': ['yuv420p10le'],
                    'main444-8': ['yuv444p'],
                    'main444-10': ['yuv444p'],
                    'main444-12': ['yuv444p']
                },
                'av1': {
                    'main': ['yuv420p', 'yuv420p10le'],
                    'high': ['yuv420p', 'yuv420p10le', 'yuv422p', 'yuv444p'],
                    'professional': ['yuv420p', 'yuv420p10le', 'yuv422p', 'yuv444p']
                }
            }
            
            compatible_formats = compatibility_matrix.get(codec_name, {}).get(profile_name, [])
            
            if not compatible_formats:
                return profile_name, pix_fmt
            
            if pix_fmt not in compatible_formats:
                corrected_format = compatible_formats[0]
                logger.warning(f"⚠️ Incompatible: {profile_name} + {pix_fmt}, auto-correcting to {corrected_format}")
                return profile_name, corrected_format
            
            return profile_name, pix_fmt

        # Apply compatibility validation
        validated_profile, validated_pixel_format = validate_compatibility(codec, profile, pixel_format)
        
        # 🎯 FIX: Force MKV container for H.265/AV1 for better compatibility
        original_output = output_path
        if output_path.endswith('.mp4') and codec in ['h265', 'av1']:
            output_path = output_path.replace('.mp4', '.mkv')
            logger.info(f"🔄 Changed container to MKV for {codec.upper()} compatibility")

        # Create per-quality directory
        if quality != 'noencode':
            base_dir = os.path.dirname(output_path)
            if not base_dir.endswith(quality):
                quality_dir = os.path.join(base_dir, quality)
                os.makedirs(quality_dir, exist_ok=True)
                filename = os.path.basename(output_path)
                output_path = os.path.join(quality_dir, filename)

        # 🎯 ENHANCED QUALITY SETTINGS WITH CODEC-SPECIFIC OPTIMIZATIONS
        quality_settings = {
            '480p': {
                'height': 480,
                'crf': 29 if codec == 'h264' else (30 if codec == 'h265' else 35),
                'preset': 'medium',
                'pix_fmt': validated_pixel_format,
                'profile': validated_profile,
                'max_bitrate': '1500k',
                'bufsize': '3000k',
                'audio_bitrate': '96k'
            },
            '720p': {
                'height': 720,
                'crf': 28 if codec == 'h264' else (29 if codec == 'h265' else 33),
                'preset': 'medium',
                'pix_fmt': validated_pixel_format,
                'profile': validated_profile,
                'max_bitrate': '2500k',
                'bufsize': '5000k',
                'audio_bitrate': '128k'
            },
            '1080p': {
                'height': 1080,
                'crf': 27 if codec == 'h264' else (28 if codec == 'h265' else 31),
                'preset': 'slow' if codec in ['h264', 'h265'] else 'fast',
                'pix_fmt': validated_pixel_format,
                'profile': validated_profile,
                'max_bitrate': '4500k',
                'bufsize': '9000k',
                'audio_bitrate': '192k'
            },
            '4k': {
                'height': 2160,
                'crf': 25 if codec == 'h264' else (27 if codec == 'h265' else 30),
                'preset': 'slow' if codec in ['h264', 'h265'] else 'fast',
                'pix_fmt': validated_pixel_format,
                'profile': validated_profile,
                'max_bitrate': '12000k',
                'bufsize': '24000k',
                'audio_bitrate': '256k'
            },
            'noencode': {'audio_bitrate': 'copy'}
        }

        # 🎯 FIX: ENHANCED TUNE SETTINGS FOR H.265/AV1
        tune_settings = {
            'none': {'tune': None, 'x264_params': '', 'x265_params': '', 'av1_params': ''},
            'animation': {
                'tune': 'animation',
                'x264_params': 'aq-mode=2:aq-strength=1.2:psy-rd=1.0:deblock=-1,-1',
                'x265_params': 'aq-mode=3:aq-strength=1.2:psy-rd=2.0:deblock=-1,-1',
                'av1_params': 'film-grain=8:aq-mode=3'
            },
            'film': {
                'tune': 'film',
                'x264_params': 'aq-mode=2:psy-rd=1.0',
                'x265_params': 'aq-mode=2:psy-rd=1.2',
                'av1_params': 'aq-mode=3'
            },
            'grain': {
                'tune': 'grain',
                'x264_params': 'aq-mode=2:deblock=-2,-2:psy-rd=0.8',
                'x265_params': 'aq-mode=3:deblock=-2,-2:psy-rd=1.0',
                'av1_params': 'film-grain=15'
            },
            'fastdecode': {
                'tune': 'fastdecode',
                'x264_params': 'bframes=0:no-deblock=1:ref=2',
                'x265_params': 'bframes=0:no-deblock=1:ref=2',
                'av1_params': 'fast-decode=1'
            },
            'zerolatency': {
                'tune': 'zerolatency',
                'x264_params': 'bframes=0:sliced-threads:ref=1',
                'x265_params': 'bframes=0:ref=1:no-open-gop=1',
                'av1_params': 'lag-in-frames=0'
            }
        }

        # Fetch chosen settings
        settings = quality_settings.get(quality, quality_settings['720p'])
        tune_config = tune_settings.get(video_tune, tune_settings['none'])
        crf_value = custom_crf if custom_crf is not None else settings.get('crf')
        chosen_preset = preset if preset not in [None, ""] else settings.get("preset", "medium")

        logger.info(f"🔎 Building FFmpeg command: codec={codec}, quality={quality}, preset={preset}")
        logger.info(f"    Settings: height={settings['height']}, crf={crf_value}, pix_fmt={settings['pix_fmt']}")

        # Detect watermark usage
        use_watermark = (
            watermark_enabled and watermark_path and os.path.exists(watermark_path) and quality != 'noencode'
        )

        # Start building command
        cmd = ['ffmpeg', '-hide_banner', '-loglevel', 'error']
        if enable_progress:
            cmd.extend(['-progress', 'pipe:1', '-nostats'])

        # Main input(s)
        cmd.extend(['-i', input_path])
        input_count = 1

        # Optional subtitle input for hard-sub
        if subtitle_mode == 'hard_new' and subtitle_file and os.path.exists(subtitle_file):
            cmd.extend(['-i', subtitle_file])
            input_count += 1

        # Optional external audio input
        if audio_mode == 'add' and audio_file and os.path.exists(audio_file):
            cmd.extend(['-i', audio_file])
            input_count += 1

        # Watermark input handling
        if use_watermark:
            cmd.extend(['-i', watermark_path])
            watermark_index = input_count
            input_count += 1

        # Noencode mode: just copy and return
        if quality == 'noencode':
            cmd.extend([
                '-c', 'copy',
                '-map', '0',
                '-map_metadata', '-1',
                '-metadata', f'title={title}',
                '-movflags', '+faststart',
                '-y', output_path
            ])
            return cmd

        # 🎯 FIX: CODEC-SPECIFIC SETTINGS WITH BETTER COMPATIBILITY
        codec_lower = codec.lower()
        
        if codec_lower == 'h265':
            video_codec = ['-c:v', 'libx265']
            # 🎯 CRITICAL FIX: Proper H.265 compatibility flags
            cmd.extend(['-tag:v', 'hvc1'])  # Apple compatibility
            cmd.extend(['-x265-params', 'hdr-opt=1:repeat-headers=1:no-info=1'])
            logger.info("🎥 Selected codec: libx265 with enhanced compatibility")
            
        elif codec_lower == 'av1':
            video_codec = ['-c:v', 'libsvtav1']
            logger.info("🎥 Selected codec: libsvtav1 (AV1)")
            
        else:
            video_codec = ['-c:v', 'libx264']
            logger.info("🎥 Selected codec: libx264")

        # Audio settings
        audio_bitrate = settings.get('audio_bitrate', '128k')
        if audio_mode == 'remove':
            audio_settings = ['-an']
        elif audio_mode == 'add' and audio_file and os.path.exists(audio_file):
            audio_settings = ['-map', f'{input_count-1}:a?', '-c:a', 'aac', '-b:a', audio_bitrate, '-ac', '2']
        elif audio_mode == 'extract':
            return ['ffmpeg', '-i', input_path, '-vn', '-acodec', 'aac', '-b:a', '192k', '-ac', '2', '-y', output_path]
        else:
            audio_settings = ['-map', '0:a?', '-c:a', 'aac', '-b:a', audio_bitrate, '-ac', '2']

        # Subtitle mapping
        if subtitle_mode == 'remove':
            subtitle_settings = ['-sn']
        elif subtitle_mode == 'hard_new':
            subtitle_settings = ['-sn']
        else:
            subtitle_settings = ['-map', '0:s?', '-c:s', 'copy']

        # 🎯 FIX: BUILD FILTER CHAIN WITH BETTER COMPATIBILITY
        if use_watermark:
            # Parse watermark options
            try:
                scale_percent = int(watermark_scale.replace('%', ''))
            except Exception:
                scale_percent = 10
            try:
                opacity_value = float(watermark_opacity.replace('%', '')) / 100.0
            except Exception:
                opacity_value = 0.7

            position_configs = {
                "top-left": "10:10",
                "top-right": "main_w-overlay_w-10:10",
                "bottom-left": "10:main_h-overlay_h-10",
                "bottom-right": "main_w-overlay_w-10:main_h-overlay_h-10",
                "center": "(main_w-overlay_w)/2:(main_h-overlay_h)/2"
            }
            position_map = {
                'tl': 'top-left', 'tr': 'top-right', 'bl': 'bottom-left', 'br': 'bottom-right', 'c': 'center'
            }
            actual_position = position_map.get(watermark_position, watermark_position)
            overlay_position = position_configs.get(actual_position, position_configs["bottom-right"])

            # Build filter_complex
            main_scale_filter = f"[0:v]scale=-2:{settings['height']}:flags=lanczos[bg]"
            watermark_proc = f"[{watermark_index}:v]scale=iw*{scale_percent}/100:-1:flags=lanczos,format=rgba,colorchannelmixer=aa={opacity_value}[wm]"
            overlay = f"[bg][wm]overlay={overlay_position}[outv]"

            filter_complex = ";".join([main_scale_filter, watermark_proc, overlay])

            # If hard-sub is requested
            if subtitle_mode == 'hard_new' and subtitle_file and os.path.exists(subtitle_file):
                subtitle_chain = f"[0:v]scale=-2:{settings['height']}:flags=lanczos,subtitles='{subtitle_file}':force_style='FontName=Arial,FontSize=16'[bg]"
                filter_complex = ";".join([subtitle_chain, watermark_proc, overlay])

            cmd.extend(['-filter_complex', filter_complex])
            cmd.extend(['-map', '[outv]'])
            
        else:
            # Simple scaling (and optional hard subtitles)
            video_filters = [f"scale=-2:{settings['height']}"]
            if subtitle_mode == 'hard_new' and subtitle_file and os.path.exists(subtitle_file):
                video_filters.append(f"subtitles='{subtitle_file}':force_style='FontName=Arial,FontSize=16'")
            if video_filters:
                cmd.extend(['-vf', ','.join(video_filters)])
            cmd.extend(['-map', '0:v?'])

        # Add codec and basic stream params
        cmd.extend([
            *audio_settings,
            *subtitle_settings,
            *video_codec
        ])

        # 🎯 FIX: ENHANCED PROFILE HANDLING
        if codec_lower == 'h265' and validated_profile and validated_profile != 'main':
            cmd.extend(['-profile:v', validated_profile])
        elif codec_lower == 'h264' and validated_profile and validated_profile != 'main':
            cmd.extend(['-profile:v', validated_profile])
        elif codec_lower == 'av1' and validated_profile and validated_profile != 'main':
            av1_profiles = {'high': '1', 'professional': '2'}
            if validated_profile in av1_profiles:
                cmd.extend(['-profile', av1_profiles[validated_profile]])

        # 🎯 FIX: CODEC-SPECIFIC PARAMETER ASSEMBLY
        if codec_lower == 'av1':
            # AV1 (libsvtav1) settings - FIXED PRESET MAPPING
            preset_lower = chosen_preset.lower()
            svtav1_preset_map = {
                'veryslow': '0', 'slower': '1', 'slow': '2',
                'medium': '4', 'fast': '6', 'faster': '8',
                'veryfast': '10', 'superfast': '12', 'ultrafast': '13'
            }
            svt_preset = svtav1_preset_map.get(preset_lower, '4')  # Default to medium
            
            cmd.extend([
                '-preset', svt_preset,
                '-crf', str(crf_value),
                '-pix_fmt', validated_pixel_format
            ])

            # Add AV1-specific tune parameters
            if video_tune in tune_config and tune_config.get('av1_params'):
                cmd.extend(['-svtav1-params', tune_config['av1_params']])

            logger.info(f"🔧 AV1 params: preset={svt_preset}, crf={crf_value}")

        else:
            # H.264/H.265 settings
            cmd.extend([
                '-preset', chosen_preset,
                '-crf', str(crf_value),
                '-pix_fmt', validated_pixel_format
            ])

            # Add bitrate control for non-AV1 codecs
            if codec_lower != 'av1':
                cmd.extend([
                    '-maxrate', settings.get('max_bitrate'),
                    '-bufsize', settings.get('bufsize')
                ])

            # Apply tune parameters
            if tune_config.get('tune'):
                cmd.extend(['-tune', tune_config['tune']])
                
            if codec_lower == 'h264' and tune_config.get('x264_params'):
                cmd.extend(['-x264-params', tune_config['x264_params']])
                
            if codec_lower == 'h265' and tune_config.get('x265_params'):
                # Merge with existing x265 params
                existing_params = 'hdr-opt=1:repeat-headers=1:no-info=1:' + tune_config['x265_params']
                cmd.extend(['-x265-params', existing_params])

        # Final common flags
        cmd.extend([
            '-map_metadata', '-1',
            '-metadata', f'title={title}',
            '-threads', '0',
            '-y', output_path
        ])

        logger.info(f"✅ Final FFmpeg command for {codec.upper()}: {output_path}")
        return cmd
    async def encode_all_qualities(
        self,
        input_path: str,
        base_output_dir: str,
        metadata: Dict[str, str],
        watermark_path: Optional[str],
        watermark_enabled: bool,
        thumbnail_path: Optional[str],
        codec: str = 'h264',
        progress_callback: Optional[Callable[[float, str, str], None]] = None,
        remname: Optional[str] = None,
        subtitle_mode: str = 'keep',
        subtitle_file: Optional[str] = None,
        audio_mode: str = 'keep',
        audio_file: Optional[str] = None,
        custom_crf: Optional[int] = None,
        preset: str = 'veryfast',
        selected_qualities: List[str] = None,
        video_tune: str = 'none',  # 🆕 ADD THIS PARAMETER
        watermark_position: str = "bottom-right",
        watermark_scale: str = "10%", 
        watermark_opacity: str = "70%",
        chat_id: int = None  # 🆕 ADD chat_id parameter
    ) -> Dict[str, str]:
        """Encode video in selected qualities with proper watermark settings"""
        # 🆕 GET ACTUAL WATERMARK SETTINGS
        watermark_position = "bottom-right"
        watermark_scale = "10%"
        watermark_opacity = "70%"
        if chat_id:
            try:
                session = await get_user_settings(chat_id)  # ✅ PROPER AWAIT
                if session:
                    watermark_position = session.get("watermark_position", "bottom-right")
                    watermark_scale = session.get("watermark_scale", "10%")
                    watermark_opacity = session.get("watermark_opacity", "70%")
                    logger.info(f"🎯 All Qualities - Loaded watermark settings: {watermark_position}, {watermark_scale}, {watermark_opacity}")
            except Exception as e:
                logger.error(f"Failed to get watermark settings in all qualities: {str(e)}")
        # Use selected qualities or default to all
        qualities = selected_qualities or ['480p', '720p', '1080p', 'noencode']
        logger.info(f"🎯 encode_all_qualities called for: {ospath.basename(input_path)}")
        logger.info(f"🎯 Qualities to encode: {qualities}")
        results = {}
        total_qualities = len(qualities)
        for idx, quality in enumerate(qualities):
            try:
                logger.info(f"🔄 Encoding {quality} for {ospath.basename(input_path)}")
                # Create base output directory for this quality
                output_dir = ospath.join(base_output_dir, quality)
                makedirs(output_dir, exist_ok=True)

                original_name = ospath.splitext(ospath.basename(input_path))[0]
                if remname:
                    original_name = self.apply_remname(original_name, remname)

                # Different filename format for noencode
                if quality == 'noencode':
                    output_filename = f"{original_name}.[NOENCODE].mkv"
                    output_path = ospath.join(output_dir, output_filename)
                else:
                    # For encoding qualities, use a simple filename - quality tag will be added later
                    output_filename = f"{original_name}.mkv"
                    output_path = ospath.join(output_dir, output_filename)
                    
                logger.info(f"🎯 Output path for {quality}: {output_path}")
                logger.info(f"🎯 Output path for {quality}: {output_path}")
                if progress_callback:
                    await progress_callback(0, f"Starting {quality}", quality)
                if quality == 'noencode':
                    # For noencode, just copy the original file
                    import shutil
                    shutil.copy2(input_path, output_path)
                    results[quality] = output_path
                    logger.info(f"✅ Noencode copy created: {output_path}")
                    if progress_callback:
                        await progress_callback(100, f"Completed {quality}", quality)
                else:
                    # FIXED: Create proper progress callback for quality encoding
                    async def quality_progress_callback(progress: float, status: str, details: Dict = None):
                        """Progress callback for individual quality encoding WITH QUALITY INFO"""
                        if progress_callback:
                            # Format the status for display with quality info
                            quality_info = f"🎯 Encoding {quality.upper()}"
                            if details:
                                speed = details.get('speed', 1.0)
                                fps = details.get('fps', 0)
                                bitrate = details.get('bitrate', 'N/A')
                                frame = details.get('frame', 0)
                                eta_str = details.get('eta_str', 'calculating...')
                                elapsed_str = details.get('elapsed_str', '0s')
                                status_text = (
                                    f"{quality_info}\n"
                                 #   f"Speed: {speed:.1f}x | Bitrate: {bitrate}\n"
                                  #  f"Frame: {frame} | FPS: {fps}\n"
                                    f"ETA: {eta_str} | Elapsed: {elapsed_str}"
                                )
                            else:
                                status_text = f"{quality_info}\n{status}"
                            # Pass quality as the third parameter
                            await progress_callback(progress, status_text, quality)
                    # For other qualities, encode normally with proper progress
                    encoded_path = await self.encode_with_progress(
                        input_path=input_path,
                        output_path=output_path,
                        quality=quality,
                        metadata=metadata,
                        watermark_path=watermark_path,
                        watermark_enabled=watermark_enabled,
                        thumbnail_path=thumbnail_path,
                        codec=codec,
                        progress_callback=quality_progress_callback,
                        subtitle_mode=subtitle_mode,
                        subtitle_file=subtitle_file,
                        audio_mode=audio_mode,
                        audio_file=audio_file,
                        custom_crf=custom_crf,
                        preset=preset,
                        video_tune=video_tune,  # 🆕 ADD THIS LINE
                        chat_id=chat_id,  # 🆕 Add this to get watermark settings
                        watermark_position=watermark_position,
                        watermark_scale=watermark_scale,
                        watermark_opacity=watermark_opacity
                    )
                    results[quality] = encoded_path
                    logger.info(f"✅ Successfully encoded {quality}: {encoded_path}")
            except Exception as e:
                logger.error(f"Failed to process {quality}: {str(e)}")
                results[quality] = None
                if progress_callback:
                    await progress_callback(100, f"Failed {quality}: {str(e)}", quality)
        return results
    def _handle_all_qualities_progress(self, progress, status, quality, quality_idx, total_qualities, progress_callback):
        """Handle progress callbacks for all qualities encoding"""
        if progress_callback:
            # Calculate overall progress for this quality
            base_progress = (quality_idx / total_qualities) * 100
            current_progress = (progress / 100) * (100 / total_qualities)
            total_progress = base_progress + current_progress
            asyncio.create_task(progress_callback(total_progress, status, quality))
    async def encode_with_progress(
        self,
        input_path: str,
        output_path: str,
        quality: str,
        metadata: Dict[str, str],
        watermark_path: Optional[str],
        watermark_enabled: bool,
        thumbnail_path: Optional[str],
        codec: str = 'h264',
        progress_callback: Optional[Callable[[float, str, Dict], None]] = None,
        remname: Optional[str] = None,
        subtitle_mode: str = 'keep',
        subtitle_file: Optional[str] = None,
        audio_mode: str = 'keep',
        audio_file: Optional[str] = None,
        custom_crf: Optional[int] = None,
        preset: str = 'medium',
        video_tune: str = 'none',
        task_id: Optional[str] = None,
        chat_id: Optional[int] = None,
        watermark_position: str = "bottom-right",
        watermark_scale: str = "10%", 
        watermark_opacity: str = "70%"
    ) -> str:
        """Encoding with proper watermark settings and ENHANCED OUTPUT VALIDATION - FIXED VERSION"""
        try:
            if not ospath.exists(input_path):
                raise FileNotFoundError(f"Input file not found: {input_path}")
            
            # 🎯 ENHANCED: Validate input file first
            video_info = await self.get_video_info(input_path)
            if video_info['duration'] <= 0:
                raise ValueError("Invalid video duration")
            
            # 🎯 ENHANCED: Ensure output directory exists
            output_dir = ospath.dirname(output_path)
            makedirs(output_dir, exist_ok=True)
            
            # 🎯 FIX: Get actual watermark settings
            watermark_position = "bottom-right"
            watermark_scale = "10%"
            watermark_opacity = "70%"
            if chat_id:
                try:
                    session = await get_user_settings(chat_id) if chat_id else {}
                    if session:
                        watermark_position = session.get("watermark_position", "bottom-right")
                        watermark_scale = session.get("watermark_scale", "10%")
                        watermark_opacity = session.get("watermark_opacity", "70%")
                        logger.info(f"🎯 Loaded watermark settings for {chat_id}: {watermark_position}, {watermark_scale}, {watermark_opacity}")
                except Exception as e:
                    logger.error(f"Failed to get watermark settings: {str(e)}")
            
            # 🎯 ENHANCED: Build command with better error handling
            cmd = self._build_ffmpeg_command(
                input_path=input_path,
                quality=quality,
                watermark_path=watermark_path,
                watermark_enabled=watermark_enabled,
                title=metadata.get('title', 'Untitled'),
                output_path=output_path,
                codec=codec,
                enable_progress=True,
                subtitle_mode=subtitle_mode,
                subtitle_file=subtitle_file,
                audio_mode=audio_mode,
                audio_file=audio_file,
                custom_crf=custom_crf,
                preset=preset,
                video_tune=video_tune,
                watermark_position=watermark_position,
                watermark_scale=watermark_scale,
                watermark_opacity=watermark_opacity
            )
            
            logger.info(f"Running FFmpeg command: {' '.join(cmd)}")
            
            # 🎯 FIX: Enhanced process execution with better error handling
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=PIPE,
                stderr=PIPE
            )
            
            global_queue_manager.set_current_process(process)
            start_time = time.time()
            last_update = start_time
            stderr_output = []
            
            # For stable progress tracking
            last_progress = 0
            progress_history = []
            
            async def read_stderr():
                """Collect stderr for error reporting"""
                while True:
                    line = await process.stderr.readline()
                    if not line:
                        break
                    line_str = line.decode().strip()
                    stderr_output.append(line_str)
                    logger.debug(f"FFmpeg stderr: {line_str}")
            
            stderr_task = asyncio.create_task(read_stderr())
            
            progress_patterns = {
                'out_time': re.compile(r"out_time_ms=(\d+)"),
                'speed': re.compile(r"speed=([\d.]+)x"),
                'fps': re.compile(r"fps=([\d.]+)"),
                'bitrate': re.compile(r"bitrate=([\d.]+kbits/s)"),
                'frame': re.compile(r"frame=(\d+)")
            }
            
            # 🎯 ENHANCED: Monitor process with better cancellation handling
            while True:
                # Check for cancellation
                user_event = await get_user_cancellation_event(chat_id)
                if user_event.is_set():
                    logger.warning(f"Encoding task cancelled for {chat_id}")
                    process.terminate()
                    await process.wait()
                    raise TaskCancelled(f"Encoding cancelled by user (chat_id={chat_id})")
                
                line = await process.stdout.readline()
                if not line:
                    break
                    
                line_str = line.decode().strip()
                current_time = time.time()
                
                if "out_time_ms=" in line_str:
                    match = progress_patterns['out_time'].search(line_str)
                    if match:
                        current_time_ms = int(match.group(1))
                        raw_progress = min(99.9, (current_time_ms / 1_000_000 / video_info['duration']) * 100)
                        
                        # Smooth progress to avoid jumping
                        progress_history.append(raw_progress)
                        if len(progress_history) > 5:
                            progress_history.pop(0)
                        current_progress = sum(progress_history) / len(progress_history)
                        
                        # Only update if progress actually changed significantly
                        if abs(current_progress - last_progress) < 0.5 and current_time - last_update < 5:
                            continue
                            
                        last_progress = current_progress
                        speed_match = progress_patterns['speed'].search(line_str)
                        speed = float(speed_match.group(1)) if speed_match else 1.0
                        fps_match = progress_patterns['fps'].search(line_str)
                        fps = int(float(fps_match.group(1))) if fps_match else 0
                        bitrate_match = progress_patterns['bitrate'].search(line_str)
                        bitrate = bitrate_match.group(1) if bitrate_match else "N/A"
                        frame_match = progress_patterns['frame'].search(line_str)
                        frame = int(frame_match.group(1)) if frame_match else 0
                        
                        # Calculate stable ETA
                        elapsed = current_time - start_time
                        if current_progress > 5 and speed > 0:
                            total_estimated_time = (elapsed / current_progress) * 100
                            remaining = max(10, total_estimated_time - elapsed)
                        else:
                            remaining = 0
                            
                        if progress_callback:
                            try:
                                # Format times without floating point issues
                                elapsed_seconds = int(elapsed)
                                if elapsed_seconds < 60:
                                    elapsed_str = f"{elapsed_seconds}s"
                                elif elapsed_seconds < 3600:
                                    elapsed_str = f"{elapsed_seconds//60}m {elapsed_seconds%60}s"
                                else:
                                    elapsed_str = f"{elapsed_seconds//3600}h {(elapsed_seconds%3600)//60}m"
                                    
                                if remaining > 0:
                                    remaining_seconds = int(remaining)
                                    if remaining_seconds < 60:
                                        eta_str = f"{remaining_seconds}s"
                                    elif remaining_seconds < 3600:
                                        eta_str = f"{remaining_seconds//60}m {remaining_seconds%60}s"
                                    else:
                                        eta_str = f"{remaining_seconds//3600}h {(remaining_seconds%3600)//60}m"
                                else:
                                    eta_str = "calculating..."
                                    
                                progress_details = {
                                    'speed': speed,
                                    'fps': fps,
                                    'bitrate': bitrate,
                                    'frame': frame,
                                    'elapsed': elapsed,
                                    'eta': remaining,
                                    'elapsed_str': elapsed_str,
                                    'eta_str': eta_str
                                }
                                
                                progress_text = (
                                    f"Encoding ({speed:.1f}x)\n"
                                    f"Bitrate: {bitrate} | FPS: {fps}\n"
                                    f"Frame: {frame} | ETA: {eta_str}\n"
                                    f"Elapsed: {elapsed_str}"
                                )
                                
                                await progress_callback(current_progress, progress_text, progress_details)
                                last_update = current_time
                                
                            except Exception as e:
                                logger.warning(f"Progress callback error: {str(e)}")
                                
                elif "progress=end" in line_str:
                    if progress_callback:
                        elapsed = time.time() - start_time
                        elapsed_seconds = int(elapsed)
                        if elapsed_seconds < 60:
                            elapsed_str = f"{elapsed_seconds}s"
                        elif elapsed_seconds < 3600:
                            elapsed_str = f"{elapsed_seconds//60}m {elapsed_seconds%60}s"
                        else:
                            elapsed_str = f"{elapsed_seconds//3600}h {(elapsed_seconds%3600)//60}m"
                            
                        final_details = {
                            'elapsed': elapsed,
                            'elapsed_str': elapsed_str,
                            'speed': 1.0,
                            'fps': int(video_info.get('fps', 0)),
                            'bitrate': 'Completed',
                            'frame': 'Final'
                        }
                        await progress_callback(100.0, f"Encoding complete! | Time: {elapsed_str}", final_details)
                    break
            
            await stderr_task
            return_code = await process.wait()
            
            # 🎯 FIX: BETTER OUTPUT VALIDATION WITH RETRIES
            max_retries = 5
            retry_delay = 2
            
            for attempt in range(max_retries):
                # Wait a bit for FFmpeg to finalize the file
                await asyncio.sleep(retry_delay)
                
                if ospath.exists(output_path):
                    file_size = ospath.getsize(output_path)
                    if file_size > 0:
                        logger.info(f"✅ Output file created successfully: {output_path} ({humanize.naturalsize(file_size)})")
                        break
                    else:
                        logger.warning(f"⚠️ Output file exists but is empty (0 bytes), attempt {attempt + 1}/{max_retries}")
                else:
                    logger.warning(f"⚠️ Output file not found, attempt {attempt + 1}/{max_retries}")
                    
                if attempt == max_retries - 1:
                    # Final attempt failed
                    error_msg = "\n".join(stderr_output[-10:])
                    logger.error(f"FFmpeg failed to create output file after {max_retries} attempts")
                    logger.error(f"FFmpeg stderr: {error_msg}")
                    
                    if return_code != 0:
                        raise RuntimeError(f"FFmpeg encoding failed with return code {return_code}: {error_msg}")
                    else:
                        raise RuntimeError(f"FFmpeg completed but output file was not created: {error_msg}")
            
            # 🎯 ENHANCED: Verify it's a valid video file
            try:
                verify_cmd = ['ffprobe', '-v', 'error', '-select_streams', 'v:0', '-count_frames', '-show_entries', 'stream=nb_read_frames', '-of', 'csv=p=0', output_path]
                verify_process = await asyncio.create_subprocess_exec(*verify_cmd, stdout=PIPE, stderr=PIPE)
                await verify_process.wait()
                
                if verify_process.returncode != 0:
                    raise RuntimeError("Output file is not a valid video")
                    
            except Exception as e:
                logger.warning(f"Output verification warning: {str(e)}")
                # Continue anyway as the file might still be playable
            
            logger.info(f"✅ Encoding successful: {output_path} ({humanize.naturalsize(file_size)})")
            return output_path
            
        except Exception as e:
            logger.error(f"Encoding failed: {str(e)}")
            
            # 🎯 ENHANCED: Cleanup failed output file
            if 'output_path' in locals() and ospath.exists(output_path):
                try:
                    osremove(output_path)
                    logger.info(f"🧹 Cleaned up failed output: {output_path}")
                except Exception as cleanup_error:
                    logger.warning(f"Failed to clean up output file: {str(cleanup_error)}")
                    
            raise
    def _format_time(self, seconds: float) -> str:
        """Format seconds into HH:MM:SS or MM:SS"""
        if seconds < 0:
            return "00:00"
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)
        if hours > 0:
            return f"{hours:02d}:{minutes:02d}:{secs:02d}"
        else:
            return f"{minutes:02d}:{secs:02d}"
    def apply_remname(self, filename: str, remname: str) -> str:
        """Apply REMNAME pattern to filename (without extension)"""
        if not remname:
            return filename
        if not remname.startswith("|"):
            remname = f"|{remname}"
        remname = remname.replace("\\s", " ")
        slit = remname.split("|")
        __newFileName = filename
        for rep in range(1, len(slit)):
            args = slit[rep].split(":")
            if len(args) == 3:
                __newFileName = re.sub(args[0], args[1], __newFileName, int(args[2]))
            elif len(args) == 2:
                __newFileName = re.sub(args[0], args[1], __newFileName)
            elif len(args) == 1:
                __newFileName = re.sub(args[0], "", __newFileName)
        return __newFileName
async def encode_with_fallback(self, input_path: str, output_path: str, quality: str, 
                              metadata: dict, codec: str, **kwargs) -> str:
    """
    Encode with fallback to H.264 if H.265/AV1 fails
    """
    max_retries = 2
    current_codec = codec
    
    for attempt in range(max_retries):
        try:
            encoded_path = await self.encode_with_progress(
                input_path=input_path,
                output_path=output_path,
                quality=quality,
                metadata=metadata,
                codec=current_codec,
                **kwargs
            )
            return encoded_path
            
        except Exception as e:
            logger.error(f"Encoding attempt {attempt + 1} failed with {current_codec}: {str(e)}")
            
            if attempt < max_retries - 1:
                # Fallback to H.264 on final attempt
                if current_codec in ['h265', 'av1']:
                    logger.info(f"🔄 Falling back to H.264 from {current_codec}")
                    current_codec = 'h264'
                    # Update output path for new codec
                    if output_path.endswith('.mkv') and current_codec == 'h264':
                        output_path = output_path.replace('.mkv', '_fallback.mkv')
                    continue
                    
            # If all retries failed, re-raise the exception
            raise
    
    # This should never be reached, but just in case
    raise RuntimeError("All encoding attempts failed")
async def handle_torrent_download(chat_id: int, magnet_link: str) -> Tuple[str, List[Dict]]:
    """Handle torrent download - FIXED CANCELLATION"""
     # Use global limits instead of hardcoded ones
    MAX_TORRENT_SIZE = TORRENT_SIZE_LIMIT
    MAX_SINGLE_FILE_SIZE = SINGLE_FILE_SIZE_LIMIT
    download_dir = ospath.join("downloads", f"dl_{chat_id}_{int(time.time())}")
    makedirs(download_dir, exist_ok=True)
    progress_msg = None
    downloader = None
    try:
        cancel_button = InlineKeyboardMarkup([[
            InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_download_{chat_id}")
        ]])
        progress_msg = await bot.send_message(
            chat_id,
            "🛰️ **Starting Torrent Download**\n"
            "🔗 Connecting to peers...",
            reply_markup=cancel_button
        )
        # 🎯 SIMPLE APPROACH: Start download immediately, check size after
        await progress_msg.edit_text(
            "🌀 **Starting Download**\n\n"
            "⚡ No pre-check - downloading directly\n"
            "📦 Size will be verified after download\n"
            "🚫 Oversized files will be auto-deleted",
            reply_markup=cancel_button
        )
        # Start the actual download immediately
        downloader = SimpleAria2Downloader(magnet_link, download_dir)
                # Update task info with downloader reference for progress tracking

        download_task = asyncio.create_task(downloader.start_download())
        start_time = time.time()
        last_update = start_time
        speed_samples = []
        max_speed = 0
        while not download_task.done():
            await asyncio.sleep(2)
            # 🎯 FIXED: Check for cancellation using user-specific event
            user_event = await get_user_cancellation_event(chat_id)
            if user_event.is_set():
                logger.info(f"🛑 Download cancelled by user {chat_id}")
                if downloader:
                    downloader.stop_download()
                try:
                    await download_task
                except TaskCancelled:
                    pass
                finally:
                    user_event.clear()  # 🎯 Clear the event after cancellation
                    raise TaskCancelled("Download cancelled by user")
            current_time = time.time()
            if current_time - last_update < 3:
                continue
            try:
                # Get progress from downloader
                progress, dl_speed, ul_speed, eta = downloader.get_progress()
                peers = downloader.get_peers()
                total_size = downloader.get_total_size()
                downloaded_size = (progress / 100) * total_size if total_size > 0 else 0

                async with task_lock:
                    if chat_id in active_tasks:
                        active_tasks[chat_id].update({
                            'real_progress': progress,
                            'real_speed': dl_speed,
                            'real_eta': eta,
                            'last_progress_update': current_time
                        })
                # Speed monitoring
                if dl_speed > 0:
                    speed_samples.append(dl_speed)
                    if len(speed_samples) > 8:
                        speed_samples.pop(0)
                    if dl_speed > max_speed:
                        max_speed = dl_speed
                avg_speed = sum(speed_samples) / len(speed_samples) if speed_samples else 0
                # Format display
                current_speed_str = humanize.naturalsize(dl_speed) + "/s" if dl_speed > 0 else "0 B/s"
                avg_speed_str = humanize.naturalsize(avg_speed) + "/s" if avg_speed > 0 else "0 B/s"
                max_speed_str = humanize.naturalsize(max_speed) + "/s" if max_speed > 0 else "0 B/s"
                size_str = f"{humanize.naturalsize(downloaded_size)} / {humanize.naturalsize(total_size)}" if total_size > 0 else "Calculating..."
                eta_str = humanize.naturaldelta(eta) if eta > 0 else "calculating..."
                progress_bar_length = 16
                filled_length = int(progress / 100 * progress_bar_length)
                progress_bar = get_random_progress_bar(progress, length=progress_bar_length)
                # Status detection
                if progress == 0:
                    status_line = "🔄 Connecting to peers..."
                elif progress < 10:
                    status_line = "📥 Getting file list..."
                elif progress < 25:
                    status_line = "📦 Downloading metadata..."
                elif progress < 90:
                    status_line = "⚡ Downloading content..."
                else:
                    status_line = "🎯 Finalizing..."
                # Speed status
                speed_status = ""
                if dl_speed > 100 * 1024 * 1024:
                    speed_status = "🚀 **EXTREME**"
                elif dl_speed > 50 * 1024 * 1024:
                    speed_status = "⚡ **VERY FAST**"
                elif dl_speed > 20 * 1024 * 1024:
                    speed_status = "💨 **FAST**"
                elif dl_speed > 5 * 1024 * 1024:
                    speed_status = "📊 **GOOD**"
                elif dl_speed > 1 * 1024 * 1024:
                    speed_status = "🐢 **SLOW**"
                else:
                    speed_status = "🌐 **CONNECTING**"
                await progress_msg.edit_text(
                    f"🌀 **Torrent Download** {speed_status}\n\n"
                    f"{status_line}\n"
                    f"{progress_bar} {progress:.1f}%\n\n"
                    f"📦 **Size:** {size_str}\n"
                    f"🚀 **Speed:** {current_speed_str}\n"
                    f"📊 **Average:** {avg_speed_str}\n"
                    f"🏆 **Peak:** {max_speed_str}\n"
                    f"👥 **Peers:** {peers}\n"
                    f"⏳ **ETA:** {eta_str}\n"
                    f"⏱️ **Elapsed:** {humanize.precisedelta(current_time - start_time)}",
                    reply_markup=cancel_button
                )
                last_update = current_time
            except Exception as e:
                logger.warning(f"Progress update error: {str(e)}")
                continue
        # Download completed - NOW check size
        result = await download_task
        # 🎯 SIZE VALIDATION AFTER DOWNLOAD
        await progress_msg.edit_text(
            "📊 **Download Complete**\n\n"
            "✅ Checking file sizes...\n"
            "⏳ Verifying limits..."
        )
        all_files = []
        total_download_size = 0
        valid_files = []
        if ospath.exists(download_dir):
            for root, _, files in os.walk(download_dir):
                for f in files:
                    if not f.startswith('.'):
                        file_path = ospath.join(root, f)
                        try:
                            if ospath.exists(file_path) and ospath.getsize(file_path) > 0:
                                file_size = ospath.getsize(file_path)
                                total_download_size += file_size
                                file_info = {
                                    'name': f,
                                    'size': file_size,
                                    'path': file_path,
                                    'is_video': f.lower().endswith(('.mkv', '.mp4', '.avi', '.mov', '.flv', '.wmv', '.webm'))
                                }
                                all_files.append(file_info)
                                # Check individual file size
                                if file_size <= MAX_SINGLE_FILE_SIZE:
                                    valid_files.append(file_info)
                                else:
                                    logger.warning(f"Oversized file skipped: {f} - {humanize.naturalsize(file_size)}")
                        except Exception as e:
                            logger.warning(f"Skipping file {f}: {str(e)}")
                            continue
        # 🎯 FINAL SIZE CHECK
        if total_download_size > MAX_TORRENT_SIZE:
            logger.info(f"Torrent exceeds size limit: {humanize.naturalsize(total_download_size)} > {humanize.naturalsize(MAX_TORRENT_SIZE)}")
            if ospath.exists(download_dir):
                shutil.rmtree(download_dir, ignore_errors=True)
            await progress_msg.edit_text(
                f"❌ **Size Limit Exceeded**\n\n"
                f"📦 **Downloaded Size:** {humanize.naturalsize(total_download_size)}\n"
                f"📏 **Size Limit:** {humanize.naturalsize(MAX_TORRENT_SIZE)}\n\n"
                f"🗑️ **Files deleted** - Please choose a smaller torrent."
            )
            raise ValueError(f"Torrent size {humanize.naturalsize(total_download_size)} exceeds limit")
        # After download completes and we have valid_files:
        if not valid_files:
            if ospath.exists(download_dir):
                shutil.rmtree(download_dir, ignore_errors=True)
            raise ValueError("❌ No valid files found - all files were too large or corrupted")
        # 🎯 CRITICAL FIX: Store ALL valid files in user session
        await update_user_settings(chat_id, {
            "files": valid_files,  # 🎯 Store ALL files, not just one
            "status": "batch_downloaded",  # 🎯 Important: Use batch_downloaded status
            "download_path": download_dir,
            "file_path": valid_files[0]['path'] if valid_files else None  # 🎯 Keep for compatibility
        })
        # Calculate final stats
        total_time = time.time() - start_time
        final_avg_speed = total_download_size / total_time if total_time > 0 else 0
        # Show final success message
        skipped_count = len(all_files) - len(valid_files)
        status_details = []
        if skipped_count > 0:
            status_details.append(f"🗑️ {skipped_count} oversized files skipped")
        status_details_text = "\n".join(status_details) if status_details else "✅ All files within limits"
        await progress_msg.edit_text(
            f"🎉 **Download Complete!** 🏆\n\n"
            f"📂 **Valid Files:** {len(valid_files)}\n"
            f"📦 **Total Size:** {humanize.naturalsize(total_download_size)}\n"
            f"⏱️ **Time:** {humanize.precisedelta(total_time)}\n"
            f"🚀 **Avg Speed:** {humanize.naturalsize(final_avg_speed) + '/s'}\n"
            f"🏆 **Peak Speed:** {humanize.naturalsize(max_speed) + '/s'}\n\n"
            f"{status_details_text}"
        )
        await update_storage_analytics(chat_id, total_download_size, 0, "downloaded")
        # 🎯 DEBUG: Log what we're storing
        logger.info(f"💾 Stored {len(valid_files)} files in session for {chat_id}:")
        for i, file_info in enumerate(valid_files):
            logger.info(f"💾 File {i+1}: {file_info['name']} - {file_info['path']}")
        return download_dir, valid_files
    except TaskCancelled:
        logger.info(f"🛑 Download cancelled by user {chat_id}")
        if progress_msg:
            await progress_msg.edit_text("❌ **Download Cancelled**\n\nDownload was stopped by user.")
        # 🎯 FIXED: Clear the cancellation event
        user_event = await get_user_cancellation_event(chat_id)
        user_event.clear()
        raise
    except ValueError as e:
        # Size limit errors - message already shown
        logger.info(f"Size limit exceeded: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Torrent download failed: {str(e)}", exc_info=True)
        if progress_msg:
            error_msg = str(e)
            if "size" in error_msg.lower() or "limit" in error_msg.lower():
                # Message already shown in the specific handler
                pass
            else:
                await progress_msg.edit_text(f"❌ **Download Failed**\n\nError: {error_msg}")
        # Cleanup on failure
        if download_dir and ospath.exists(download_dir):
            try:
                shutil.rmtree(download_dir, ignore_errors=True)
            except:
                pass
        raise ValueError(f"Download failed: {str(e)}")
    finally:
        # Cleanup progress message after delay
        if progress_msg:
            await asyncio.sleep(1)
            try:
                await bot.delete_messages(chat_id, progress_msg.id)
            except Exception:
                pass
async def process_single_file(
    chat_id: int,
    file_path: str,
    output_dir: str,
    quality: str,
    metadata: Dict[str, str],
    watermark_path: Optional[str],
    watermark_enabled: bool,
    thumbnail_path: Optional[str],
    codec: str,
    file_index: int,
    total_files: int,
    remname: Optional[str] = None,
    subtitle_mode: str = 'keep',
    subtitle_file: Optional[str] = None,
    audio_mode: str = 'keep',
    audio_file: Optional[str] = None,
    custom_crf: Optional[int] = None,
    preset: str = 'medium',
    task_id: str = None,
    progress_callback: Optional[Callable[[float, str, Dict], None]] = None,
    custom_output_filename: Optional[str] = None,
    video_tune: str = 'none',
    watermark_position: str = "bottom-right",
    watermark_scale: str = "10%", 
    watermark_opacity: str = "70%"
) -> Optional[str]:
    """Process a single file with proper cancellation and CUSTOM FILENAME SUPPORT - ENHANCED VERSION"""
    file_start_time = time.time()
    file_name = ospath.basename(file_path)
    
    try:
        if not ospath.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
            
        encoder = VideoEncoder()
        video_info = await encoder.get_video_info(file_path)
        if video_info['duration'] <= 0:
            raise ValueError("Invalid video duration")
            
        metadata.update({
            'duration': video_info['duration'],
            'width': video_info['video_streams'][0]['width'] if video_info['video_streams'] else 0,
            'height': video_info['video_streams'][0]['height'] if video_info['video_streams'] else 0,
        })
        
        # 🎯 CREATE OUTPUT DIRECTORY
        makedirs(output_dir, exist_ok=True)
        
        # 🎯 STEP 1: Apply REMNAME pattern first (if provided)
        original_name = ospath.splitext(file_name)[0]
        if remname:
            processed_name = encoder.apply_remname(original_name, remname)
            logger.info(f"🔧 Applied REMNAME: '{original_name}' -> '{processed_name}'")
        else:
            processed_name = original_name
        
        # 🎯 STEP 2: Create temporary output path for encoding
        temp_output_path = ospath.join(output_dir, f"{processed_name}{ospath.splitext(file_name)[1]}")
        
        # 🎯 STEP 3: Perform encoding
        if progress_callback:
            await progress_callback(0, f"Starting encoding for {file_name}", {})
        
        # Encode the file with better error handling
        try:
            encoded_path = await encoder.encode_with_fallback(
                input_path=file_path,
                output_path=temp_output_path,
                quality=quality,
                metadata=metadata,
                watermark_path=watermark_path,
                watermark_enabled=watermark_enabled,
                thumbnail_path=thumbnail_path,
                codec=codec,
                progress_callback=progress_callback,
                subtitle_mode=subtitle_mode,
                subtitle_file=subtitle_file,
                audio_mode=audio_mode,
                audio_file=audio_file,
                custom_crf=custom_crf,
                preset=preset,
                task_id=task_id,
                chat_id=chat_id,
                video_tune=video_tune,
                watermark_position=watermark_position,
                watermark_scale=watermark_scale,
                watermark_opacity=watermark_opacity
            )
        except Exception as encode_error:
            logger.error(f"❌ Encoding failed for {file_path}: {str(encode_error)}")
            # Try to get more details about the failure
            if "Permission denied" in str(encode_error):
                raise RuntimeError(f"Permission error: Cannot write to output directory {output_dir}")
            elif "Invalid data" in str(encode_error):
                raise RuntimeError("Invalid video file - may be corrupted")
            else:
                raise encode_error
        
        if not ospath.exists(encoded_path) or ospath.getsize(encoded_path) == 0:
            raise RuntimeError("Encoding produced invalid output file")
        
        # 🎯 STEP 4: Apply quality tag renaming using your function
        logger.info(f"🎯 Applying quality tag renaming to: {ospath.basename(encoded_path)}")
        final_output_path = await ensure_quality_tag_in_filename(encoded_path, quality)
        
        # If the file was renamed, update our path
        if final_output_path != encoded_path:
            logger.info(f"✅ File renamed: {ospath.basename(encoded_path)} -> {ospath.basename(final_output_path)}")
            # Clean up the temporary named file
            if ospath.exists(encoded_path) and encoded_path != final_output_path:
                try:
                    osremove(encoded_path)
                except Exception as e:
                    logger.warning(f"Failed to clean up temp file: {str(e)}")
        else:
            final_output_path = encoded_path
            logger.info(f"ℹ️  File name unchanged: {ospath.basename(final_output_path)}")
        
        logger.info(f"✅ Successfully processed: {ospath.basename(final_output_path)}")
        return final_output_path
        
    except Exception as e:
        logger.error(f"Processing failed for {file_path}: {str(e)}", exc_info=True)
        error_msg = f"❌ Failed to process {file_name}\nError: {str(e)}"
        try:
            await update_progress(chat_id, error_msg, force_new=True)
        except Exception as update_err:
            logger.error(f"Failed to send error message: {str(update_err)}")
        
        # Cleanup on failure
        if 'temp_output_path' in locals() and ospath.exists(temp_output_path):
            try:
                osremove(temp_output_path)
            except Exception as cleanup_err:
                logger.warning(f"Cleanup failed: {str(cleanup_err)}")
        if 'encoded_path' in locals() and ospath.exists(encoded_path) and 'final_output_path' in locals() and encoded_path != final_output_path:
            try:
                osremove(encoded_path)
            except Exception as cleanup_err:
                logger.warning(f"Cleanup failed: {str(cleanup_err)}")
                
        return None
async def get_file_size_info(original_files, processed_files):
    """Calculate file size changes"""
    try:
        original_total = sum(f['size'] for f in original_files if 'size' in f)
        processed_total = sum(ospath.getsize(f['path']) for f in processed_files if ospath.exists(f['path']))
        if original_total > 0 and processed_total > 0:
            reduction = ((original_total - processed_total) / original_total) * 100
            return original_total, processed_total, reduction
        return original_total, processed_total, 0
    except:
        return 0, 0, 0
async def process_batch(
    chat_id: int,
    files: List[Dict],
    quality: str,
    metadata: Dict[str, str],
    watermark_path: Optional[str],
    watermark_enabled: bool,
    thumbnail_path: Optional[str],
    upload_mode: str,
    codec: str = 'h264',
    remname: Optional[str] = None,
    subtitle_mode: str = 'keep',
    subtitle_file: Optional[str] = None,
    audio_mode: str = 'keep',
    audio_file: Optional[str] = None,
    custom_crf: Optional[int] = None,
    preset: str = 'medium',
    samples_enabled: bool = False,
    screenshots_enabled: bool = False,
    task_id: str = None,
    is_all_qualities: bool = False,
    selected_qualities: List[str] = None,
    video_tune: str = 'none',
    watermark_position: str = "bottom-right",
    watermark_scale: str = "10%", 
    watermark_opacity: str = "70%"
) -> None:
    """Process a batch of files with support for single quality or all qualities mode."""
    logger.info(f"🔍 PROCESS_BATCH called with:")
    logger.info(f"🔍 - Chat ID: {chat_id}")
    logger.info(f"🔍 - Files: {len(files)}")
    logger.info(f"🔍 - All Qualities: {is_all_qualities}")
    logger.info(f"🔍 - Selected Qualities: {selected_qualities}")
    logger.info(f"🔍 - Quality: {quality}")
    logger.info(f"🔍 - Files to process: {[f.get('path', 'No path') for f in files]}")
    
    processing_dirs = []  # Track all processing directories for cleanup
    processed_paths = []  # Store all processed files
    batch_start_time = time.time()
        # 🎯 FIX: Get session here for use in noencode processing
    session = await get_user_settings(chat_id)
    if not session:
        raise ValueError("Session not found for user")
    try:
        # 🎯 FIX: Handle all_qualities mode properly
        valid_qualities = ["480p", "720p", "1080p", "4k", "noencode"]
        
        if is_all_qualities and selected_qualities:
            # All qualities mode - validate selected qualities
            if not all(q in valid_qualities for q in selected_qualities):
                invalid_qualities = [q for q in selected_qualities if q not in valid_qualities]
                raise ValueError(f"Invalid qualities in selected_qualities: {invalid_qualities}")
        elif quality not in valid_qualities and quality != "all_qualities":
            # Single quality mode - validate quality
            raise ValueError(f"Invalid quality setting: {quality}")
        
        total_files = len(files)
        if total_files == 0:
            raise ValueError("No files provided for processing")
        
        files = sorted(files, key=lambda x: x['name'])
        cancel_button = InlineKeyboardMarkup([[
            InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_batch_{chat_id}")
        ]])
        # Initialize progress message
        if is_all_qualities and selected_qualities:
            selected_text = ", ".join(selected_qualities)
            await update_progress(
                chat_id,
                f"🚀 **Batch All Qualities Started**\n\n"
                f"📊 **Total Files:** {total_files}\n"
                f"🎯 **Qualities per file:** {selected_text}\n"
                f"🔤 **Codec:** {codec.upper()}\n"
                f"⚡ **Preset:** {preset.upper()}\n"
                f"💧 **Watermark:** {'Yes' if watermark_enabled else 'No'}\n"
                f"🎬 **Samples:** {'Yes' if samples_enabled else 'No'}\n"
                f"📸 **Screenshots:** {'Yes' if screenshots_enabled else 'No'}",
                force_new=True,
                reply_markup=cancel_button,
                start_time=batch_start_time
            )
        else:
            if len(files) == 1:
                file_name = ospath.basename(files[0]['path'])
                display_name = file_name[:35] + '...' if len(file_name) > 35 else file_name
                if quality == "noencode":

                    await update_progress(
                        chat_id,
                        f"🚀 **Starting Direct Upload**\n\n"
                        f"📄 **File:** {display_name}\n"
                        f"⚡ **Mode:** No Encoding\n"
                        f"🎬 **Samples:** {'Yes' if samples_enabled else 'No'}\n"
                        f"📸 **Screenshots:** {'Yes' if screenshots_enabled else 'No'}",
                        force_new=True,
                        reply_markup=cancel_button,
                        start_time=batch_start_time
                    )
                else:
                    await update_progress(
                        chat_id,
                        f"🚀 **Starting Encoding**\n\n"
                        f"📄 **File:** {display_name}\n"
                        f"🎚️ **Quality:** {quality.upper()}\n"
                        f"🔤 **Codec:** {codec.upper()}\n"
                        f"⚡ **Preset:** {preset.upper()}\n"
                        f"💧 **Watermark:** {'Yes' if watermark_enabled else 'No'}",
                        force_new=True,
                        reply_markup=cancel_button,
                        start_time=batch_start_time
                    )
            else:
                if quality == "noencode":
                    await update_progress(
                        chat_id,
                        f"🚀 **Batch Direct Upload Started**\n\n"
                        f"📊 **Total Files:** {total_files}\n"
                        f"⚡ **Mode:** No Encoding\n"
                        f"🎬 **Samples:** {'Yes' if samples_enabled else 'No'}\n"
                        f"📸 **Screenshots:** {'Yes' if screenshots_enabled else 'No'}",
                        force_new=True,
                        reply_markup=cancel_button,
                        start_time=batch_start_time
                    )
                else:
                    await update_progress(
                        chat_id,
                        f"🚀 **Batch Processing Started**\n\n"
                        f"📊 **Total Files:** {total_files}\n"
                        f"⚙️ **Mode:** {quality.upper()}\n"
                        f"🔤 **Codec:** {codec.upper()}\n"
                        f"⚡ **Preset:** {preset.upper()}\n"
                        f"💧 **Watermark:** {'Yes' if watermark_enabled else 'No'}\n"
                        f"🎬 **Samples:** {'Yes' if samples_enabled else 'No'}\n"
                        f"📸 **Screenshots:** {'Yes' if screenshots_enabled else 'No'}",
                        force_new=True,
                        reply_markup=cancel_button,
                        start_time=batch_start_time
                    )
        successful_encodes = 0
        failed_encodes = 0
        encoder = VideoEncoder()
        # Define progress callback for both single and batch files
        async def progress_handler(progress: float, status: str, current_quality: str = None):
            """Handle progress updates for both single files and batch processing"""
            try:
                current_time = time.time()  # ✅ Define current_time
                
                if total_files > 1:
                    # Batch mode - show overall progress
                    if is_all_qualities:
                        file_base_progress = (idx / total_files) * 100
                        quality_progress = (progress / 100) * (100 / total_files)
                        overall_progress = min(99.9, file_base_progress + quality_progress)
                    else:
                        overall_progress = ((idx + (progress / 100)) / total_files) * 100
                    
                    elapsed = current_time - batch_start_time
                    elapsed_str = f"{int(elapsed // 60):02d}:{int(elapsed % 60):02d}"
                    
                    # Build display text for batch
                    if is_all_qualities:
                        display_text = (
                            f"🚀 **Batch All Qualities**\n\n"
                            f"📊 **Overall Progress:** {overall_progress:.1f}%\n"
                            f"📁 **Files:** {idx+1}/{total_files} files\n" 
                            f"🎯 **Current:** {current_quality if current_quality else 'Processing...'}\n"
                            f"⏱️ **Elapsed:** {elapsed_str}"
                        )
                    else:
                        display_text = (
                            f"🔧 **Batch Encoding**\n\n"
                            f"📊 **Overall Progress:** {overall_progress:.1f}%\n"
                            f"📁 **Files:** {idx+1}/{total_files} files\n"
                            f"⏱️ **Elapsed:** {elapsed_str}"
                        )
                    
                    await update_progress(
                        chat_id,
                        display_text,
                        progress=overall_progress,
                        reply_markup=cancel_button,
                        start_time=batch_start_time
                    )
                else:
                    # Single file - show detailed encoding progress
                    elapsed = current_time - batch_start_time
                    elapsed_str = f"{int(elapsed // 60):02d}:{int(elapsed % 60):02d}"
                    
                    # Calculate ETA for single file
                    eta_str = ""
                    if 5 <= progress <= 95:
                        total_estimated = (elapsed / progress) * 100 if progress > 0 else 0
                        remaining = max(10, total_estimated - elapsed)
                        remaining_seconds = int(remaining)
                        if remaining_seconds < 60:
                            eta_str = f"| ⏳ {remaining_seconds}s"
                        elif remaining_seconds < 3600:
                            eta_str = f"| ⏳ {remaining_seconds//60}m {remaining_seconds%60}s"
                        else:
                            eta_str = f"| ⏳ {remaining_seconds//3600}h {(remaining_seconds%3600)//60}m"
                    
                    display_text = (
                        f"🔧 **Encoding**\n\n"
                        f"📄 **File:** {ospath.basename(files[0]['path'])[:35]}{'...' if len(ospath.basename(files[0]['path'])) > 35 else ''}\n"
                        f"{status}\n"
                        f"📊 **Progress:** {progress:.1f}%\n"
                        f"⏱️ **Elapsed:** {elapsed_str} {eta_str}"
                    )
                    
                    await update_progress(
                        chat_id,
                        display_text,
                        progress=progress,
                        reply_markup=cancel_button,
                        start_time=batch_start_time
                    )
                    
                # ✅ Update active tasks with real progress data
                async with task_lock:
                    if chat_id in active_tasks:
                        active_tasks[chat_id].update({
                            'real_progress': progress,
                            'last_progress_update': current_time,
                            'real_eta': remaining if 'remaining' in locals() else 0
                        })
                        
            except Exception as e:
                logger.warning(f"Progress handler error: {str(e)}")
        # Process files
        async with task_lock:
            if chat_id in active_tasks:
                active_tasks[chat_id].update({
                    'type': 'process_batch',
                    'real_progress': 0,  # ✅ Initialize with 0
                    'last_progress_update': time.time(),  # ✅ Use current time
                    'real_eta': 0,  # ✅ Initialize with 0
                    'total_files': len(files),
                    'current_file': 0,
                    'all_qualities': is_all_qualities
                })
        for idx, file_info in enumerate(files):
            # Update current file progress
            async with task_lock:
                if chat_id in active_tasks:
                    active_tasks[chat_id]['current_file'] = idx + 1
            
            logger.info(f"🔄 Starting processing for file {idx+1}/{total_files}: {file_info.get('path', 'No path')}")
            user_event = await get_user_cancellation_event(chat_id)
            if user_event.is_set():
                raise TaskCancelled("Batch processing cancelled by user")
            file_path = file_info['path']
            file_name = ospath.basename(file_path)
            if not ospath.exists(file_path):
                logger.error(f"❌ File not found, skipping: {file_path}")
                failed_encodes += 1
                continue
            # Create output directory for this file
            output_dir = ospath.join("tempwork", f"batch_{chat_id}_{idx}_{int(time.time())}")
            makedirs(output_dir, exist_ok=True)
            processing_dirs.append(output_dir)
            logger.info(f"🗂️ Created output directory: {output_dir}")
            # Process file based on mode
            try:

                if is_all_qualities and selected_qualities:
                    # All qualities mode
                    logger.info(f"🎯 Encoding {len(selected_qualities)} qualities for file {idx+1}: {file_name}")
                    file_results = await encoder.encode_all_qualities(
                        input_path=file_path,
                        base_output_dir=output_dir,
                        metadata=metadata,
                        watermark_path=watermark_path,
                        watermark_enabled=watermark_enabled,
                        thumbnail_path=thumbnail_path,
                        codec=codec,
                        progress_callback=progress_handler,
                        remname=remname,
                        subtitle_mode=subtitle_mode,
                        subtitle_file=subtitle_file,
                        audio_mode=audio_mode,
                        audio_file=audio_file,
                        custom_crf=custom_crf,
                        preset=preset,
                        selected_qualities=selected_qualities,
                        video_tune=video_tune,
                        chat_id=chat_id,
                        watermark_position=watermark_position,
                        watermark_scale=watermark_scale,
                        watermark_opacity=watermark_opacity
                    )
                    for quality, encoded_path in file_results.items():
                        if encoded_path and ospath.exists(encoded_path):
                            samples = []
                            screenshots = []
                            if samples_enabled:
                                try:
                                    samples = await generate_samples(encoded_path, chat_id, output_dir)
                                    logger.info(f"🎬 Generated {len(samples)} samples for {quality}: {encoded_path}")
                                except Exception as e:
                                    logger.warning(f"Failed to generate samples for {quality}: {str(e)}")
                            if screenshots_enabled:
                                try:
                                    screenshots = await generate_screenshots(encoded_path, chat_id, output_dir)
                                    logger.info(f"📸 Generated {len(screenshots)} screenshots for {quality}: {encoded_path}")
                                except Exception as e:
                                    logger.warning(f"Failed to generate screenshots for {quality}: {str(e)}")
                            
                            # 🎯 FIX: Apply quality tag renaming to ALL qualities files
                            try:
                                logger.info(f"🎯 Applying quality tag to {quality}: {ospath.basename(encoded_path)}")
                                final_path = await ensure_quality_tag_in_filename(encoded_path, quality)
                                logger.info(f"✅ Renamed {quality}: {ospath.basename(encoded_path)} -> {ospath.basename(final_path)}")
                            except Exception as e:
                                logger.error(f"❌ Failed to rename {quality} file: {str(e)}")
                                final_path = encoded_path  # Use original if renaming fails
                            
                            processed_paths.append({
                                'path': final_path,
                                'is_video': True,
                                'name': ospath.basename(final_path),
                                'samples': samples,
                                'screenshots': screenshots,
                                'original_name': file_name,
                                'quality': quality
                            })
                            successful_encodes += 1
                            logger.info(f"✅ Successfully encoded {quality} for file {idx+1}: {final_path}")
                        else:
                            failed_encodes += 1
                            logger.error(f"❌ Failed to encode {quality} for file {idx+1}: {file_name}")
                else:
                    # Single quality mode
                    if quality == "noencode":
                        processed_file = await process_noencode_with_subtitles_audio(
                            chat_id=chat_id,
                            file_path=file_path,
                            output_dir=output_dir,
                            session=session,  # Make sure session is available
                            file_name=file_name
                        )
                        
                        if processed_file:
                            processed_paths.append({
                                'path': processed_file,
                                'is_video': file_info['is_video'],
                                'name': ospath.basename(processed_file),
                                'samples': [],
                                'screenshots': []
                            })
                            successful_encodes += 1
                        else:
                            failed_encodes += 1
                    else:
                        # ✅ THIS IS THE MISSING PART - Execute encoding for non-noencode qualities
                        original_name = ospath.splitext(file_name)[0]
                        if remname:
                            original_name = encoder.apply_remname(original_name, remname)
                        
                        crf_display = f"CRF{custom_crf}" if custom_crf else "AutoCRF"
                        watermark_display = "WM" if watermark_enabled else "NoWM"
                        preset_display = preset.upper()
                        
                        output_filename = f"{original_name}.[{quality.upper()}][{codec.upper()}][{preset_display}][{crf_display}][{watermark_display}].mkv"
                        
                        # 🎯 ACTUALLY CALL THE ENCODING FUNCTION
                        encoded_path = await process_single_file(
                            chat_id=chat_id,
                            file_path=file_path,
                            output_dir=output_dir,
                            quality=quality,
                            metadata=metadata,
                            watermark_path=watermark_path,
                            watermark_enabled=watermark_enabled,
                            thumbnail_path=thumbnail_path,
                            codec=codec,
                            file_index=idx,
                            total_files=total_files,
                            remname=remname,
                            subtitle_mode=subtitle_mode,
                            subtitle_file=subtitle_file,
                            audio_mode=audio_mode,
                            audio_file=audio_file,
                            custom_crf=custom_crf,
                            preset=preset,
                            task_id=task_id,
                            progress_callback=progress_handler,
                            custom_output_filename=output_filename,
                            video_tune=video_tune,
                            watermark_position=watermark_position,
                            watermark_scale=watermark_scale,
                            watermark_opacity=watermark_opacity
                        )
                        
                        if encoded_path and ospath.exists(encoded_path):
                            # Initialize samples and screenshots
                            file_samples = []
                            file_screenshots = []
                            
                            if samples_enabled:
                                try:
                                    file_samples = await generate_samples(encoded_path, chat_id, output_dir)
                                except Exception as e:
                                    logger.warning(f"Failed to generate samples: {str(e)}")
                            
                            if screenshots_enabled:
                                try:
                                    file_screenshots = await generate_screenshots(encoded_path, chat_id, output_dir)
                                except Exception as e:
                                    logger.warning(f"Failed to generate screenshots: {str(e)}")
                            
                            processed_paths.append({
                                'path': encoded_path,
                                'is_video': True,
                                'name': ospath.basename(encoded_path),
                                'samples': file_samples,
                                'screenshots': file_screenshots,
                                'original_name': file_name,
                                'quality': quality
                            })
                            successful_encodes += 1
                        else:
                            failed_encodes += 1
                            logger.error(f"❌ Failed to encode file {idx+1}: {file_name}")
            except Exception as e:
                logger.error(f"❌ Failed to process file {idx+1}/{total_files}: {str(e)}", exc_info=True)
                failed_encodes += 1
                continue
        logger.info(f"🔍 Encoding complete: {successful_encodes} successful, {failed_encodes} failed")
        # 🆕 Update storage analytics for all processed files
        if processed_paths:
            for original_file, processed_file in zip(files, processed_paths):
                original_size = original_file.get('size', 0)
                processed_size = ospath.getsize(processed_file['path']) if ospath.exists(processed_file['path']) else 0
                await update_storage_analytics(chat_id, original_size, processed_size, "processed")
        # Upload phase
        upload_start_time = time.time()
        uploaded_count = 0
        failed_uploads = 0
        
        if processed_paths:
            total_time = time.time() - batch_start_time
            total_time_str = f"{int(total_time // 60):02d}:{int(total_time % 60):02d}"
            
            # 🎯 SORT PROCESSED FILES BY QUALITY FOR ORGANIZED UPLOAD
            quality_order = {"480p": 1, "720p": 2, "1080p": 3, "4k": 4, "noencode": 5}
            
            def get_quality_priority(file_info):
                quality = file_info.get('quality', 'noencode')
                return quality_order.get(quality, 99)
            
            sorted_processed_paths = sorted(processed_paths, key=get_quality_priority)
            
            if len(files) == 1:
                await update_progress(
                    chat_id,
                    f"📤 **Starting Upload**\n\n"
                    f"⏱️ **Encoding Time:** {total_time_str}\n"
                    f"🔄 **Upload Mode:** {upload_mode.capitalize()}\n"
                    f"🎯 **Upload Order:** Sorted by quality (480p → 720p → 1080p → 4K → NoEncode)",
                    reply_markup=cancel_button,
                    start_time=batch_start_time
                )
            else:
                expected_uploads = total_files * (len(selected_qualities) if is_all_qualities else 1)
                await update_progress(
                    chat_id,
                    f"📤 **Starting Upload Phase**\n\n"
                    f"📊 **Files to Upload:** {len(sorted_processed_paths)}/{expected_uploads}\n"
                    f"✅ **Encoded Successfully:** {successful_encodes}\n"
                    f"❌ **Encoding Failed:** {failed_encodes}\n"
                    f"⏱️ **Encoding Time:** {total_time_str}\n"
                    f"🔄 **Upload Mode:** {upload_mode.capitalize()}\n"
                    f"🎯 **Upload Order:** Sorted by quality",
                    reply_markup=cancel_button,
                    start_time=batch_start_time
                )
        
        # 🎯 FIX: Initialize sorted_processed_paths properly
        sorted_processed_paths = processed_paths  # Default to unsorted

        # Only sort if we have multiple files and it's all qualities mode
        if len(processed_paths) > 1 and is_all_qualities:
            # 🎯 SORT PROCESSED FILES BY QUALITY FOR ORGANIZED UPLOAD
            quality_order = {"480p": 1, "720p": 2, "1080p": 3, "4k": 4, "noencode": 5}
            
            def get_quality_priority(file_info):
                quality = file_info.get('quality', 'noencode')
                return quality_order.get(quality, 99)
            
            sorted_processed_paths = sorted(processed_paths, key=get_quality_priority)
        else:
            sorted_processed_paths = processed_paths

        # Use sorted list for upload
        for idx, file_info in enumerate(sorted_processed_paths):
            user_event = await get_user_cancellation_event(chat_id)
            if user_event.is_set():
                raise TaskCancelled("Upload cancelled by user")
                
            file_path = file_info['path']
            file_name = file_info['name']
            file_quality = file_info.get('quality', 'Unknown')
            samples_to_upload = file_info.get('samples', [])
            screenshots_to_upload = file_info.get('screenshots', [])
            original_name = file_info.get('original_name', file_name)
            
            upload_progress = ((idx) / len(sorted_processed_paths)) * 100
            upload_bar = "[" + "█" * int(upload_progress / 8.33) + "░" * (12 - int(upload_progress / 8.33)) + "]"
            upload_elapsed = time.time() - upload_start_time
            upload_elapsed_str = f"{int(upload_elapsed // 60):02d}:{int(upload_elapsed % 60):02d}"
            upload_eta_str = ""
            
            if upload_progress > 5:
                upload_total_estimated = (upload_elapsed / upload_progress) * 100 if upload_progress > 0 else 0
                upload_remaining = upload_total_estimated - upload_elapsed
                upload_eta_str = f"| ⏳ {int(upload_remaining // 60):02d}:{int(upload_remaining % 60):02d}"
                
            if len(sorted_processed_paths) > 1:
                await update_progress(
                    chat_id,
                    f"📤 **Uploading Files** {upload_bar}\n\n"
                    f"📊 **Progress:** {idx + 1}/{len(sorted_processed_paths)} ({upload_progress:.1f}%)\n"
                    f"📄 **File:** {file_name[:30]}{'...' if len(file_name) > 30 else ''}\n"
                    f"🎯 **Quality:** {file_quality}\n"
                    f"✅ **Uploaded:** {uploaded_count}\n"
                    f"🎬 **Samples:** {len(samples_to_upload)}\n"
                    f"📸 **Screenshots:** {len(screenshots_to_upload)}\n"
                    f"⏱️ **Upload Time:** {upload_elapsed_str} {upload_eta_str}",
                    reply_markup=cancel_button,
                    start_time=batch_start_time
                )
                
            file_start_time = time.time()
            file_metadata = metadata.copy()
            
            try:
                # 🎯 FIX: Only use upload_file_with_progress which already sends the file with caption
                success = await upload_file_with_progress(
                    chat_id=chat_id,
                    file_path=file_path,
                    upload_mode=upload_mode if file_info['is_video'] else "document",
                    thumbnail_path=thumbnail_path if file_info['is_video'] else None,
                    metadata=file_metadata,
                    progress_msg=None,
                    start_time=file_start_time,
                    task_id=task_id
                )
                
                if success:
                    uploaded_count += 1
                    logger.info(f"✅ Uploaded file {idx+1}/{len(sorted_processed_paths)}: {file_path} (Quality: {file_quality})")
                    
                    # Upload samples and screenshots
                    for sample_idx, sample_path in enumerate(samples_to_upload):
                        try:
                            if len(samples_to_upload) > 1:
                                await update_progress(
                                    chat_id,
                                    f"🎬 Uploading sample {sample_idx + 1}/{len(samples_to_upload)}\n"
                                    f"📄 {ospath.basename(sample_path)[:25]}...\n"
                                    f"🎯 Quality: {file_quality}",
                                    start_time=batch_start_time
                                )
                            await bot.send_video(
                                chat_id=chat_id,
                                video=sample_path,
                                caption=f"🎬 Sample {sample_idx + 1} of {original_name} [{file_quality}]"
                            )
                            if ospath.exists(sample_path):
                                osremove(sample_path)
                                logger.info(f"🧹 Removed sample: {sample_path}")
                        except Exception as e:
                            logger.error(f"❌ Failed to upload sample {sample_idx+1}: {str(e)}")
                            continue
                            
                    for screenshot_idx, screenshot_path in enumerate(screenshots_to_upload):
                        try:
                            if len(screenshots_to_upload) > 1:
                                await update_progress(
                                    chat_id,
                                    f"📸 Uploading screenshot {screenshot_idx + 1}/{len(screenshots_to_upload)}\n"
                                    f"📄 {ospath.basename(screenshot_path)[:25]}...\n"
                                    f"🎯 Quality: {file_quality}",
                                    start_time=batch_start_time
                                )
                            await bot.send_photo(
                                chat_id=chat_id,
                                photo=screenshot_path,
                                caption=f"📸 Screenshot {screenshot_idx + 1} of {original_name} [{file_quality}]"
                            )
                            if ospath.exists(screenshot_path):
                                osremove(screenshot_path)
                                logger.info(f"🧹 Removed screenshot: {screenshot_path}")
                        except Exception as e:
                            logger.error(f"❌ Failed to upload screenshot {screenshot_idx+1}: {str(e)}")
                            continue
                            
                    if len(sorted_processed_paths) == 1:
                        await update_progress(
                            chat_id,
                            f"✅ **Upload Complete**\n\n"
                            f"📄 **File:** {file_name[:30]}{'...' if len(file_name) > 30 else ''}\n"
                            f"🎯 **Quality:** {file_quality}\n"
                            f"✅ **Status:** Uploaded successfully",
                            start_time=batch_start_time
                        )
                else:
                    failed_uploads += 1
                    logger.error(f"❌ Failed to upload file {idx+1}: {file_path}")
                    
            except Exception as e:
                logger.error(f"❌ Upload error for file {idx+1}: {str(e)}")
                failed_uploads += 1
                continue
        # Final completion message
        total_end_time = time.time()
        total_duration = total_end_time - batch_start_time
        upload_duration = total_end_time - upload_start_time
        encode_duration = upload_start_time - batch_start_time
        total_duration_str = f"{int(total_duration // 60):02d}:{int(total_duration % 60):02d}"
        encode_duration_str = f"{int(encode_duration // 60):02d}:{int(encode_duration % 60):02d}"
        upload_duration_str = f"{int(upload_duration // 60):02d}:{int(upload_duration % 60):02d}"
        completion_time = datetime.now().strftime('%H:%M:%S')
        await cleanup_progress_messages(chat_id)
        expected_uploads = total_files * (len(selected_qualities) if is_all_qualities else 1)
        # Final completion message - ENHANCED FOR ALL QUALITIES
        total_end_time = time.time()
        total_duration = total_end_time - batch_start_time
        total_duration_str = f"{int(total_duration // 60):02d}:{int(total_duration % 60):02d}"
        
        await cleanup_progress_messages(chat_id)
        
        # 🎯 ENHANCED COMPLETION MESSAGE FOR ALL QUALITIES
        if is_all_qualities and selected_qualities:
            # All Qualities completion message
            selected_text = ", ".join(selected_qualities)
            
            # Calculate total size statistics for all qualities
            total_original_size = sum(f.get('size', 0) for f in files)
            total_processed_size = 0
            total_encoded_files = 0
            
            for file_info in processed_paths:
                if file_info['path'] and ospath.exists(file_info['path']):
                    total_processed_size += ospath.getsize(file_info['path'])
                    total_encoded_files += 1
            
            total_reduction = 0
            if total_original_size > 0 and total_processed_size > 0:
                total_reduction = ((total_original_size - total_processed_size) / total_original_size) * 100
            
            # Get user settings for details
            session = await get_user_settings(chat_id)
            codec = session.get("codec", "h264").upper()
            preset = session.get("preset", "medium").upper()
            video_tune = session.get("video_tune", "none")
            watermark_enabled = session.get("watermark_enabled", False)
            samples_enabled = session.get("samples_enabled", False)
            screenshots_enabled = session.get("screenshots_enabled", False)
            
            completion_message = (
                f"<blockquote>🎉 PROCESSING COMPLETE! 🎉</blockquote>\n"
                f"• 📁 Original Files: {len(files)}\n"
                f"• 🎯 Qualities Created: {selected_text}\n"
                f"• 📦 Total Encoded Files: {total_encoded_files}\n"
                f"• ⏱️ Total Time: {total_duration_str}\n"
                
                f"<blockquote>📈SIZE STATISTICS</blockquote>\n"
                f"• 📦 Total Original: {humanize.naturalsize(total_original_size)}\n"
                f"• 🎯 Total Processed: {humanize.naturalsize(total_processed_size)}\n"
                f"• 💾 Total Reduction: {total_reduction:.1f}% saved\n\n"
                
                f"<blockquote>⚙️ **ENCODING SETTINGS USED\n"
                f"• 🔤 Codec: {codec}\n"
                f"• ⚡ Preset: {preset}\n"
                f"• 🎨 Video Tune: {video_tune.title() if video_tune != 'none' else 'None'}\n"
                f"• 💧 Watermark: {'✅ Enabled' if watermark_enabled else '❌ Disabled'}\n"
                f"• 🎬 Samples: {'✅ Generated' if samples_enabled else '❌ Disabled'}\n"
                f"• 📸 Screenshots: {'✅ Generated' if screenshots_enabled else '❌ Disabled'}\n\n"
                
                f"📋 **QUALITY BREAKDOWN\n"
                "</blockquote>"
            )
            
            # Add quality-specific file information
            quality_files = {}
            for file_info in processed_paths:
                file_quality = file_info.get('quality', 'unknown')
                if file_quality not in quality_files:
                    quality_files[file_quality] = []
                quality_files[file_quality].append(file_info)
            
            for quality_name in selected_qualities:
                if quality_name in quality_files:
                    file_count = len(quality_files[quality_name])
                    total_size = sum(ospath.getsize(f['path']) for f in quality_files[quality_name] if ospath.exists(f['path']))
                    completion_message += f"• {quality_name.upper()}: {file_count} files, {humanize.naturalsize(total_size)}\n"
            
            completion_message += f"\n✅ **All {len(processed_paths)} files successfully processed!**"
            
        elif len(files) == 1:
            # Single file completion message (your existing code)
            original_size = files[0].get('size', 0)
            if processed_paths and ospath.exists(processed_paths[0]['path']):
                processed_size = ospath.getsize(processed_paths[0]['path'])
                size_reduction = ((original_size - processed_size) / original_size) * 100 if original_size > 0 else 0
            else:
                processed_size = 0
                size_reduction = 0
            
            # Get user settings for details
            session = await get_user_settings(chat_id)
            quality = session.get("quality", "720p")
            codec = session.get("codec", "h264").upper()
            preset = session.get("preset", "medium").upper()
            video_tune = session.get("video_tune", "none")
            watermark_enabled = session.get("watermark_enabled", False)
            samples_enabled = session.get("samples_enabled", False)
            screenshots_enabled = session.get("screenshots_enabled", False)
            custom_crf = session.get("custom_crf", "Auto")
            
            # Build detailed message
            completion_message = (
                f"🎉 **PROCESSING COMPLETE!** 🎉\n\n"
                f"📄 **File:** `{ospath.basename(files[0]['path'])[:35]}{'...' if len(ospath.basename(files[0]['path'])) > 35 else ''}`\n"
                f"✅ **Status:** Successfully processed\n\n"
                f"📊 **SIZE COMPARISON:**\n"
                f"• 📦 Original: {humanize.naturalsize(original_size) if original_size > 0 else 'N/A'}\n"
                f"• 🎯 Processed: {humanize.naturalsize(processed_size) if processed_size > 0 else 'N/A'}\n"
                f"• 💾 Reduction: {size_reduction:.1f}% saved\n\n"
                f"⚙️ **ENCODING SETTINGS USED:**\n"
                f"• 🎚️ Quality: {quality.upper()}\n"
                f"• 🔤 Codec: {codec}\n"
                f"• ⚡ Preset: {preset}\n"
                f"• 🎛️ CRF: {custom_crf}\n"
                f"• 🎨 Video Tune: {video_tune.title() if video_tune != 'none' else 'None'}\n"
                f"• 💧 Watermark: {'✅ Enabled' if watermark_enabled else '❌ Disabled'}\n"
                f"• 🎬 Samples: {'✅ Generated' if samples_enabled else '❌ Disabled'}\n"
                f"• 📸 Screenshots: {'✅ Generated' if screenshots_enabled else '❌ Disabled'}\n\n"
                f"⏱️ **Total Time:** {total_duration_str}\n"
            )
        else:
            # Batch processing completion message (your existing code)
            original_total, processed_total, total_reduction = await get_file_size_info(files, processed_paths)
            session = await get_user_settings(chat_id)
            quality = session.get("quality", "720p")
            codec = session.get("codec", "h264").upper()
            preset = session.get("preset", "medium").upper()
            
            completion_message = (
                f"<blockquote>🎉 BATCH PROCESSING COMPLETE! 🎉</blockquote>\n"
                f"• Total Input Files: {total_files}\n"
                f"• Successful Uploads: {uploaded_count}/{len(processed_paths)}\n"
                f"• Failed: {failed_encodes + failed_uploads}\n"
                f"• Mode: {quality.upper()}\n\n"
                f"<blockquote>📈 **SIZE STATISTICS:</blockquote>\n"
                f"• Original Total: {humanize.naturalsize(original_total) if original_total > 0 else 'N/A'}\n"
                f"• Processed Total: {humanize.naturalsize(processed_total) if processed_total > 0 else 'N/A'}\n"
                f"• Total Reduction: {total_reduction:.1f}% saved\n\n"
                f"⚙️ **SETTINGS USED:**\n"
                f"• Codec: {codec}\n"
                f"• Preset: {preset}\n"
                f"• Quality: {quality.upper()}\n"
                f"• Files Processed: {len(processed_paths)}\n\n"
                f"⏱️ **TIMING**\n"
                f"• Total: {total_duration_str}\n"
            )
            
        await update_progress(
            chat_id,
            completion_message,
            force_new=True
        )
        logger.info(f"🏁 Batch completed: {uploaded_count}/{expected_uploads} successful, {failed_encodes + failed_uploads} failed")
    except TaskCancelled:
        logger.info(f"Batch processing cancelled by user {chat_id}")
        total_time = time.time() - batch_start_time
        total_time_str = f"{int(total_time // 60):02d}:{int(total_time % 60):02d}"
        await update_progress(
            chat_id,
            f"❌ **Processing Cancelled**\n\n"
            f"⏱️ **Time spent:** {total_time_str}\n"
            f"📊 **Files processed:** {len(processed_paths)}\n",
          #  f"🕒 **Cancelled at:** {datetime.now().strftime('%H:%M:%S')}",
            force_new=True
        )
    except Exception as e:
        logger.error(f"Batch processing failed: {str(e)}", exc_info=True)
        total_time = time.time() - batch_start_time
        total_time_str = f"{int(total_time // 60):02d}:{int(total_time % 60):02d}"
        await update_progress(
            chat_id,
            f"❌ **Processing Failed**\n\n"
            f"📄 **Error:** {str(e)[:100]}{'...' if len(str(e)) > 100 else ''}\n"
            f"⏱️ **Time spent:** {total_time_str}\n"
            f"📊 **Files processed:** {len(processed_paths)}",
            force_new=True
        )
    finally:
        # Cleanup processing directories
        for processing_dir in processing_dirs:
            if processing_dir and ospath.exists(processing_dir):
                try:
                    shutil.rmtree(processing_dir, ignore_errors=True)
                    logger.info(f"🧹 Cleaned processing directory: {processing_dir}")
                except Exception as e:
                    logger.warning(f"Failed to cleanup {processing_dir}: {str(e)}")
        # Cleanup generated files (samples/screenshots)
        try:
            await cleanup_generated_files(chat_id)
            logger.info(f"🧹 Cleaned generated files for {chat_id}")
        except Exception as e:
            logger.error(f"Cleanup error: {str(e)}")
async def debug_batch_processing(chat_id: int, files: List[Dict]):
    """Debug function to track batch processing issues"""
    logger.info(f"🔍 DEBUG BATCH PROCESSING for {chat_id}:")
    logger.info(f"🔍 Total files received: {len(files)}")
    for i, file_info in enumerate(files):
        file_path = file_info.get('path', 'No path')
        file_name = file_info.get('name', 'Unknown')
        exists = ospath.exists(file_path) if file_path else False
        logger.info(f"🔍 File {i+1}: {file_name} - Exists: {exists} - Path: {file_path}")
        if not exists:
            logger.error(f"❌ FILE DOES NOT EXIST: {file_path}")
@bot.on_callback_query(filters.regex(r"^cancel_batch_all_qualities_(\d+)$"))
async def cancel_batch_all_qualities_handler(client: Client, query: CallbackQuery):
    """Handle cancellation of Batch All Qualities processing"""
    try:
        chat_id = int(query.data.split("_")[4])
        # Set global cancellation flag
        user_event.set()
        # Cancel current task in global queue
        await global_queue_manager.cancel_current_task()
        await query.answer("🛑 Cancelling Batch All Qualities processing...")
        # Update progress message
        await query.message.edit_text(
            "❌ **Batch All Qualities Processing Cancelled**\n\n"
            "The batch multi-quality encoding has been stopped.\n"
            "You can start a new task whenever you're ready."
        )
        logger.info(f"Batch All Qualities processing cancelled by user {chat_id}")
    except Exception as e:
        logger.error(f"Cancel Batch All Qualities error: {str(e)}")
        await query.answer("Failed to cancel processing!", show_alert=True)
async def upload_batch_all_qualities(chat_id, processed_files, thumbnail_path, upload_mode, metadata, batch_start_time, total_original_files, selected_qualities):
    """Upload all files from batch All Qualities processing - FIXED UPLOAD"""
    try:
        total_files = len(processed_files)
        if total_files == 0:
            await update_progress(chat_id, "❌ No files were processed successfully", force_new=True)
            return
        selected_text = ", ".join(selected_qualities)
        await update_progress(
            chat_id,
            f"📤 **Starting Batch Upload**\n\n"
            f"📊 **Files to Upload:** {total_files}\n"
            f"🎯 **Qualities:** {selected_text}\n"
            f"🔢 **Original Files:** {total_original_files}",
            start_time=batch_start_time
        )
        uploaded_count = 0
        failed_uploads = 0
        # 🎯 CANCEL BUTTON for upload
        upload_cancel_button = InlineKeyboardMarkup([[
            InlineKeyboardButton("❌ Cancel Upload", callback_data=f"cancel_upload_{chat_id}")
        ]])
        for idx, file_info in enumerate(processed_files):
            try:
                user_event = await get_user_cancellation_event(chat_id)
                if user_event.is_set():
                    raise TaskCancelled("Upload cancelled by user")
                file_path = file_info['path']
                file_name = file_info['name']
                file_quality = file_info.get('quality', 'Unknown')
                upload_progress = ((idx) / total_files) * 100
                await update_progress(
                    chat_id,
                    f"📤 **Uploading Files**\n\n"
                    f"📊 **Progress:** {idx+1}/{total_files} ({upload_progress:.1f}%)\n"
                    f"📄 **File:** {file_name[:30]}{'...' if len(file_name) > 30 else ''}\n"
                    f"🎯 **Quality:** {file_quality}\n"
                    f"✅ **Uploaded:** {uploaded_count}\n"
                    f"❌ **Failed:** {failed_uploads}",
                    progress=upload_progress,
                    reply_markup=upload_cancel_button,
                    start_time=batch_start_time
                )
                file_start_time = time.time()
                file_metadata = metadata.copy()
                success = await upload_file_with_progress(
                    chat_id=chat_id,
                    file_path=file_path,
                    upload_mode=upload_mode,
                    thumbnail_path=thumbnail_path,
                    metadata=file_metadata,
                    progress_msg=None,
                    start_time=file_start_time,
                    task_id=None
                )
                if success:
                    uploaded_count += 1
                    logger.info(f"✅ Uploaded: {file_name}")
                else:
                    failed_uploads += 1
                    logger.error(f"❌ Failed to upload: {file_name}")
                await asyncio.sleep(1)  # Small delay between uploads
            except TaskCancelled:
                raise
            except Exception as e:
                logger.error(f"Failed to upload file {idx + 1}: {str(e)}")
                failed_uploads += 1
                continue
        # Final completion message
        total_time = time.time() - batch_start_time
        total_time_str = f"{int(total_time // 60):02d}:{int(total_time % 60):02d}"
        await cleanup_progress_messages(chat_id)
        await update_progress(
            chat_id,
            f"🎉 **BATCH ALL QUALITIES COMPLETE!** 🎉\n\n"
            f"📊 **Summary**\n"
            f"• Original Files: {total_original_files}\n"
            f"• Total Uploads: {uploaded_count}/{total_files}\n"
            f"• Failed Uploads: {failed_uploads}\n"
            f"• Qualities: {selected_text}\n\n"
            f"⏱️ **Total Time:** {total_time_str}",
            force_new=True
        )
        logger.info(f"✅ Batch All Qualities completed: {uploaded_count}/{total_files} files uploaded")
    except TaskCancelled:
        raise
    except Exception as e:
        logger.error(f"Batch All Qualities upload failed: {str(e)}")
        raise
async def upload_file_with_progress(
    chat_id: int,
    file_path: str,
    upload_mode: str,
    thumbnail_path: Optional[str],
    metadata: Dict[str, str],
    progress_msg: Optional[Message],
    start_time: float,
    task_id: str = None
) -> bool:
    """Upload with progress tracking, dump channel, and showcase channel support.

    This function preserves your original progress/cancel/ETA logic and
    adds multi-target upload (dump, chat, showcase) and showcase captioning.
    """
    if not hasattr(upload_file_with_progress, 'upload_messages_meta'):
        upload_file_with_progress.upload_messages_meta = {}
    
    # Track this upload in queue
    upload_file_with_progress.upload_messages_meta[chat_id] = {
        'current_file': 0,
        'total_files': 1,  # Will be updated if batch
        'start_time': start_time,
        'status': 'uploading'
    }
    async def _get_showcase_caption(metadata: dict, user_info: dict) -> str:
        """Generate showcase caption with user credits and user ID"""
        title = metadata.get('title', 'Unknown Title')

        caption_parts = [f"🎬 **{title}**"]

        # Add user credits + ID
        if user_info:
            if user_info.get('username'):
                caption_parts.append(f"👤 Encoded by: @{user_info['username']}")
            else:
                caption_parts.append(f"👤 Encoded by: {user_info['first_name']}")
            caption_parts.append(f"🆔 User ID: `{user_info['id']}`")

        # Add bot credit
        me = await bot.get_me()
        caption_parts.append(f"✨ Encoded with @{me.username}")

        return "\n".join(caption_parts)


    async def _get_upload_caption(metadata: Dict, user_info: Optional[Dict], target_type: str) -> str:
        if target_type == "showcase":
            return await _get_showcase_caption(metadata, user_info)
        title = metadata.get('title', 'Unknown Title')
        return f"🎬 {title}"

    # Get user info for showcase (if enabled)
    user_info = None
    if SHOWCASE_ENABLED:
        try:
            user = await bot.get_users(chat_id)
            user_info = {
                "id": user.id,
                "username": user.username,
                "first_name": user.first_name,
                "mention": user.mention
            }
        except Exception as e:
            logger.warning(f"Could not get user info for showcase: {str(e)}")
            user_info = {"id": chat_id, "username": None, "first_name": f"User {chat_id}", "mention": f"User {chat_id}"}

    # CHECK FOR DUMP CHANNEL (existing code path)
    dump_channel = None
    try:
        dump_channel = await get_dump_channel(chat_id)
        # if you have additional validation for dump_channel do it here (kept intentionally minimal)
    except Exception as e:
        logger.error(f"Error checking dump channel: {str(e)}")
        dump_channel = None

    # Determine upload targets
    upload_targets = []

    # 1. User's dump channel (if set)
    if dump_channel:
        upload_targets.append(("dump", dump_channel))

    # 2. Original chat (if no dump channel or showcase requires it)
    if not dump_channel or SHOWCASE_FORMAT == "upload":
        upload_targets.append(("chat", chat_id))

    # 3. Showcase channel (if enabled)
    if SHOWCASE_ENABLED and SHOWCASE_CHANNEL:
        upload_targets.append(("showcase", SHOWCASE_CHANNEL))

    if not upload_targets:
        await bot.send_message(chat_id, "❌ No upload destinations configured!")
        return False

    # Ensure a place to keep per-chat progress messages (preserve your existing pattern)
    if not hasattr(upload_file_with_progress, 'upload_messages'):
        upload_file_with_progress.upload_messages = {}
    current_progress_msg = upload_file_with_progress.upload_messages.get(chat_id)

    # Per-chat upload tracking
    if not hasattr(upload_file_with_progress, 'upload_messages_meta'):
        upload_file_with_progress.upload_messages_meta = {}

    uploaded_messages = {}  # Keep uploaded message objects per target_type
    success_count = 0

    # Shared variables for progress callback (kept from your original implementation)
    file_size = ospath.getsize(file_path)
    file_name = ospath.basename(file_path)
    last_update = time.time()
    last_bytes = 0
    speed_history = []

    # Progress callback used by pyrogram during each upload
    async def progress_callback(current, total):
        nonlocal last_update, last_bytes, speed_history
        # Check for cancellation
        user_event = await get_user_cancellation_event(chat_id)
        if user_event.is_set():
            raise TaskCancelled("Upload cancelled by user")

        now = time.time()
        # throttle updates (kept your original 7.0s threshold)
        if now - last_update < 7.0:
            return

        progress = (current / total) * 100
        elapsed = now - start_time

        # Calculate speed and ETA
        if now > last_update:
            current_speed = (current - last_bytes) / (now - last_update)
            speed_history.append(current_speed)

        if len(speed_history) > 3:
            speed_history.pop(0)

        avg_speed = sum(speed_history) / len(speed_history) if speed_history else 0
        remaining_bytes = total - current
        eta = remaining_bytes / avg_speed if avg_speed > 0 else 0

        # Update active tasks with real upload progress (keeps your existing active_tasks usage)
        async with task_lock:
            if chat_id in active_tasks:
                progress_percent = (current / total) * 100 if total > 0 else 0
                active_tasks[chat_id].update({
                    'real_progress': progress_percent,
                    'real_speed': avg_speed,
                    'real_eta': eta,
                    'last_progress_update': now,
                    'type': 'upload_file_with_progress'  # 🎯 Ensure correct type
                })
        # Format display for progress message
        speed_str = humanize.naturalsize(avg_speed) + "/s" if avg_speed > 0 else "calculating..."
        elapsed_str = f"{int(elapsed // 60):02d}:{int(elapsed % 60):02d}"
        eta_str = humanize.naturaldelta(eta) if eta > 0 else "calculating..."
        progress_bar_length = 10
        # keep your helper get_random_progress_bar to build the bar
        progress_bar = get_random_progress_bar(progress, length=progress_bar_length)

        cancel_button = InlineKeyboardMarkup([[
            InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_upload_{chat_id}")
        ]])

        upload_target_display = "📢 Dump Channel" if dump_channel else "💬 This Chat"

        update_text = (
            f"📤 **Uploading to {upload_target_display}**\n"
            f"📄 {file_name[:30]}{'...' if len(file_name) > 30 else ''}\n\n"
            f"{progress_bar} {progress:.1f}%\n"
            f"📦 {humanize.naturalsize(current)}/{humanize.naturalsize(total)}\n"
            f"🚀 {speed_str}\n"
            f"⏳ ETA: {eta_str}\n"
            f"🕒 Elapsed: {elapsed_str}"
        )

        try:
            current_msg = upload_file_with_progress.upload_messages.get(chat_id)
            if current_msg and hasattr(current_msg, 'edit_text'):
                await current_msg.edit_text(
                    update_text,
                    reply_markup=cancel_button
                )
                logger.debug(f"✅ Successfully edited progress message for {file_name} - {progress:.1f}%")
            else:
                # Recreate if missing
                logger.warning("Progress message missing, creating new one")
                new_msg = await bot.send_message(
                    chat_id,
                    update_text,
                    reply_markup=cancel_button
                )
                upload_file_with_progress.upload_messages[chat_id] = new_msg
            last_update = now
            last_bytes = current

        except MessageNotModified:
            # This is fine - message doesn't need updating
            pass
        except FloodWait as e:
            logger.warning(f"FloodWait: Waiting {e.value}s")
            await asyncio.sleep(e.value)
        except Exception as e:
            logger.warning(f"Failed to edit progress message: {str(e)}")
            # don't spam retries

    # Create initial progress message if not exists
    upload_target_display = "📢 Dump Channel" if dump_channel else "💬 This Chat"
    if current_progress_msg is None:
        try:
            current_progress_msg = await bot.send_message(
                chat_id,
                f"📤 **Starting Upload to {upload_target_display}**\n\n📄 {file_name}\n📦 Size: {humanize.naturalsize(file_size)}"
            )
            upload_file_with_progress.upload_messages[chat_id] = current_progress_msg
            logger.info(f"📤 Created upload progress message for: {file_name} to {upload_target_display}")
        except Exception as e:
            logger.warning(f"Could not create initial progress message: {str(e)}")
            current_progress_msg = None

    # Now perform uploads to each target in upload_targets in sequence (keeps your single-progress-message approach)
    try:
        for target_type, target_id in upload_targets:
            target_name = {
                "dump": "📢 Dump Channel",
                "chat": "💬 Your Chat",
                "showcase": "🌟 Showcase Channel"
            }.get(target_type, "Unknown")

            # If showcase is requested as forward mode, skip uploading directly to showcase
            if target_type == "showcase" and SHOWCASE_FORMAT == "forward":
                # We'll forward after primary uploads are done
                logger.info("Skipping direct upload to showcase since SHOWCASE_FORMAT == 'forward'")
                continue

            target_chat_id = target_id
            logger.info(f"🚀 Uploading to {target_name}: {file_name}")

            try:
                if upload_mode == "document":
                    message = await bot.send_document(
                        chat_id=target_chat_id,
                        document=file_path,
                        thumb=thumbnail_path,
                        progress=progress_callback,
                        caption=await _get_upload_caption(metadata, user_info, target_type) if (target_type == "showcase" and SHOWCASE_FORMAT == "upload") else None
                    )
                else:
                    encoder = VideoEncoder()
                    video_info = await encoder.get_video_info(file_path)
                    message = await bot.send_video(
                        chat_id=target_chat_id,
                        video=file_path,
                        thumb=thumbnail_path,
                        duration=int(video_info.get('duration', 0)),
                        width=video_info['video_streams'][0]['width'] if video_info.get('video_streams') else 0,
                        height=video_info['video_streams'][0]['height'] if video_info.get('video_streams') else 0,
                        supports_streaming=True,
                        progress=progress_callback,
                        caption=await _get_upload_caption(metadata, user_info, target_type) if (target_type == "showcase" and SHOWCASE_FORMAT == "upload") else None
                    )

                uploaded_messages[target_type] = message
                success_count += 1
                logger.info(f"✅ Uploaded to {target_name}: {file_name}")

                # small pause to avoid flood issues (optional but safe)
                await asyncio.sleep(0.5)

            except Exception as e:
                logger.error(f"❌ Failed to upload to {target_type} {target_id}: {str(e)}")
                # Only notify the user if primary upload (chat) fails
                if target_type == "chat":
                    try:
                        await bot.send_message(chat_id, f"❌ Failed to upload to {target_name}: {str(e)}")
                    except Exception as _:
                        pass
                # continue to next target

        # Handle showcase forwarding if enabled and configured as forward
        if SHOWCASE_ENABLED and SHOWCASE_FORMAT == "forward" and SHOWCASE_CHANNEL:
            try:
                # Prefer dump upload as forward source, fallback to chat
                source_message = uploaded_messages.get("dump") or uploaded_messages.get("chat")

                if source_message:
                    forwarded_msg = await source_message.forward(SHOWCASE_CHANNEL)
                    if user_info:
                        caption = await _get_showcase_caption(metadata, user_info)
                        try:
                            await forwarded_msg.edit_caption(caption)
                            logger.info(f"✅ Forwarded to showcase channel and caption edited: {file_name}")
                            success_count += 1
                        except Exception as e:
                            logger.warning(f"Could not edit showcase caption (forward mode): {str(e)}")
                            try:
                                await bot.send_message(SHOWCASE_CHANNEL, caption)
                                logger.info("📎 Sent separate showcase caption message below forwarded post")
                                success_count += 1
                            except Exception as e2:
                                logger.error(f"Failed to send fallback showcase caption: {str(e2)}")
                else:
                    logger.warning("⚠️ No valid source message found to forward to showcase.")

            except Exception as e:
                logger.error(f"❌ Failed to forward to showcase: {str(e)}")
        logger.info(f"✅ Upload loop completed for {file_name} (success_count={success_count})")
        try:
            current_msg = upload_file_with_progress.upload_messages.get(chat_id)
            if current_msg and hasattr(current_msg, 'delete'):
                await current_msg.delete()
                logger.info(f"🧹 Deleted upload progress message for {file_name}")
            if chat_id in upload_file_with_progress.upload_messages:
                del upload_file_with_progress.upload_messages[chat_id]
        except Exception as e:
            logger.warning(f"Failed to delete progress message: {str(e)}")

        return success_count > 0
        if chat_id in upload_file_with_progress.upload_messages_meta:
            del upload_file_with_progress.upload_messages_meta[chat_id]
    except TaskCancelled:
        logger.info(f"Upload cancelled for {file_name}")
        try:
            current_msg = upload_file_with_progress.upload_messages.get(chat_id)
            if current_msg and hasattr(current_msg, 'edit_text'):
                await current_msg.edit_text("❌ Upload cancelled by user")
        except Exception as e:
            logger.warning(f"Failed to update progress message on cancellation: {str(e)}")
        return False

    except Exception as e:
        logger.error(f"Upload failed for {file_name}: {str(e)}")
        try:
            current_msg = upload_file_with_progress.upload_messages.get(chat_id)
            if current_msg and hasattr(current_msg, 'edit_text'):
                await current_msg.edit_text(f"❌ Upload failed: {str(e)[:100]}")
        except Exception as update_error:
            logger.warning(f"Failed to update progress message with error: {str(update_error)}")
        return False

async def handle_download_and_process(chat_id: int, magnet_link: str, task_id: str = None):
    """Handle download and process in one go without asking settings again - FIXED VERSION"""
    logger.info(f"🔧 handle_download_and_process called with: chat_id={chat_id}, magnet_link={magnet_link[:50]}..., task_id={task_id}")
    download_dir = None
    try:
        session = await get_user_settings(chat_id)
        if not session:
            await update_progress(chat_id, "❌ Session expired", force_new=True)
            return
        await cleanup_settings_messages(chat_id)
        # Download torrent
        download_dir, files = await handle_torrent_download(chat_id, magnet_link)
        # Check if user cancelled during download
        user_event = await get_user_cancellation_event(chat_id)
        if user_event.is_set():
            raise TaskCancelled("Download cancelled by user")
        # Process files immediately using existing settings
        await update_user_settings(chat_id, {
            "download_path": download_dir,
            "files": files,
            "status": "batch_downloaded" if len(files) > 1 else "downloaded",
            "file_path": files[0]['path'] if files else None
        })
        await start_processing(chat_id, task_id)
    except TaskCancelled:
        logger.info(f"Download cancelled by user {chat_id}")
        await update_progress(chat_id, "❌ Download cancelled by user", force_new=True)
    except Exception as e:
        logger.error(f"Download failed: {str(e)}", exc_info=True)
        await update_progress(chat_id, f"❌ Download failed: {str(e)}", force_new=True)
    finally:
        if download_dir and ospath.exists(download_dir):
            try:
                shutil.rmtree(download_dir, ignore_errors=True)
            except Exception as e:
                logger.warning(f"Failed to cleanup download directory: {str(e)}")
async def start_processing(chat_id: int, task_id: str = None, **kwargs):
    """Start processing with global queue - handles both compress and magnet tasks - FIXED VERSION"""
    session = None
    processing_started = False
    process_start_time = time.time()
    download_dir_to_cleanup = None
    file_path_to_cleanup = None
    try:
        logger.info(f"🚀 start_processing called for chat_id: {chat_id}, task_id: {task_id}")
        session = await get_user_settings(chat_id)
        is_all_qualities = session.get("all_qualities", False)
        selected_qualities = session.get("selected_qualities")
        if is_all_qualities and selected_qualities:
            logger.info(f"🔄 All Qualities mode detected for {chat_id}, selected qualities: {selected_qualities}")
        await cleanup_progress_messages(chat_id)
        if not session:
            await update_progress(chat_id, "❌ Session expired", force_new=True)
            return
        files = []
        task_type = session.get("task_type", "magnet")
        logger.info(f"📋 Task type: {task_type}, Status: {session.get('status')}, All Qualities: {is_all_qualities}")
        if task_type == "compress":
            if "files" in session and session["files"]:
                files = session["files"]
                logger.info(f"✅ Using existing compress files: {len(files)} files")
            elif "file_path" in session and ospath.exists(session["file_path"]):
                file_path = session["file_path"]
                files = [{
                    'path': file_path,
                    'size': ospath.getsize(file_path),
                    'name': ospath.basename(file_path),
                    'is_video': True
                }]
                file_path_to_cleanup = file_path  
                download_dir_to_cleanup = ospath.dirname(file_path)  
                logger.info(f"✅ Using file_path: {file_path}")
            else:
                await update_progress(chat_id, "❌ No file to process! Please use /compress again.", force_new=True)
                return
        else:
            if session.get("files"):
                files = [f for f in session["files"] if isinstance(f, dict) and f.get('path') and ospath.exists(f['path'])]
                logger.info(f"✅ Found {len(files)} magnet files via FILES list")
                if files and "download_path" in session:
                    download_dir_to_cleanup = session["download_path"]
            elif session.get("status") == "batch_downloaded" and session.get("file_path"):
                logger.warning(f"⚠️ Status is batch_downloaded but no files list found for {chat_id}")
                if ospath.exists(session["file_path"]):
                    files = [{
                        'path': session["file_path"],
                        'size': ospath.getsize(session["file_path"]),
                        'name': ospath.basename(session["file_path"]),
                        'is_video': True
                    }]
                    file_path_to_cleanup = session["file_path"]  
                    download_dir_to_cleanup = session.get("download_path")  
                    logger.info(f"✅ Fallback to single file: {session['file_path']}")
            else:
                logger.error("❌ No files found in session for magnet task")
                await update_progress(chat_id, "❌ No files found to process. Please try the download again.", force_new=True)
                return
        if not files:
            logger.error("❌ No valid files to process after all checks")
            await update_progress(chat_id, "❌ No valid files found to process. Please try again.", force_new=True)
            return
        processing_started = True
        logger.info(f"🟢 Processing STARTED for {chat_id} with {len(files)} files, All Qualities: {is_all_qualities}")
        await update_user_settings(chat_id, {"status": "processing"})
        codec = session.get("codec", "h264")
        preset = session.get("preset", "veryfast")
        watermark_enabled = session.get("watermark_enabled", False)
        quality = session.get("quality", "720p")
        samples_enabled = session.get("samples_enabled", False)
        screenshots_enabled = session.get("screenshots_enabled", False)
        if is_all_qualities and selected_qualities:
            selected_text = ", ".join(selected_qualities)
            await update_progress(
                chat_id,
                f"🚀 Starting All Qualities Processing\n\n"
                f"📂 Total Files: {len(files)}\n"
                f"🎯 Qualities per file: {selected_text}\n"
                f"🔤 Codec: {codec.upper()}\n"
                f"⚡ Preset: {preset.upper()}\n"
                f"💧 Watermark: {'Yes' if watermark_enabled else 'No'}\n\n"
                f"Creating {len(selected_qualities)} versions for each file...",
                force_new=True,
                start_time=process_start_time
            )
            await asyncio.sleep(1)
            logger.info(f"🎯 Calling process_batch with ALL_QUALITIES=True and {len(files)} files")
            await process_batch(
                chat_id=chat_id,
                files=files,  # 🎯 This should now contain ALL files
                quality=session.get("quality", "720p"),
                metadata=session.get("metadata", {}),
                watermark_path=session.get("watermark"),
                watermark_enabled=session.get("watermark_enabled", False),
                thumbnail_path=session.get("thumbnail"),
                upload_mode=session.get("upload_mode", "video"),
                codec=session.get("codec", "h264"),
                remname=session.get("remname"),
                subtitle_mode=session.get("subtitle_mode", "keep"),
                subtitle_file=session.get("subtitle_file"),
                audio_mode=session.get("audio_mode", "keep"),
                audio_file=session.get("audio_file"),
                custom_crf=session.get("custom_crf"),
                preset=session.get("preset", "veryfast"),
                samples_enabled=session.get("samples_enabled", False),
                screenshots_enabled=session.get("screenshots_enabled", False),
                task_id=task_id,
                is_all_qualities=True,  # 🎯 Pass All Qualities flag
                selected_qualities=selected_qualities,  # 🎯 Pass selected qualities
                video_tune=session.get("video_tune", "none"),  # 🆕 ADD THIS
                watermark_position=session.get("watermark_position", "bottom-right"),
                watermark_scale=session.get("watermark_scale", "10%"),
                watermark_opacity=session.get("watermark_opacity", "70%")
            )
        else:
            await process_batch(
                chat_id=chat_id,
                files=files,
                quality=session.get("quality", "720p"),
                metadata=session.get("metadata", {}),
                watermark_path=session.get("watermark"),
                watermark_enabled=session.get("watermark_enabled", False),
                thumbnail_path=session.get("thumbnail"),
                upload_mode=session.get("upload_mode", "video"),
                codec=session.get("codec", "h264"),
                remname=session.get("remname"),
                subtitle_mode=session.get("subtitle_mode", "keep"),
                subtitle_file=session.get("subtitle_file"),
                audio_mode=session.get("audio_mode", "keep"),
                audio_file=session.get("audio_file"),
                custom_crf=session.get("custom_crf"),
                preset=session.get("preset", "veryfast"),
                samples_enabled=session.get("samples_enabled", False),
                screenshots_enabled=session.get("screenshots_enabled", False),
                video_tune=session.get("video_tune", "none"),  # 🆕 ADD THIS
                task_id=task_id,
                watermark_position=session.get("watermark_position", "bottom-right"),
                watermark_scale=session.get("watermark_scale", "10%"),
                watermark_opacity=session.get("watermark_opacity", "70%")
            )
        logger.info(f"🎉 Processing COMPLETED successfully for {chat_id}")
    except TaskCancelled:
        logger.info(f"🛑 Processing CANCELLED by user {chat_id}")
        await update_progress(chat_id, "❌ Processing cancelled by user", force_new=True)
    except Exception as e:
        logger.error(f"💥 Processing FAILED for {chat_id}: {str(e)}", exc_info=True)
        await update_progress(chat_id, f"❌ Failed to start processing: {str(e)}", force_new=True)
    finally:
        if processing_started:
            try:
                if task_type == "compress":
                    await cleanup_compress_files(chat_id)
                    logger.info(f"🧹 Cleaned compress files for {chat_id}")
                else:
                    if download_dir_to_cleanup and ospath.exists(download_dir_to_cleanup):
                        shutil.rmtree(download_dir_to_cleanup, ignore_errors=True)
                        logger.info(f"🧹 Cleaned magnet download path: {download_dir_to_cleanup}")
                    if file_path_to_cleanup and ospath.exists(file_path_to_cleanup):
                        try:
                            osremove(file_path_to_cleanup)
                            logger.info(f"🧹 Cleaned magnet file: {file_path_to_cleanup}")
                        except Exception as e:
                            logger.warning(f"Failed to delete file {file_path_to_cleanup}: {str(e)}")
                await cleanup_generated_files(chat_id)
                logger.info(f"🧹 Cleaned generated files for {chat_id}")
            except Exception as e:
                logger.error(f"🧹 Final cleanup error in start_processing: {str(e)}")
        else:
            logger.info(f"⏸️  Skipping cleanup - processing never started for {chat_id}")
async def cleanup_generated_files(chat_id: int):
    """Clean up screenshots, samples, encoded files, and other generated files"""
    try:
        folders_to_clean = ["screenshots", "samples", "encoded", "tempwork", "previews"]
        total_deleted = 0
        for folder in folders_to_clean:
            if not ospath.exists(folder) or not ospath.isdir(folder):
                continue
            try:
                for file in os.listdir(folder):
                    file_path = ospath.join(folder, file)
                    try:
                        if f"_{chat_id}_" in file or file.startswith(f"preview_{chat_id}"):
                            if ospath.isfile(file_path):
                                osremove(file_path)
                                total_deleted += 1
                                logger.debug(f"🧹 Deleted {file_path}")
                            elif ospath.isdir(file_path):
                                shutil.rmtree(file_path, ignore_errors=True)
                                total_deleted += 1
                                logger.debug(f"🧹 Deleted directory {file_path}")
                    except Exception as e:
                        logger.warning(f"🧹 Failed to cleanup {file_path}: {str(e)}")
                current_time = time.time()
                for file in os.listdir(folder):
                    file_path = ospath.join(folder, file)
                    try:
                        if ospath.isfile(file_path):
                            file_age = current_time - ospath.getmtime(file_path)
                            if file_age > 18000:  # 2 hours
                                osremove(file_path)
                                total_deleted += 1
                                logger.debug(f"🧹 Deleted old file {file_path} (age: {file_age:.0f}s)")
                        elif ospath.isdir(file_path):
                            dir_age = current_time - ospath.getmtime(file_path)
                            if dir_age > 18000:  # 2 hours
                                shutil.rmtree(file_path, ignore_errors=True)
                                total_deleted += 1
                                logger.debug(f"🧹 Deleted old directory {file_path}")
                    except Exception as e:
                        logger.warning(f"🧹 Failed to cleanup old file {file_path}: {str(e)}")
            except Exception as e:
                logger.warning(f"🧹 Failed to process folder {folder}: {str(e)}")
        logger.info(f"🧹 Generated files cleanup completed for {chat_id}: {total_deleted} items deleted")
        return total_deleted
    except Exception as e:
        logger.error(f"🧹 Generated files cleanup error for {chat_id}: {str(e)}")
        return 0
async def cleanup_storage():
    """ACTUALLY clean storage - Enhanced version"""
    try:
        folders_to_clean = ["downloads", "encoded", "tempwork", "previews", "screenshots", "samples", "subtitles", "audio"]
        total_freed = 0
        deleted_files = 0
        current_time = time.time()
        for folder in folders_to_clean:
            if not ospath.exists(folder):
                continue
            for item in os.listdir(folder):
                item_path = ospath.join(folder, item)
                try:
                    if ospath.isfile(item_path):
                        file_age = current_time - ospath.getmtime(item_path)
                        if file_age > 23000:  # 1 hour
                            file_size = ospath.getsize(item_path)
                            osremove(item_path)
                            total_freed += file_size
                            deleted_files += 1
                    elif ospath.isdir(item_path):
                        dir_age = current_time - ospath.getmtime(item_path)
                        if dir_age > 23000:  # 1 hour
                            shutil.rmtree(item_path, ignore_errors=True)
                            deleted_files += 1
                except Exception as e:
                    logger.warning(f"Failed to delete {item_path}: {str(e)}")
                    continue
        logger.info(f"🧹 Storage cleanup: {deleted_files} files deleted, {humanize.naturalsize(total_freed)} freed")
        return deleted_files, total_freed
    except Exception as e:
        logger.error(f"Cleanup failed: {str(e)}")
        return 0, 0
async def generate_samples(file_path: str, chat_id: int, output_dir: str) -> List[str]:
    """Generate a single 45–60s sample clip from video with proper error handling."""
    try:
        samples_dir = ospath.join(output_dir, "samples")
        makedirs(samples_dir, exist_ok=True)
        encoder = VideoEncoder()
        video_info = await encoder.get_video_info(file_path)
        duration = video_info.get('duration', 0)
        if duration < 20:  # Too short for a useful sample
            logger.info(f"Video too short for sample: {duration}s")
            return []
        # Pick sample duration: prefer 60s, fallback to 45s if near end
        sample_duration = 60 if duration > 75 else 45
        start_time = max(0, (duration / 2) - (sample_duration / 2))  # middle of the video
        base_name = ospath.splitext(ospath.basename(file_path))[0]
        sample_output = ospath.join(samples_dir, f"sample_{base_name}.mp4")
        cmd = [
            'ffmpeg', '-hide_banner', '-loglevel', 'error',
            '-ss', str(int(start_time)),
            '-i', file_path,
            '-t', str(sample_duration),
            '-c:v', 'libx264',          # Re-encode for compatibility
            '-c:a', 'aac',
            '-preset', 'fast',
            '-crf', '23',
            '-avoid_negative_ts', 'make_zero',
            '-y', sample_output
        ]
        logger.info(f"🎬 Generating single sample for {file_path} [{sample_duration}s at {int(start_time)}s]")
        try:
            process = await asyncio.create_subprocess_exec(*cmd)
            return_code = await process.wait()
            if return_code == 0 and ospath.exists(sample_output) and ospath.getsize(sample_output) > 0:
                logger.info(f"✅ Sample generated: {sample_output}")
                return [sample_output]
            else:
                logger.warning(f"⚠️ Sample generation failed, return code: {return_code}")
                return []
        except Exception as e:
            logger.warning(f"❌ FFmpeg failed: {str(e)}")
            return []
    except Exception as e:
        logger.error(f"Sample generation failed for {file_path}: {str(e)}")
        return []
async def generate_screenshots(file_path: str, chat_id: int, output_dir: str) -> List[str]:
    """Generate screenshots from video"""
    try:
        screenshots_dir = ospath.join(output_dir, "screenshots")
        makedirs(screenshots_dir, exist_ok=True)
        encoder = VideoEncoder()
        video_info = await encoder.get_video_info(file_path)
        duration = video_info['duration']
        screenshot_times = []
        # Generate 3-5 screenshots throughout the video
        if duration > 300:  # Long video - 5 screenshots
            intervals = duration // 6
            screenshot_times = [intervals * i for i in range(1, 6)]
        else:  # Shorter video - 3 screenshots
            intervals = duration // 4
            screenshot_times = [intervals * i for i in range(1, 4)]
        screenshot_files = []
        for i, timestamp in enumerate(screenshot_times):
            if timestamp >= duration:
                continue
            screenshot_output = ospath.join(screenshots_dir, f"screenshot_{i+1}_{ospath.basename(file_path)}.jpg")
            cmd = [
                'ffmpeg', '-hide_banner', '-loglevel', 'error',
                '-ss', str(timestamp),
                '-i', file_path,
                '-vframes', '1',
                '-q:v', '2',  # Quality 2-31 (2 = best)
                '-y', screenshot_output
            ]
            try:
                process = await asyncio.create_subprocess_exec(*cmd)
                await process.wait()
                if process.returncode == 0 and ospath.exists(screenshot_output):
                    # Resize screenshot to reasonable size
                    try:
                        with Image.open(screenshot_output) as img:
                            img.thumbnail((1280, 720))  # Max 1280x720
                            img.save(screenshot_output, "JPEG", quality=85)
                    except:
                        pass
                    screenshot_files.append(screenshot_output)
                    logger.info(f"✅ Generated screenshot {i+1}: {screenshot_output}")
            except Exception as e:
                logger.warning(f"Failed to generate screenshot {i+1}: {str(e)}")
                continue
        return screenshot_files
    except Exception as e:
        logger.error(f"Screenshot generation failed: {str(e)}")
        return []
@handle_floodwait
async def update_progress(
    chat_id: int,
    text: str,
    force_new: bool = False,
    progress: Optional[float] = None,
    reply_markup: Optional[InlineKeyboardMarkup] = None,
    start_time: Optional[float] = None
) -> bool:
    """Enhanced global progress update with STABLE ETA calculation"""
    try:
        current_time = time.time()
        if start_time is None:
            start_time = current_time
        # Flood protection - only update every 3 seconds minimum
        if not force_new and chat_id in progress_message_tracker:
            last_update = progress_message_tracker[chat_id].get('last_update', 0)
            if current_time - last_update < 7:
                return False
        # Add progress bar if progress is provided
        display_text = text
        if progress is not None:
            progress_bar_length = 12
            filled_length = int(progress / 100 * progress_bar_length)
            progress_bar = get_random_progress_bar(progress, length=progress_bar_length)
            # Calculate timing information - STABLE VERSION
            elapsed = current_time - start_time
            # Format elapsed time properly (no floating point issues)
            elapsed_seconds = int(elapsed)
            if elapsed_seconds < 60:
                elapsed_str = f"{elapsed_seconds}s"
            elif elapsed_seconds < 3600:
                minutes = elapsed_seconds // 60
                seconds = elapsed_seconds % 60
                elapsed_str = f"{minutes}m {seconds}s"
            else:
                hours = elapsed_seconds // 3600
                minutes = (elapsed_seconds % 3600) // 60
                elapsed_str = f"{hours}h {minutes}m"
            # STABLE ETA calculation - only show if we have meaningful progress
            eta_str = ""
            if 5 <= progress <= 95:
                # Use simple linear estimation with minimum 10 seconds
                total_estimated = (elapsed / progress) * 100 if progress > 0 else 0
                remaining = max(10, total_estimated - elapsed)  # Minimum 10 seconds
                # Format ETA properly without floating point issues
                remaining_seconds = int(remaining)
                if remaining_seconds < 60:
                    eta_str = f"| ⏳ ETA: {remaining_seconds}s"
                elif remaining_seconds < 3600:
                    minutes = remaining_seconds // 60
                    seconds = remaining_seconds % 60
                    eta_str = f"| ⏳ ETA: {minutes}m {seconds}s"
                else:
                    hours = remaining_seconds // 3600
                    minutes = (remaining_seconds % 3600) // 60
                    eta_str = f"| ⏳ ETA: {hours}h {minutes}m"
            display_text = (
                f"{text}\n"
                f"{progress_bar} {progress:.1f}%\n"
                f"⏱️ {elapsed_str} {eta_str}"
            )
        try:
            # Check if we have a tracked progress message for this chat
            if chat_id in progress_message_tracker and not force_new:
                message_data = progress_message_tracker[chat_id]
                message_id = message_data.get('message_id')
                if message_id:
                    try:
                        await bot.edit_message_text(
                            chat_id=chat_id,
                            message_id=message_id,
                            text=display_text,
                            reply_markup=reply_markup
                        )
                        # Update last update time
                        progress_message_tracker[chat_id]['last_update'] = current_time
                        return True
                    except MessageNotModified:
                        return True
                    except (BadRequest, Exception) as e:
                        logger.warning(f"Failed to edit message {message_id}: {str(e)}")
                        # Message doesn't exist or can't be edited, create new one
                        pass
            # Create new message (either no existing message or edit failed)
            msg = await bot.send_message(
                chat_id=chat_id, 
                text=display_text,
                reply_markup=reply_markup
            )
            # Track the new message with additional data
            progress_message_tracker[chat_id] = {
                'message': msg,
                'message_id': msg.id,
                'last_update': current_time,
                'start_time': start_time
            }
            logger.info(f"📝 Created/Updated progress message {msg.id} for chat {chat_id}")
            return True
        except FloodWait as e:
            logger.warning(f"FloodWait: Waiting {e.value}s")
            await asyncio.sleep(e.value)
            return False
        except Exception as e:
            logger.warning(f"Failed to send progress message: {str(e)}")
            return False
    except Exception as e:
        logger.error(f"Progress update failed: {str(e)}")
        return False
async def ask_for_quality(chat_id: int):
    """Show enhanced video quality selection menu with codec-aware settings"""
    try:
        session = await get_user_settings(chat_id)
        if not session:
            await bot.send_message(chat_id, "⚠️ Session expired. Please start again with /magnet.")
            return
            
        current_codec = session.get("codec", "h264")
        current_preset = session.get("preset", "medium")
        current_profile = session.get("profile", "main")
        current_pixel_format = session.get("pixel_format", "yuv420p")

        # 🎯 CODEC-SPECIFIC COMPATIBILITY INFO
        codec_info = {
            "h264": "🔸 Best compatibility, fast encoding",
            "h265": "🔸 Better compression, good compatibility", 
            "av1": "🔸 Best compression, newer devices only"
        }

        # 🎯 SHOW ONLY COMPATIBLE SETTINGS FOR CURRENT CODEC
        compatible_profiles = list(CODEC_PROFILES.get(current_codec, {}).keys())
        compatible_pixel_formats = []
        
        for profile in compatible_profiles:
            formats = COMPATIBILITY_MATRIX.get(current_codec, {}).get(profile, [])
            compatible_pixel_formats.extend(formats)
        
        compatible_pixel_formats = list(set(compatible_pixel_formats))

        buttons = [
            [
                InlineKeyboardButton("🎯 SELECT QUALITIES 🚀", callback_data=f"all_qualities_{chat_id}"),
            ],
            [
                InlineKeyboardButton(f"🔤 {current_codec.upper()}", callback_data=f"toggle_codec_{chat_id}"),
                InlineKeyboardButton(f"⚡ {current_preset.title()}", callback_data=f"set_preset_{chat_id}") 
            ],
            [
                InlineKeyboardButton(f"📊 {current_profile}", callback_data=f"set_profile_{chat_id}"),
                InlineKeyboardButton(f"🎨 {current_pixel_format}", callback_data=f"set_pixel_format_{chat_id}")
            ],
            [
                InlineKeyboardButton("⚙️ SETTINGS", callback_data=f"settings_menu_{chat_id}"),
                InlineKeyboardButton("❌ CLOSE", callback_data=f"close_menu_{chat_id}")
            ]
        ]
        
        caption = (
            f"🎯 <b>Video Quality Selection</b>\n\n"
            f"<blockquote expandable>"
            f"Current Codec: <b>{current_codec.upper()}</b>\n"
            f"{codec_info.get(current_codec, '')}\n\n"
            f"💡 <b>Recommended for {current_codec.upper()}:</b>\n"
        )
        
        # Add codec-specific recommendations
        if current_codec == "h265":
            caption += "• Profile: main (best compatibility)\n• Pixel: yuv420p\n• Preset: medium\n"
        elif current_codec == "av1":
            caption += "• Profile: main\n• Pixel: yuv420p\n• Preset: medium (faster)\n"
        else:  # h264
            caption += "• Profile: main\n• Pixel: yuv420p\n• Preset: medium\n"
            
        caption += (
            f"</blockquote>"
            f"<blockquote>"
            f"🛠 <b>Current Settings:</b>\n"
            f"• Codec: {current_codec.upper()}\n"
            f"• Preset: {current_preset.title()}\n"
            f"• Profile: {current_profile}\n"
            f"• Pixel: {current_pixel_format}\n"
            "</blockquote>"
            "<blockquote>🚀 Hosted on Heroku </blockquote>\n"
            "<blockquote>"
            "💡<b>NOTE:</b> Bot only Uplodes 2GB Below </blockquote>"

        )
        
        # Send or edit the quality menu
        try:
            if chat_id in user_active_messages:
                await bot.edit_message_media(
                    chat_id=chat_id,
                    message_id=user_active_messages[chat_id],
                    media=InputMediaPhoto(
                        media="https://envs.sh/X60.jpg",
                        caption=caption,
                        parse_mode=ParseMode.HTML
                    ),
                    reply_markup=InlineKeyboardMarkup(buttons)
                )
            else:
                msg = await bot.send_photo(
                    chat_id=chat_id,
                    photo="https://envs.sh/X60.jpg",
                    caption=caption,
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup(buttons)
                )
                user_active_messages[chat_id] = msg.id
                
        except Exception as e:
            logger.warning(f"Failed to edit quality menu: {str(e)}")
            msg = await bot.send_photo(
                chat_id=chat_id,
                photo="https://files.catbox.moe/baybei.jpg",
                caption=caption,
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(buttons)
            )
            user_active_messages[chat_id] = msg.id
            
    except Exception as e:
        logger.error(f"Error displaying quality menu for {chat_id}: {e}")
        await bot.send_message(chat_id, "❌ Failed to show quality options. Please try again.")
@bot.on_callback_query(filters.regex(r"^toggle_codec_(\d+)$"))
@handle_floodwait
async def toggle_codec_handler(client: Client, query: CallbackQuery):
    """Handle codec toggle with AV1 availability check - FIXED VERSION"""
    try:
        chat_id = int(query.data.split("_")[2])
        session = await get_user_settings(chat_id)
        if not session:
            return await query.answer("Session expired! Start over with /magnet", show_alert=True)
        
        current_codec = session.get("codec", "h264")
        codecs = ["h264", "h265", "av1"]
        current_index = codecs.index(current_codec) if current_codec in codecs else 0
        
        # Find next available codec
        for i in range(1, len(codecs) + 1):
            next_index = (current_index + i) % len(codecs)
            new_codec = codecs[next_index]
            
            # Check if AV1 is available
            if new_codec == "av1":
                if not await check_svtav1_available():
                    if i == len(codecs):  # If no codecs available
                        await query.answer("❌ No alternative codecs available!", show_alert=True)
                        return
                    continue  # Skip AV1 if not available
            
            # Valid codec found
            await update_user_settings(chat_id, {"codec": new_codec})
            
            # Auto-adjust settings for codec compatibility
            if new_codec == "h265":
                # Set compatible defaults for H.265
                await update_user_settings(chat_id, {
                    "pixel_format": "yuv420p",
                    "profile": "main",
                    "preset": "medium"
                })
            elif new_codec == "av1":
                # Set compatible defaults for AV1
                await update_user_settings(chat_id, {
                    "pixel_format": "yuv420p", 
                    "profile": "main",
                    "preset": "medium"
                })
            
            await query.answer(f"✅ Codec set to {AVAILABLE_CODECS[new_codec]}")
            await ask_for_quality(chat_id)
            break
            
    except Exception as e:
        logger.error(f"Codec toggle error: {str(e)}")
        await query.answer("Failed to toggle codec!", show_alert=True)
@bot.on_callback_query(filters.regex(r"^set_profile_(\d+)$"))
async def set_profile_handler(client: Client, query: CallbackQuery):
    """Handle profile selection"""
    try:
        chat_id = int(query.data.split("_")[2])
        session = await get_user_settings(chat_id)
        if not session:
            return await query.answer("Session expired!", show_alert=True)
        
        current_codec = session.get("codec", "h264")
        current_profile = session.get("profile", "main")
        
        # Get available profiles for current codec
        available_profiles = CODEC_PROFILES.get(current_codec, {})
        
        if not available_profiles:
            await query.answer("No profiles available for this codec!", show_alert=True)
            return
        
        buttons = []
        row = []
        for i, (profile_key, profile_name) in enumerate(available_profiles.items()):
            row.append(InlineKeyboardButton(
                f"{'✅' if profile_key == current_profile else '🔲'} {profile_name}",
                callback_data=f"profile_{profile_key}_{chat_id}"
            ))
            if len(row) == 2 or i == len(available_profiles) - 1:
                buttons.append(row)
                row = []
        
        buttons.append([InlineKeyboardButton("🔙 Back to Quality", callback_data=f"set_quality_{chat_id}")])
        
        profile_info = (
            f"📊 <b>Select {current_codec.upper()} Profile</b>\n\n"
            "<blockquote expandable>"
            "Video profiles determine feature compatibility:\n\n"
        )
        
        if current_codec == 'h265':
            profile_info += (
                "• <b>Main</b>: Best compatibility (8-bit)\n"
                "• <b>Main10</b>: HDR support (10-bit)\n"
                "• <b>Main444</b>: Full color quality\n\n"
                "💡 <b>Recommended:</b> Main for compatibility\n"
            )
        elif current_codec == 'h264':
            profile_info += (
                "• <b>Baseline</b>: Maximum device compatibility\n"
                "• <b>Main</b>: Good balance (recommended)\n"
                "• <b>High</b>: Best quality\n\n"
                "💡 <b>Recommended:</b> Main for most uses\n"
            )
        else:
            profile_info += "Select profile based on your needs\n"
        
        profile_info += f"</blockquote><b>Current:</b> {available_profiles.get(current_profile, 'Auto')}"
        
        await query.message.edit_text(
            profile_info,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        await query.answer()
    except Exception as e:
        logger.error(f"Profile handler error: {str(e)}")
        await query.answer("Failed to set profile!", show_alert=True)
@bot.on_callback_query(filters.regex(r"^profile_(main|main10|main12|main444-8|main444-10|main444-12|baseline|main|high|high10|professional)_(\d+)$"))
async def profile_selection_handler(client: Client, query: CallbackQuery):
    """Handle profile selection with compatibility checks"""
    try:
        profile = query.data.split("_")[1]
        chat_id = int(query.data.split("_")[2])
        session = await get_user_settings(chat_id)
        if not session:
            return await query.answer("Session expired!", show_alert=True)
        
        current_codec = session.get("codec", "h264")
        current_pixel_format = session.get("pixel_format", "yuv420p")
        
        # Check compatibility
        compatible_formats = COMPATIBILITY_MATRIX.get(current_codec, {}).get(profile, [])
        
        if compatible_formats and current_pixel_format not in compatible_formats:
            # Auto-adjust pixel format for compatibility
            recommended_format = compatible_formats[0]
            await update_user_settings(chat_id, {
                "profile": profile,
                "pixel_format": recommended_format
            })
            await query.answer(
                f"✅ Profile set to {profile}\n"
                f"Pixel format auto-adjusted to {recommended_format} for compatibility",
                show_alert=True
            )
        else:
            await update_user_settings(chat_id, {"profile": profile})
            await query.answer(f"✅ Profile set to {profile}")
        
        await ask_for_quality(chat_id)
        
    except Exception as e:
        logger.error(f"Profile selection error: {str(e)}")
        await query.answer("Failed to set profile!", show_alert=True)
async def set_pixel_format_handler(client: Client, query: CallbackQuery):
    """Handle pixel format selection"""
    try:
        chat_id = int(query.data.split("_")[2])
        session = await get_user_settings(chat_id)
        if not session:
            return await query.answer("Session expired!", show_alert=True)
        
        formats = [
            ["🎨 yuv420p (Compatible)", "pixfmt_yuv420p"],
            ["🌈 yuv420p10le (10-bit)", "pixfmt_yuv420p10le"],
            ["🎬 yuv422p (Pro)", "pixfmt_yuv422p"],
            ["💎 yuv444p (Full Quality)", "pixfmt_yuv444p"],
            ["🔙 Back", f"settings_menu_{chat_id}"]
        ]
        
        buttons = []
        for i in range(0, len(formats), 2):
            row = []
            for j in range(2):
                if i + j < len(formats):
                    row.append(InlineKeyboardButton(
                        formats[i+j][0], 
                        callback_data=f"{formats[i+j][1]}_{chat_id}"
                    ))
            buttons.append(row)
            
        await query.message.edit_text(
            "🎨 Select Pixel Format\n\n"
            "Different pixel formats affect color quality and compatibility:\n\n"
            "• **yuv420p** - Most compatible, good quality\n"
            "• **yuv420p10le** - 10-bit for HDR content\n"  
            "• **yuv422p** - Better color for professional use\n"
            "• **yuv444p** - Full color quality, larger files\n\n"
            "Recommended: yuv420p for compatibility",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        await query.answer()
    except Exception as e:
        logger.error(f"Pixel format handler error: {str(e)}")
        await query.answer("Failed to set pixel format!", show_alert=True)

@bot.on_callback_query(filters.regex(r"^pixfmt_(yuv420p|yuv420p10le|yuv422p|yuv444p)_(\d+)$"))
async def pixel_format_selection_handler(client: Client, query: CallbackQuery):
    """Handle pixel format selection"""
    try:
        pix_fmt = query.data.split("_")[1]
        chat_id = int(query.data.split("_")[2])
        session = await get_user_settings(chat_id)
        if not session:
            return await query.answer("Session expired!", show_alert=True)
        
        await update_user_settings(chat_id, {"pixel_format": pix_fmt})
        await query.answer(f"Pixel format set to {pix_fmt}")
        await collect_settings(chat_id)
    except Exception as e:
        logger.error(f"Pixel format selection error: {str(e)}")
        await query.answer("Failed to set pixel format!", show_alert=True)
@bot.on_callback_query(filters.regex(r"^set_pixel_format_(\d+)$"))
async def set_pixel_format_handler(client: Client, query: CallbackQuery):
    """Handle pixel format selection menu"""
    try:
        chat_id = int(query.data.split("_")[3])
        await query.answer("🎨 Selecting pixel format...")
        await show_pixel_format_selector(chat_id)
    except Exception as e:
        logger.error(f"Set pixel format handler error: {str(e)}")
        await query.answer("Failed to show pixel format options!", show_alert=True)

async def show_pixel_format_selector(chat_id: int):
    """Show pixel format selection menu"""
    try:
        session = await get_user_settings(chat_id)
        if not session:
            return
            
        current_pixel_format = session.get("pixel_format", "yuv420p")
        
        buttons = [
            [
                InlineKeyboardButton(
                    f"{'✅' if current_pixel_format == 'yuv420p' else '🔲'} yuv420p", 
                    callback_data=f"pixfmt_yuv420p_{chat_id}"
                ),
                InlineKeyboardButton(
                    f"{'✅' if current_pixel_format == 'yuv420p10le' else '🔲'} yuv420p10le", 
                    callback_data=f"pixfmt_yuv420p10le_{chat_id}"
                )
            ],
            [
                InlineKeyboardButton(
                    f"{'✅' if current_pixel_format == 'yuv422p' else '🔲'} yuv422p", 
                    callback_data=f"pixfmt_yuv422p_{chat_id}"
                ),
                InlineKeyboardButton(
                    f"{'✅' if current_pixel_format == 'yuv444p' else '🔲'} yuv444p", 
                    callback_data=f"pixfmt_yuv444p_{chat_id}"
                )
            ],
            [
                InlineKeyboardButton("🔙 Back to Settings", callback_data=f"settings_menu_{chat_id}")
            ]
        ]
        
        caption = (
            "🎨 <b>Select Pixel Format</b>\n\n"
            f"📊 <b>Current:</b> {PIXEL_FORMATS.get(current_pixel_format, current_pixel_format)}\n\n"
            "💡 <b>Format Guide:</b>\n"
            "• <b>yuv420p</b> - Best compatibility, all devices\n"
            "• <b>yuv420p10le</b> - 10-bit color, HDR content\n"
            "• <b>yuv422p</b> - Better color, professional use\n"
            "• <b>yuv444p</b> - Full quality, large files\n\n"
            "⚠️ <i>Not all formats work with all codecs</i>"
        )
        
        # Send or edit the selector
        try:
            await bot.edit_message_media(
                chat_id=chat_id,
                message_id=user_active_messages.get(chat_id),
                media=InputMediaPhoto(
                    media=SETTINGS_SUMMARY_PIC,  # Or use a dedicated image
                    caption=caption,
                    parse_mode=ParseMode.HTML
                ),
                reply_markup=InlineKeyboardMarkup(buttons)
            )
        except Exception as e:
            # If edit fails, send new message
            msg = await bot.send_photo(
                chat_id=chat_id,
                photo=SETTINGS_SUMMARY_PIC,
                caption=caption,
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(buttons)
            )
            user_active_messages[chat_id] = msg.id
            
    except Exception as e:
        logger.error(f"Error showing pixel format selector: {e}")
        await bot.send_message(chat_id, "❌ Failed to show pixel format selector. Please try again.")
@bot.on_callback_query(filters.regex(r"^set_preset_(\d+)$"))
async def set_preset_handler(client: Client, query: CallbackQuery):
    """Handle preset selection"""
    try:
        chat_id = int(query.data.split("_")[2])
        session = await get_user_settings(chat_id)
        if not session:
            return await query.answer("Session expired!", show_alert=True)
        presets = [
            ["⚡ Ultrafast", "preset_ultrafast"],
            ["🚀 Superfast", "preset_superfast"],
            ["💨 Veryfast", "preset_veryfast"],
            ["🐇 Faster", "preset_faster"],
            ["🏃 Fast", "preset_fast"],
            ["⚖️ Medium", "preset_medium"],
            ["🐢 Slow", "preset_slow"],
            ["🚶 Slower", "preset_slower"],
           # ["🦥 Veryslow", "preset_veryslow"],
            ["🔙 Back", f"settings_menu_{chat_id}"]
        ]
        buttons = []
        for i in range(0, len(presets), 2):
            row = []
            for j in range(2):
                if i + j < len(presets):
                    row.append(InlineKeyboardButton(
                        presets[i+j][0], 
                        callback_data=f"{presets[i+j][1]}_{chat_id}"
                    ))
            buttons.append(row)
        await query.message.edit_text(
            "⚡ Select Encoding Preset\n\n"
            "Faster presets = quicker encoding, larger files\n"
            "Slower presets = better compression, smaller files\n\n"
            "Recommended: Medium (balanced)",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        await query.answer()
    except Exception as e:
        logger.error(f"Preset handler error: {str(e)}")
        await query.answer("Failed to set preset!", show_alert=True)
@bot.on_callback_query(filters.regex(r"^preset_(ultrafast|superfast|veryfast|faster|fast|medium|slow|slower|veryslow)_(\d+)$"))
async def preset_selection_handler(client: Client, query: CallbackQuery):
    """Handle preset selection - FIXED TO EDIT SAME QUALITY MESSAGE"""
    try:
        preset = query.data.split("_")[1]
        chat_id = int(query.data.split("_")[2])
        session = await get_user_settings(chat_id)
        if not session:
            return await query.answer("Session expired!", show_alert=True)
        
        await update_user_settings(chat_id, {"preset": preset})
        await query.answer(f"Preset set to {preset}")
        
        # 🎯 FIX: Edit the SAME quality selection message with updated preset
        await ask_for_quality(chat_id)
        
    except Exception as e:
        logger.error(f"Preset selection error: {str(e)}")
        await query.answer("Failed to set preset!", show_alert=True)
@bot.on_callback_query(filters.regex(r"^settings_menu_(\d+)$"))
@handle_floodwait
async def settings_menu_handler(client: Client, query: CallbackQuery):
    """Handle settings menu callback with better UX"""
    try:
        chat_id = int(query.data.split("_")[2])
        await query.answer("⚙️ Loading settings...")
        await collect_settings(chat_id)
    except Exception as e:
        logger.error(f"Settings menu error: {str(e)}")
        await query.answer("Failed to load settings!", show_alert=True)
@bot.on_callback_query(filters.regex(r"^close_menu_(\d+)$"))
@handle_floodwait
async def close_menu_handler(client: Client, query: CallbackQuery):
    """Handle menu close callback"""
    try:
        chat_id = int(query.data.split("_")[2])
        await query.answer("Menu closed")
        if chat_id in user_active_messages:
            try:
                await bot.delete_messages(chat_id, user_active_messages[chat_id])
                del user_active_messages[chat_id]
            except Exception:
                pass
    except Exception as e:
        logger.error(f"Close menu error: {str(e)}")
async def collect_settings(chat_id: int):
    """Display enhanced settings summary for All Qualities Only"""
    try:
        session = await get_user_settings(chat_id)
        if not session:
            await bot.send_message(chat_id, "⚠️ Session expired. Please start again.")
            return
        
        # Force All Qualities mode
        if not session.get("all_qualities"):
            await update_user_settings(chat_id, {
                "quality": "all_qualities", 
                "all_qualities": True,
                "selected_qualities": session.get("selected_qualities", ["480p", "720p", "1080p", "noencode"])
            })
            session = await get_user_settings(chat_id)
        
        # Extract session data for All Qualities mode
        selected_qualities = session.get("selected_qualities", ["480p", "720p", "1080p", "noencode"])
        codec = session.get("codec", "h264")
        preset = session.get("preset", "veryfast")
        upload_mode = session.get("upload_mode", "video")
        watermark_enabled = session.get("watermark_enabled", False)
        watermark_position = session.get("watermark_position", "bottom-right")
        watermark_scale = session.get("watermark_scale", "10%")
        watermark_opacity = session.get("watermark_opacity", "70%")
        has_watermark_file = session.get("watermark") and ospath.exists(session["watermark"])
        has_thumb = "✅" if session.get("thumbnail") and ospath.exists(session["thumbnail"]) else "❌"
        has_title = "✅" if session.get("metadata", {}).get("title") else "❌"
        remname_pattern = session.get("remname")
        has_remname = "✅" + (remname_pattern if remname_pattern else " ❌")
        subtitle_mode = session.get("subtitle_mode", "keep")
        audio_mode = session.get("audio_mode", "keep")
        custom_crf = session.get("custom_crf")
        crf_display = "AUTO" if custom_crf is None else str(custom_crf)
        samples_enabled = session.get("samples_enabled", False)
        screenshots_enabled = session.get("screenshots_enabled", False)
        video_tune = session.get("video_tune", "none")
        pixel_format = session.get("pixel_format", "yuv420p")
        # Enhanced button layout for All Qualities Only
        buttons = [
            [
                
                InlineKeyboardButton(f"🎯QUALITIES & CODEC", callback_data="set_quality_" + str(chat_id)),

                InlineKeyboardButton(f"Mode: {upload_mode.upper()}", callback_data="toggle_upload_" + str(chat_id))
            ],
            [
                InlineKeyboardButton(f"{has_title} Metadata", callback_data="set_title_" + str(chat_id)),
               # InlineKeyboardButton(f"{has_remname} Rename", callback_data="set_remname_" + str(chat_id))
                InlineKeyboardButton(f"🎨 {PIXEL_FORMATS.get(pixel_format, pixel_format)}", callback_data="set_pixel_format_" + str(chat_id))

            ],
            [
                InlineKeyboardButton(f"📝Audio {subtitle_mode.replace('_', ' ').title()}", callback_data="set_subtitle_mode_" + str(chat_id)),
                InlineKeyboardButton(f"🔊Subtitles {audio_mode.title()}", callback_data="set_audio_mode_" + str(chat_id))
            ],
            [
                InlineKeyboardButton(f"🎛️ CRF: {crf_display}", callback_data="set_crf_" + str(chat_id)),
                InlineKeyboardButton(f"🎛️ Tune: {video_tune.upper()}", callback_data="set_tune_" + str(chat_id))
            ],
            [
                InlineKeyboardButton(f"🎬 Sample {'🟢' if samples_enabled else '🔴'}", callback_data="toggle_samples_" + str(chat_id)),
                InlineKeyboardButton(f"📸 SS {'🟢' if screenshots_enabled else '🔴'}", callback_data="toggle_screenshots_" + str(chat_id))
            ],
            [
                InlineKeyboardButton(f"{has_thumb} Thumbnail", callback_data="set_thumb_" + str(chat_id)),
                InlineKeyboardButton(f"💧 Watermark {'🟢' if watermark_enabled else '🔴'}", callback_data="set_wm_" + str(chat_id))
            ],
            [
                InlineKeyboardButton("🚀 Start Processing", callback_data="confirm_download_" + str(chat_id)),
                InlineKeyboardButton("Close", callback_data="close_menu_" + str(chat_id))
            ]
        ]
        
        # Enhanced caption for All Qualities
        selected_text = ", ".join(selected_qualities)
        caption = (
            "<blockquote expandable>"
            "🎯 <b>Encoding Settings:</b>\n"
            f"• Qualities: {selected_text}\n"
            f"• Codec: {AVAILABLE_CODECS.get(codec, codec.upper())}\n"
            f"• Preset: {preset.upper()}\n"
            f"• CRF: {crf_display}\n\n"
            f"• Pixel Format: {PIXEL_FORMATS.get(pixel_format, pixel_format)}\n"
            f"• Tune: {video_tune.upper()}\n"
            "💧 <b>Watermark:</b>\n"
            f"• Status: {'🟢 Enabled' if watermark_enabled else '🔴 Disabled'}\n"
            f"• Position: {WATERMARK_POSITIONS.get(watermark_position, 'Default')}\n"
            f"• Scale: {WATERMARK_SCALES.get(watermark_scale, 'Default')}\n"
            f"• Opacity: {WATERMARK_OPACITY.get(watermark_opacity, 'Default')}\n"
            f"• File: {'✅ Uploaded' if has_watermark_file else '❌ Not Set'}\n\n"
            "🔧 <b>Additional Settings:</b>\n"
            f"• Upload Mode: {upload_mode.upper()}\n"
            f"• Subtitles: {subtitle_mode.replace('_', ' ').title()}\n"
            f"• Audio: {audio_mode.title()}\n"
            f"• Samples: {'✅ On' if samples_enabled else '❌ Off'}\n"
            f"• Screenshots: {'✅ On' if screenshots_enabled else '❌ Off'}\n"
            f"• Thumbnail: {has_thumb}\n"
            f"• Metadata: {has_title}\n"
            f"• Rename: {has_remname}\n"
            "</blockquote>"
            "<blockquote>"
            "Note-: check/set qualities before you start everytime"
            "</blockquote>"

        )
        
        # Send or edit settings summary
        try:
            prev_msg_id = user_active_messages.get(chat_id)
            if prev_msg_id:
                await bot.edit_message_media(
                    chat_id=chat_id,
                    message_id=prev_msg_id,
                    media=InputMediaPhoto(
                        media=SETTINGS_SUMMARY_PIC,
                        caption=caption,
                        parse_mode=ParseMode.HTML
                    ),
                    reply_markup=InlineKeyboardMarkup(buttons)
                )
            else:
                msg = await bot.send_photo(
                    chat_id=chat_id,
                    photo=SETTINGS_SUMMARY_PIC,
                    caption=caption,
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup(buttons)
                )
                user_active_messages[chat_id] = msg.id
        except Exception as e:
            logger.warning(f"Edit failed, sending new message: {e}")
            try:
                msg = await bot.send_photo(
                    chat_id=chat_id,
                    photo=SETTINGS_SUMMARY_PIC,
                    caption=caption,
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup(buttons)
                )
                user_active_messages[chat_id] = msg.id
            except Exception as ee:
                logger.error(f"Failed to send or edit settings message: {ee}")
                await bot.send_message(chat_id, "⚠️ Failed to refresh settings menu. Please try again.")
    except Exception as e:
        logger.error(f"Error in collect_settings for {chat_id}: {e}")
        await bot.send_message(chat_id, "❌ Failed to load settings. Please try again.")
@bot.on_callback_query(filters.regex(r"^set_quality_(\d+)$"))
@handle_floodwait
async def set_quality_handler(client: Client, query: CallbackQuery):
    """Handle quality selection menu - POLISHED VERSION"""
    try:
        chat_id = int(query.data.split("_")[2])
        await ask_for_quality(chat_id)
        await query.answer()
    except Exception as e:
        logger.error(f"Set quality handler error: {str(e)}")
        await query.answer("Failed to show quality options!", show_alert=True)
@bot.on_callback_query(filters.regex(r"^quality_(\w+)_(\d+)$"))
@handle_floodwait
async def quality_handler(client: Client, query: CallbackQuery):
    """Handle quality selection with enhanced feedback"""
    try:
        parts = query.data.split("_")
        if len(parts) != 3:
            return await query.answer("Invalid request", show_alert=True)
        quality = parts[1]
        chat_id = int(parts[2])
        session = await get_user_settings(chat_id)
        if not session:
            return await query.answer("Session expired! Start over with /magnet", show_alert=True)
        quality_map = {
            '480p': '480p',
            '720p': '720p', 
            '1080p': '1080p',
            'noencode': 'noencode'
        }
        if quality not in quality_map:
            return await query.answer("Invalid quality!", show_alert=True)
        normalized_quality = quality_map[quality]
        await update_user_settings(chat_id, {
            "quality": normalized_quality,
            "status": "quality_set",
            "last_update": time.time()
        })
        await query.answer(f"✅ Quality set to {normalized_quality}")
        await collect_settings(chat_id)
    except Exception as e:
        logger.error(f"Quality handler error: {str(e)}")
        await query.answer("Failed to set quality!", show_alert=True)
@bot.on_callback_query(filters.regex(r"^set_tune_(\d+)$"))
async def set_tune_handler(client: Client, query: CallbackQuery):
    """Handle video tune selection"""
    try:
        chat_id = int(query.data.split("_")[2])
        session = await get_user_settings(chat_id)
        if not session:
            return await query.answer("Session expired!", show_alert=True)
        tunes = [
            ["🎯 None (Balanced)", "tune_none"],
            ["🎨 Animation", "tune_animation"], 
            ["🎬 Film", "tune_film"],
            ["📺 Grain", "tune_grain"],
            ["⚡ Fast Decode", "tune_fastdecode"],
            ["💨 Zero Latency", "tune_zerolatency"],
            ["📊 PSNR", "tune_psnr"],
            ["🖼️ SSIM", "tune_ssim"],
            ["🔙 Back", f"settings_menu_{chat_id}"]
        ]
        buttons = []
        for i in range(0, len(tunes), 2):
            row = []
            for j in range(2):
                if i + j < len(tunes):
                    row.append(InlineKeyboardButton(
                        tunes[i+j][0], 
                        callback_data=f"{tunes[i+j][1]}_{chat_id}"
                    ))
            buttons.append(row)
        await query.message.edit_text(
            "🎛️ Select Video Tune\n\n"
            "Different tunes optimize for specific content types:\n\n"
            "• **None** - Balanced for general content\n"
            "• **Animation** - Anime/cartoons (default)\n"  
            "• **Film** - Live-action movies\n"
            "• **Grain** - Preserves film grain\n"
            "• **Fast Decode** - Faster playback\n"
            "• **Zero Latency** - Live streaming\n"
            "• **PSNR/SSIM** - Quality metrics\n\n"
            "Recommended: Animation for anime, Film for movies",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        await query.answer()
    except Exception as e:
        logger.error(f"Tune handler error: {str(e)}")
        await query.answer("Failed to set tune!", show_alert=True)
@bot.on_callback_query(filters.regex(r"^tune_(none|animation|film|grain|fastdecode|zerolatency|psnr|ssim)_(\d+)$"))
async def tune_selection_handler(client: Client, query: CallbackQuery):
    """Handle tune selection"""
    try:
        tune = query.data.split("_")[1]
        chat_id = int(query.data.split("_")[2])
        session = await get_user_settings(chat_id)
        if not session:
            return await query.answer("Session expired!", show_alert=True)
        await update_user_settings(chat_id, {"video_tune": tune})
        await query.answer(f"Tune set to {tune}")
        await collect_settings(chat_id)
    except Exception as e:
        logger.error(f"Tune selection error: {str(e)}")
        await query.answer("Failed to set tune!", show_alert=True)
@bot.on_callback_query(filters.regex(r"^all_qualities_(\d+)$"))
@handle_floodwait
async def all_qualities_handler(client: Client, query: CallbackQuery):
    """Handle all qualities selection - FIXED TO EDIT SAME MESSAGE"""
    try:
        chat_id = int(query.data.split("_")[2])
        session = await get_user_settings(chat_id)
        if not session:
            await query.answer("❌ Session expired! Start over with /magnet", show_alert=True)
            return
        
        await query.answer("🎯 Select Qualities")
        
        # 🎯 FIX: Edit the same message to show quality selector
        await show_all_qualities_selector(chat_id)
        
    except Exception as e:
        logger.error(f"All qualities selection error: {str(e)}")
        await query.answer("Failed to select all qualities!", show_alert=True)
@bot.on_callback_query(filters.regex(r"^set_crf_(\d+)$"))
async def set_crf_handler(client: Client, query: CallbackQuery):
    """Handle CRF setting with proper state management"""
    try:
        chat_id = int(query.data.split("_")[2])
        session = await get_user_settings(chat_id)
        if not session:
            return await query.answer("Session expired!", show_alert=True)
            
        # Clear any existing awaiting state first
        await update_user_settings(chat_id, {"awaiting": None})
        
        # Set new awaiting state
        await update_user_settings(chat_id, {"awaiting": "crf"})
        
        text = (
            "🎛️ **CRF Quality Setting**\n\n"
            "**CRF Range:** 0-51 (Lower = Better Quality, Larger Files)\n\n"
            "**Recommended Values:**\n"
            "• 18-22: High Quality (Large files)\n"
            "• 23-26: Good Balance ⭐ Recommended\n"
            "• 27-30: Smaller Files\n"
            "• 31-35: Low Quality\n"
            "• 36-51: Very Low Quality\n\n"
            "💡 **Auto CRF** uses optimized defaults per quality\n\n"
            "Please enter a number (0-51) or use the buttons below:"
        )
        
        buttons = [
            [
                InlineKeyboardButton("❌ Skip (Use Auto)", callback_data=f"skip_crf_{chat_id}"),
                InlineKeyboardButton("⭐ 23 (Recommended)", callback_data=f"set_crf_value_23_{chat_id}")
            ],
            [
                InlineKeyboardButton("🎯 26 (Balanced)", callback_data=f"set_crf_value_26_{chat_id}"),
                InlineKeyboardButton("📦 28 (Small Files)", callback_data=f"set_crf_value_28_{chat_id}")
            ],
            [
                InlineKeyboardButton("🔙 Back to Settings", callback_data=f"settings_menu_{chat_id}")
            ]
        ]
        
        await query.message.edit_text(
            text,
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        await query.answer()
    except Exception as e:
        logger.error(f"CRF handler error: {str(e)}")
        await query.answer("Failed to set CRF!", show_alert=True)
@bot.on_callback_query(filters.regex(r"^set_crf_value_(\d+)_(\d+)$"))
async def set_crf_value_handler(client: Client, query: CallbackQuery):
    """Handle preset CRF value selection - FIXED DATA TYPE"""
    try:
        crf_value = int(query.data.split("_")[3])  # This is already int
        chat_id = int(query.data.split("_")[4])
        
        # Clear awaiting state
        await update_user_settings(chat_id, {"awaiting": None})
        
        # Set CRF value as integer
        await update_user_settings(chat_id, {"custom_crf": crf_value})
        
        await query.answer(f"✅ CRF set to {crf_value}")
        await collect_settings(chat_id)
        
    except Exception as e:
        logger.error(f"CRF value handler error: {str(e)}")
        await query.answer("Failed to set CRF value!", show_alert=True)
@bot.on_callback_query(filters.regex(r"^skip_crf_(\d+)$"))
async def skip_crf_handler(client: Client, query: CallbackQuery):
    """Skip CRF setting and use auto"""
    try:
        chat_id = int(query.data.split("_")[2])
        
        # Clear awaiting state
        await update_user_settings(chat_id, {
            "custom_crf": None,
            "awaiting": None
        })
        
        await query.answer("✅ Using Auto CRF")
        await collect_settings(chat_id)
        
    except Exception as e:
        logger.error(f"Skip CRF error: {str(e)}")
        await query.answer("Failed to skip CRF!", show_alert=True)
async def show_all_qualities_selector(chat_id: int):
    """Show quality selection menu for All Qualities - USER CONTROLLED"""
    try:
        session = await get_user_settings(chat_id)
        if not session:
            return
            
        # 🎯 FIX: Start with empty selection - user chooses everything
        selected_qualities = session.get("selected_qualities", [])
        
        # Create buttons - user has full control
        buttons = [
            [
                InlineKeyboardButton(
                    f"{'✅' if '480p' in selected_qualities else '❌'} 480p", 
                    callback_data=f"toggle_quality_480p_{chat_id}"
                ),
                InlineKeyboardButton(
                    f"{'✅' if '720p' in selected_qualities else '❌'} 720p", 
                    callback_data=f"toggle_quality_720p_{chat_id}"
                )
            ],
            [
                InlineKeyboardButton(
                    f"{'✅' if '1080p' in selected_qualities else '❌'} 1080p", 
                    callback_data=f"toggle_quality_1080p_{chat_id}"
                ),
                InlineKeyboardButton(
                    f"{'✅' if 'noencode' in selected_qualities else '❌'} No Encode", 
                    callback_data=f"toggle_quality_noencode_{chat_id}"
                )
            ],
            [
                InlineKeyboardButton("✅ Select All", callback_data=f"select_all_qualities_{chat_id}"),
                InlineKeyboardButton("❌ Clear All", callback_data=f"clear_all_qualities_{chat_id}")
            ],
            [
                InlineKeyboardButton("🚀 Confirm Selection", callback_data=f"confirm_all_qualities_{chat_id}"),
                InlineKeyboardButton("🔙 Back", callback_data=f"set_quality_{chat_id}")
            ]
        ]
        
        selected_count = len(selected_qualities)
        selected_text = ", ".join(selected_qualities) if selected_qualities else "None selected"
        
        caption = (
            "🎯 <b>Select Qualities to Generate</b>\n\n"
            f"📊 <b>Selected:</b> {selected_text}\n"
            f"🔢 <b>Total Versions:</b> {selected_count}\n\n"
            "💡 <i>Tap on qualities to toggle ON/OFF</i>\n"
            "⚠️ <i>Select at least one quality to continue</i>"
        )
        
        QUALITY_SELECTOR_PIC = "https://files.catbox.moe/pnf3b4.jpg"
        
        # Try to edit existing message first
        try:
            await bot.edit_message_media(
                chat_id=chat_id,
                message_id=user_active_messages.get(chat_id),
                media=InputMediaPhoto(
                    media=QUALITY_SELECTOR_PIC,
                    caption=caption,
                    parse_mode=ParseMode.HTML
                ),
                reply_markup=InlineKeyboardMarkup(buttons)
            )
        except Exception as e:
            # If edit fails, send new message
            logger.warning(f"Edit failed, sending new message: {e}")
            msg = await bot.send_photo(
                chat_id=chat_id,
                photo=QUALITY_SELECTOR_PIC,
                caption=caption,
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(buttons)
            )
            user_active_messages[chat_id] = msg.id
            
    except Exception as e:
        logger.error(f"Error showing quality selector: {e}")
        await bot.send_message(chat_id, "❌ Failed to show quality selector. Please try again.")
@bot.on_callback_query(filters.regex(r"^toggle_quality_(480p|720p|1080p|noencode)_(\d+)$"))
async def toggle_quality_handler(client: Client, query: CallbackQuery):
    """Toggle individual quality selection"""
    try:
        quality = query.data.split("_")[2]
        chat_id = int(query.data.split("_")[3])
        session = await get_user_settings(chat_id)
        if not session:
            await query.answer("Session expired!", show_alert=True)
            return
        # 🎯 FIX: Handle None case
        selected_qualities = session.get("selected_qualities")
        if selected_qualities is None:
            selected_qualities = ["480p", "720p", "1080p", "noencode"]
        if quality in selected_qualities:
            selected_qualities.remove(quality)
        else:
            selected_qualities.append(quality)
        # Ensure at least one quality is selected
        if not selected_qualities:
            selected_qualities = ["720p"]  # Default to 720p if none selected
        await update_user_settings(chat_id, {"selected_qualities": selected_qualities})
        await query.answer(f"{'✅ Added' if quality in selected_qualities else '❌ Removed'} {quality}")
        await show_all_qualities_selector(chat_id)
    except Exception as e:
        logger.error(f"Toggle quality error: {str(e)}")
        await query.answer("Failed to toggle quality!", show_alert=True)
@bot.on_callback_query(filters.regex(r"^select_all_qualities_(\d+)$"))
async def select_all_qualities_handler(client: Client, query: CallbackQuery):
    """Select all qualities"""
    try:
        chat_id = int(query.data.split("_")[3])
        session = await get_user_settings(chat_id)
        if not session:
            await query.answer("Session expired!", show_alert=True)
            return
        all_qualities = ["480p", "720p", "1080p", "noencode"]
        await update_user_settings(chat_id, {"selected_qualities": all_qualities})
        await query.answer("✅ All qualities selected")
        await show_all_qualities_selector(chat_id)
    except Exception as e:
        logger.error(f"Select all qualities error: {str(e)}")
        await query.answer("Failed to select all qualities!", show_alert=True)
@bot.on_callback_query(filters.regex(r"^clear_all_qualities_(\d+)$"))
async def clear_all_qualities_handler(client: Client, query: CallbackQuery):
    """Clear all qualities selection - USER CAN HAVE EMPTY SELECTION"""
    try:
        chat_id = int(query.data.split("_")[3])
        session = await get_user_settings(chat_id)
        if not session:
            await query.answer("Session expired!", show_alert=True)
            return
        
        # 🎯 FIX: Allow user to clear everything - they'll get a warning when confirming
        await update_user_settings(chat_id, {"selected_qualities": []})
        await query.answer("❌ All qualities cleared - select at least one")
        await show_all_qualities_selector(chat_id)
        
    except Exception as e:
        logger.error(f"Clear all qualities error: {str(e)}")
        await query.answer("Failed to clear qualities!", show_alert=True)
@bot.on_callback_query(filters.regex(r"^confirm_all_qualities_(\d+)$"))
async def confirm_all_qualities_handler(client: Client, query: CallbackQuery):
    """Confirm All Qualities selection - USER CAN REMOVE 480p"""
    try:
        chat_id = int(query.data.split("_")[3])
        session = await get_user_settings(chat_id)
        if not session:
            await query.answer("Session expired!", show_alert=True)
            return
            
        selected_qualities = session.get("selected_qualities", [])
        
        # 🎯 FIX: Allow user to remove 480p completely - only validate that at least ONE quality is selected
        if not selected_qualities:
            # If user cleared everything, suggest 480p as default but don't force it
            selected_qualities = ["480p"]  # Just a suggestion
            await query.answer("⚠️ Selected 480p as default - you can change this", show_alert=False)
        # 🎯 REMOVED: No forced 480p addition
        
        await query.answer("✅ Qualities selected - Configure settings...")
        
        # Set all_qualities flag and store selected qualities
        await update_user_settings(chat_id, {
            "quality": "all_qualities",
            "all_qualities": True,
            "selected_qualities": selected_qualities,
            "status": "all_qualities_selected"
        })
        
        await collect_settings(chat_id)
        
    except Exception as e:
        logger.error(f"Confirm all qualities error: {str(e)}")
        await query.answer("Failed to confirm qualities!", show_alert=True)
@bot.on_callback_query(filters.regex(r"^toggle_upload_(\d+)$"))
@handle_floodwait
async def toggle_upload_handler(client: Client, query: CallbackQuery):
    """Toggle between video and document upload mode"""
    chat_id = int(query.data.split("_")[2])
    session = await get_user_settings(chat_id)
    if not session:
        await query.answer("Session expired!", show_alert=True)
        return
    current_mode = session.get("upload_mode", "video")
    new_mode = "document" if current_mode == "video" else "video"
    await update_user_settings(chat_id, {"upload_mode": new_mode})
    await query.answer(f"Upload mode set to {new_mode}")
    await collect_settings(chat_id)
@bot.on_callback_query(filters.regex(r"^cancel_download_(\d+)$"))
async def cancel_download_handler(client: Client, query: CallbackQuery):
    """Handle torrent download cancellation - FIXED"""
    try:
        chat_id = int(query.data.split("_")[2])
        # Verify the user is cancelling their own download
        if query.from_user.id != chat_id:
            await query.answer("❌ You can only cancel your own downloads!", show_alert=True)
            return
        # Set user cancellation event
        user_event = await get_user_cancellation_event(chat_id)
        user_event.set()
        await query.answer("🛑 Download cancellation requested...")
        # Update the message to show cancellation in progress
        await query.message.edit_text(
            "🛑 **Download Cancellation**\n\n"
            "Stopping download process...\n"
            "Please wait a moment..."
        )
        logger.info(f"🛑 Download cancellation initiated for user {chat_id}")
    except Exception as e:
        logger.error(f"Cancel download handler error: {str(e)}")
        await query.answer("Failed to cancel download!", show_alert=True)
@bot.on_callback_query(filters.regex(r"^cancel_(download|encode|batch|upload)_(\d+)$"))
async def cancel_task_handler(client: Client, query: CallbackQuery):
    """Handle task cancellation - FIXED VERSION with message validation"""
    try:
        cancel_type = query.data.split("_")[1]
        chat_id = int(query.data.split("_")[2])
        # Verify the user is cancelling their own task
        if query.from_user.id != chat_id:
            await query.answer("❌ You can only cancel your own tasks!", show_alert=True)
            return
        await query.answer("🛑 Cancellation requested...")
        # Cancel user tasks using the new function
        success = await cancel_user_tasks(chat_id)
        if success:
            try:
                # Try to edit the message, but handle if it's gone
                await query.message.edit_text(
                    f"❌ **{cancel_type.capitalize()} Cancelled**\n\n"
                    "Your task has been stopped successfully.\n"
                    "You can start a new task whenever you're ready."
                )
            except (MessageNotModified, BadRequest) as e:
                # If message editing fails, send a new one
                await query.message.reply(
                    f"❌ **{cancel_type.capitalize()} Cancelled**\n\n"
                    "Your task has been stopped successfully."
                )
            logger.info(f"✅ Task cancelled for user {chat_id} during {cancel_type}")
        else:
            await query.answer("❌ No active tasks to cancel!", show_alert=True)
    except Exception as e:
        logger.error(f"Cancel handler error: {str(e)}")
        try:
            await query.answer("Failed to cancel task!", show_alert=True)
        except:
            pass  # Ignore if we can't answer the query
@bot.on_message(filters.command("init") & filters.private)
async def init_bot_handler(client: Client, message: Message):
    """Manual initialization of bot settings"""
    try:
        user_id = message.from_user.id
        if user_id != BOT_OWNER_ID:
            await message.reply("❌ Only bot owner can initialize settings!")
            return
        await message.reply("🔄 Initializing bot settings...")
        # Reload settings
        await load_sudo_users()
        await load_bot_settings()
        await message.reply(
            f"✅ **Bot Settings Initialized**\n\n"
            f"👥 **Sudo Users:** {len(SUDO_USERS)}\n"
            f"🔐 **Authorization:** {'RESTRICTED' if BOT_AUTHORIZED else 'PUBLIC'}\n"
            f"💾 **Storage:** {'MongoDB' if db is not None else 'Memory'}"
        )
    except Exception as e:
        logger.error(f"Init command error: {str(e)}")
        await message.reply(f"❌ Error initializing: {str(e)}")
@bot.on_message(filters.command("status") & filters.private)
@sudo_only
async def enhanced_status_handler(client: Client, message: Message):
    """Enhanced admin status with storage analytics"""
    try:
        # Get system information
        cpu_usage = psutil.cpu_percent()
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        boot_time = datetime.fromtimestamp(psutil.boot_time())
        uptime = datetime.now() - boot_time
        # Get bot information
        queue_status = global_queue_manager.get_queue_status()
        bot_info = await bot.get_me()
        # Get user statistics
        total_users = await get_total_users_count()
        active_users = len(active_tasks)
        premium_users = len([uid for uid in PREMIUM_USERS if await is_premium_user(uid)])
        sudo_users = len(SUDO_USERS)
        # Get storage analytics - NOW FROM MONGODB
        analytics = await get_storage_analytics_summary()
        # Build comprehensive status message
        status_text = (
            "📊 <b>Enhanced Bot Status Overview</b>\n\n"
            "<blockquote>"
            "🤖 <b>Bot Information</b>\n"
            f"• Username: @{bot_info.username}\n"
            f"• ID: {bot_info.id}\n"
            f"• Uptime: {get_bot_uptime()}\n\n"
            "👥 <b>User Statistics</b>\n"
            f"• Total Users: {total_users}\n"
            f"• Active Now: {active_users}\n"
            f"• Premium Users: {premium_users}\n"
            f"• Sudo Users: {sudo_users}\n\n"
        )
        # Add Storage Analytics if available
        if analytics:
            status_text += (
                "💾 <b>Storage Analytics</b>\n"
                f"• Total Downloads: {analytics['total_downloads']} files\n"
                f"• Downloaded Data: {humanize.naturalsize(analytics['total_download_size'])}\n"
                f"• Total Processed: {analytics['total_processed']} files\n"
                f"• Processed Data: {humanize.naturalsize(analytics['total_processed_size'])}\n"
                f"• Storage Saved: {humanize.naturalsize(analytics['total_saved_size'])}\n"
                f"• Efficiency Rate: {analytics['efficiency']:.1f}%\n"
                f"• Avg Saved/File: {humanize.naturalsize(analytics['avg_saved_per_file'])}\n"
                f"• Active Users Tracked: {analytics['total_users']}\n\n"
            )
        status_text += (
            "⚡ <b>System Status</b>\n"
            f"• CPU Usage: {cpu_usage}%\n"
            f"• Memory: {memory.percent}% ({humanize.naturalsize(memory.used)}/{humanize.naturalsize(memory.total)})\n"
            f"• Disk: {disk.percent}% ({humanize.naturalsize(disk.used)}/{humanize.naturalsize(disk.total)})\n"
            f"• System Uptime: {str(uptime).split('.')[0]}\n\n"
            "🔄 <b>Task Status</b>\n"
            f"• Active Tasks: {len(active_tasks)}\n"
            f"• Queued Tasks: {queue_status['queue_size']}\n"
            f"• Current Task: {queue_status['current_task'] or 'None'}\n\n"
            "🔐 <b>Authorization Status</b>\n"
            f"• Mode: {'🔒 RESTRICTED' if BOT_AUTHORIZED else '🔓 PUBLIC'}\n"
            f"• Size Limits: {humanize.naturalsize(TORRENT_SIZE_LIMIT)} torrent, "
            f"{humanize.naturalsize(SINGLE_FILE_SIZE_LIMIT)} file\n"
            "</blockquote>"
        )
        buttons = [
            [InlineKeyboardButton("📈 Storage Analytics", callback_data="storage_analytics_detailed")],
            [InlineKeyboardButton("👥 User Statistics", callback_data="user_statistics_detailed")],
            [InlineKeyboardButton("🔄 Refresh Status", callback_data="bot_status")],
            [InlineKeyboardButton("👑 Admin Panel", callback_data="admin_panel")],
            [InlineKeyboardButton("❌ Close", callback_data="close_menu")]
        ]
        await message.reply(
            status_text,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(buttons)
        )
    except Exception as e:
        logger.error(f"Enhanced status error: {str(e)}")
        await message.reply(f"❌ Error getting status: {str(e)}")
@bot.on_callback_query(filters.regex("^storage_analytics_detailed$"))
async def storage_analytics_detailed_handler(client: Client, query: CallbackQuery):
    """Show detailed storage analytics from MongoDB"""
    try:
        analytics = await get_storage_analytics_summary()
        if not analytics:
            await query.answer("No analytics data available yet!", show_alert=True)
            return
        # Build detailed analytics message
        analytics_text = (
            "📈 <b>Detailed Storage Analytics</b>\n\n"
            "<blockquote>"
            "📥 <b>Download Statistics</b>\n"
            f"• Total Files Downloaded: {analytics['total_downloads']}\n"
            f"• Total Download Size: {humanize.naturalsize(analytics['total_download_size'])}\n"
            f"• Average File Size: {humanize.naturalsize(analytics['total_download_size'] / max(1, analytics['total_downloads']))}\n\n"
            "🎯 <b>Processing Statistics</b>\n"
            f"• Total Files Processed: {analytics['total_processed']}\n"
            f"• Total Processed Size: {humanize.naturalsize(analytics['total_processed_size'])}\n"
            f"• Storage Space Saved: {humanize.naturalsize(analytics['total_saved_size'])}\n"
            f"• Efficiency Rate: {analytics['efficiency']:.1f}%\n"
            f"• Average Saved per File: {humanize.naturalsize(analytics['avg_saved_per_file'])}\n\n"
            "👥 <b>User Statistics</b>\n"
            f"• Total Users Tracked: {analytics['total_users']}\n"
            f"• Files per User: {analytics['total_processed'] / max(1, analytics['total_users']):.1f}\n\n"
            "📊 <b>Recent Activity (Last 7 Days)</b>\n"
        )
        # Add daily trends
        for day, stats in analytics['daily_trends'].items():
            analytics_text += f"• {day}: {stats['processed']} processed, {stats.get('downloads', 0)} downloads\n"
        analytics_text += "</blockquote>"
        buttons = [
            [InlineKeyboardButton("📊 Top Users", callback_data="top_users_stats")],
            [InlineKeyboardButton("🔙 Back to Status", callback_data="bot_status")],
            [InlineKeyboardButton("🔄 Refresh", callback_data="storage_analytics_detailed")],
            [InlineKeyboardButton("❌ Close", callback_data="close_menu")]
        ]
        await query.message.edit_text(
            analytics_text,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        await query.answer()
    except Exception as e:
        logger.error(f"Storage analytics detailed error: {str(e)}")
        await query.answer("Error loading analytics!", show_alert=True)
@bot.on_callback_query(filters.regex("^top_users_stats$"))
async def top_users_stats_handler(client: Client, query: CallbackQuery):
    """Show top users statistics from MongoDB"""
    try:
        analytics = await get_storage_analytics_summary()
        if not analytics or not analytics['top_users']:
            await query.answer("No user statistics available yet!", show_alert=True)
            return
        top_users_text = "🏆 <b>Top Users by Processing Volume</b>\n\n<blockquote>"
        for i, (user_id, stats) in enumerate(analytics['top_users'], 1):
            try:
                # Try to get user info
                user = await bot.get_users(int(user_id))
                user_name = user.first_name or f"User {user_id}"
            except:
                user_name = f"User {user_id}"
            top_users_text += (
                f"{i}. {user_name}\n"
                f"   • Files: {stats['processed']} processed, {stats['downloads']} downloads\n"
                f"   • Data: {humanize.naturalsize(stats['processed_size'])} processed\n"
                f"   • Saved: {humanize.naturalsize(stats['saved_size'])}\n\n"
            )
        top_users_text += "</blockquote>"
        buttons = [
            [InlineKeyboardButton("📈 Back to Analytics", callback_data="storage_analytics_detailed")],
            [InlineKeyboardButton("🔙 Back to Status", callback_data="bot_status")],
            [InlineKeyboardButton("❌ Close", callback_data="close_menu")]
        ]
        await query.message.edit_text(
            top_users_text,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        await query.answer()
    except Exception as e:
        logger.error(f"Top users stats error: {str(e)}")
        await query.answer("Error loading user statistics!", show_alert=True)
@bot.on_message(filters.command("reset_analytics") & filters.private)
@sudo_only
async def reset_analytics_handler(client: Client, message: Message):
    """Reset storage analytics data"""
    try:
        default_analytics = {
            "total_downloads": 0,
            "total_download_size": 0,
            "total_processed": 0,
            "total_processed_size": 0,
            "total_saved_size": 0,
            "user_statistics": {},
            "daily_statistics": {},
            "type": "main_analytics"
        }
        await save_storage_analytics(default_analytics)
        await message.reply("✅ Storage analytics data has been reset to zero.")
    except Exception as e:
        logger.error(f"Reset analytics error: {str(e)}")
        await message.reply(f"❌ Error resetting analytics: {str(e)}")
@bot.on_callback_query(filters.regex("^top_users_stats$"))
async def top_users_stats_handler(client: Client, query: CallbackQuery):
    """Show top users statistics"""
    try:
        analytics = await get_storage_analytics_summary()
        if not analytics or not analytics['top_users']:
            await query.answer("No user statistics available yet!", show_alert=True)
            return
        top_users_text = "🏆 <b>Top Users by Processing Volume</b>\n\n<blockquote>"
        for i, (user_id, stats) in enumerate(analytics['top_users'], 1):
            try:
                # Try to get user info
                user = await bot.get_users(user_id)
                user_name = user.first_name or f"User {user_id}"
            except:
                user_name = f"User {user_id}"
            top_users_text += (
                f"{i}. {user_name}\n"
                f"   • Files: {stats['processed']} processed, {stats['downloads']} downloads\n"
                f"   • Data: {humanize.naturalsize(stats['processed_size'])} processed\n"
                f"   • Saved: {humanize.naturalsize(stats['saved_size'])}\n\n"
            )
        top_users_text += "</blockquote>"
        buttons = [
            [InlineKeyboardButton("📈 Back to Analytics", callback_data="storage_analytics_detailed")],
            [InlineKeyboardButton("🔙 Back to Status", callback_data="bot_status")],
            [InlineKeyboardButton("❌ Close", callback_data="close_menu")]
        ]
        await query.message.edit_text(
            top_users_text,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        await query.answer()
    except Exception as e:
        logger.error(f"Top users stats error: {str(e)}")
        await query.answer("Error loading user statistics!", show_alert=True)
@bot.on_message(filters.command("setshowcase") & filters.private)
@sudo_only
async def set_showcase_handler(client: Client, message: Message):
    """Set showcase channel"""
    try:
        if len(message.command) < 2:
            await message.reply(
                "❌ **Usage:** `/setshowcase <channel_id_or_username>`\n\n"
                "💡 **Examples:**\n"
                "• `/setshowcase -1001234567890` (Channel ID)\n"
                "• `/setshowcase @yourchannel` (Channel username)\n\n"
                "⚙️ **Additional Commands:**\n"
                "• `/showcase on/off` - Enable/disable showcase\n"
                "• `/showcase format forward/upload` - Set upload format\n"
                "• `/showcase status` - Check current settings"
            )
            return
        
        global SHOWCASE_CHANNEL
        channel_input = message.command[1].strip()
        
        # Validate channel
        try:
            if channel_input.startswith('@'):
                chat = await bot.get_chat(channel_input)
                SHOWCASE_CHANNEL = chat.id
            else:
                SHOWCASE_CHANNEL = int(channel_input)
            
            # Test access
            test_msg = await bot.send_message(
                SHOWCASE_CHANNEL,
                "✅ Showcase channel configured successfully!",
                disable_notification=True
            )
            await bot.delete_messages(SHOWCASE_CHANNEL, [test_msg.id])
            
            await save_showcase_settings()
            
            await message.reply(
                f"✅ **Showcase Channel Set!**\n\n"
                f"📢 **Channel:** `{SHOWCASE_CHANNEL}`\n"
                f"🌟 **All encoded files will be showcased here**\n\n"
                f"Use `/showcase on` to enable showcasing\n"
                f"Use `/showcase status` to check settings"
            )
            
        except Exception as e:
            await message.reply(
                f"❌ **Invalid channel!**\n\n"
                f"Error: {str(e)}\n\n"
                f"Please ensure:\n"
                "1. Bot is added to channel as admin\n"
                "2. Bot has send message permission\n"
                "3. Channel ID/username is correct"
            )
            
    except Exception as e:
        logger.error(f"Set showcase error: {str(e)}")
        await message.reply(f"❌ Error setting showcase: {str(e)}")
@bot.on_message(filters.command("showcase") & filters.private)
@sudo_only
async def showcase_control_handler(client: Client, message: Message):
    """Control showcase settings - FIXED VERSION"""
    global SHOWCASE_ENABLED, SHOWCASE_FORMAT  # Add this line
    
    try:
        if len(message.command) < 2:
            await showcase_status_handler(client, message)
            return
            
        action = message.command[1].lower()
        
        if action in ["on", "enable", "true", "yes"]:
            if not SHOWCASE_CHANNEL:
                await message.reply("❌ Please set showcase channel first with `/setshowcase`")
                return
            
            # Test channel access
            try:
                test_msg = await bot.send_message(
                    SHOWCASE_CHANNEL,
                    "🔧 Testing showcase channel access...",
                    disable_notification=True
                )
                await bot.delete_messages(SHOWCASE_CHANNEL, [test_msg.id])
            except Exception as e:
                await message.reply(
                    f"❌ Cannot access showcase channel!\n\n"
                    f"Error: {str(e)}\n\n"
                    f"Please ensure:\n"
                    f"1. Bot is admin in the channel\n"
                    f"2. Channel ID is correct: `{SHOWCASE_CHANNEL}`\n"
                    f"3. Bot has send message permission"
                )
                return
            
            SHOWCASE_ENABLED = True
            success = await save_showcase_settings()
            
            if success:
                await message.reply(
                    f"✅ **Showcase Enabled Successfully!**\n\n"
                    f"📢 **Channel:** `{SHOWCASE_CHANNEL}`\n"
                    f"🔄 **Format:** `{SHOWCASE_FORMAT}`\n\n"
                    f"🌟 All encoded files will now be showcased in the channel!\n"
                    f"👤 Users will be credited for their encodes.\n\n"
                    f"Use `/showcase off` to disable showcasing."
                )
                logger.info(f"✅ Showcase enabled by {message.from_user.id} for channel {SHOWCASE_CHANNEL}")
            else:
                await message.reply(
                    "⚠️ **Showcase enabled but failed to save settings!**\n\n"
                    "Changes may be lost on bot restart. Please check MongoDB connection."
                )
            
        elif action in ["off", "disable", "false", "no"]:
            SHOWCASE_ENABLED = False
            success = await save_showcase_settings()
            
            if success:
                await message.reply(
                    "❌ **Showcase Disabled!**\n\n"
                    "Files will no longer be showcased in the channel.\n\n"
                    "Use `/showcase on` to re-enable showcasing."
                )
                logger.info(f"❌ Showcase disabled by {message.from_user.id}")
            else:
                await message.reply(
                    "⚠️ **Showcase disabled but failed to save settings!**\n\n"
                    "Changes may be lost on bot restart."
                )
            
        elif action == "format":
            if len(message.command) < 3:
                await message.reply(
                    "❌ **Usage:** `/showcase format <forward/upload>`\n\n"
                    "**Formats:**\n"
                    "• `forward` - Forward from user's upload (recommended, saves bandwidth)\n"
                    "• `upload` - Direct upload to showcase channel (shows bot as uploader)\n\n"
                    f"Current format: `{SHOWCASE_FORMAT}`"
                )
                return
                
            format_type = message.command[2].lower()
            if format_type in ["forward", "upload"]:
                old_format = SHOWCASE_FORMAT
                SHOWCASE_FORMAT = format_type
                success = await save_showcase_settings()
                
                if success:
                    await message.reply(
                        f"✅ **Showcase format changed!**\n\n"
                        f"**From:** `{old_format}`\n"
                        f"**To:** `{format_type}`\n\n"
                        f"💡 **What this means:**\n"
                        f"{'• Files will be forwarded from user chats' if format_type == 'forward' else '• Files will be uploaded directly to showcase'}"
                    )
                else:
                    await message.reply("❌ Failed to save format change!")
            else:
                await message.reply("❌ Invalid format. Use `forward` or `upload`")
                
        elif action == "status":
            await showcase_status_handler(client, message)
            
        elif action == "test":
            await test_showcase_handler(client, message)
            
        else:
            await message.reply(
                "❌ **Invalid command!**\n\n"
                "**Available commands:**\n"
                "• `/showcase on` - Enable showcasing\n"
                "• `/showcase off` - Disable showcasing\n"
                "• `/showcase format forward/upload` - Set format\n"
                "• `/showcase status` - Check status\n"
                "• `/showcase test` - Test showcase channel\n"
                "• `/setshowcase <channel>` - Set channel"
            )
            
    except Exception as e:
        logger.error(f"Showcase control error: {str(e)}")
        await message.reply(f"❌ Error: {str(e)}")
async def showcase_status_handler(client: Client, message: Message):
    """Show detailed showcase status - FIXED VERSION"""
    try:
        # Try to get channel info
        channel_info = None
        if SHOWCASE_CHANNEL:
            try:
                chat = await bot.get_chat(SHOWCASE_CHANNEL)
                channel_info = {
                    'title': chat.title,
                    'type': str(chat.type),
                    'username': f"@{chat.username}" if chat.username else "Private",
                    'members_count': chat.members_count if hasattr(chat, 'members_count') else 'Unknown'
                }
            except Exception as e:
                channel_info = {'error': str(e)}
        
        status_text = (
            "🌟 **Showcase Channel Status**\n\n"
            f"**Enabled:** {'🟢 YES' if SHOWCASE_ENABLED else '🔴 NO'}\n"
            f"**Channel ID:** `{SHOWCASE_CHANNEL or 'Not set'}`\n"
            f"**Format:** `{SHOWCASE_FORMAT}`\n\n"
        )
        
        if channel_info:
            if 'error' in channel_info:
                status_text += f"**Channel Access:** ❌ {channel_info['error']}\n\n"
            else:
                status_text += (
                    f"**Channel Name:** {channel_info['title']}\n"
                    f"**Type:** {channel_info['type']}\n"
                    f"**Username:** {channel_info['username']}\n"
                    f"**Members:** {channel_info['members_count']}\n\n"
                )
        
        if SHOWCASE_ENABLED:
            status_text += (
                "🟢 **SHOWCASE IS ACTIVE**\n\n"
                "All encoded files are being showcased with:\n"
                "• File information\n"
                "• User credits\n"
                "• Encoding details\n\n"
                "Use `/showcase off` to disable"
            )
        else:
            status_text += (
                "🔴 **SHOWCASE IS INACTIVE**\n\n"
                "Encoded files are NOT being showcased.\n\n"
                "To enable:\n"
                "1. Use `/showcase on` to enable\n"
                "2. Or `/setshowcase` if channel not set"
            )
        
        # Add buttons for quick actions
        buttons = []
        if SHOWCASE_ENABLED:
            buttons.append([InlineKeyboardButton("🔴 Disable Showcase", callback_data="showcase_disable")])
        else:
            buttons.append([InlineKeyboardButton("🟢 Enable Showcase", callback_data="showcase_enable")])
        
        buttons.append([
            InlineKeyboardButton("🔄 Test Channel", callback_data="showcase_test"),
            InlineKeyboardButton("⚙️ Change Format", callback_data="showcase_format")
        ])
        
        await message.reply(
            status_text,
            reply_markup=InlineKeyboardMarkup(buttons) if buttons else None
        )
        
    except Exception as e:
        logger.error(f"Showcase status error: {str(e)}")
        await message.reply(f"❌ Error getting showcase status: {str(e)}")
@bot.on_callback_query(filters.regex("^showcase_enable$"))
async def showcase_enable_callback(client: Client, query: CallbackQuery):
    """Enable showcase from callback"""
    global SHOWCASE_ENABLED
    try:
        if not SHOWCASE_CHANNEL:
            await query.answer("❌ Set showcase channel first!", show_alert=True)
            return
            
        SHOWCASE_ENABLED = True
        await save_showcase_settings()
        await query.answer("✅ Showcase enabled!")
        await showcase_status_handler(client, query.message)
        
    except Exception as e:
        await query.answer("❌ Failed to enable showcase!")

@bot.on_callback_query(filters.regex("^showcase_disable$"))
async def showcase_disable_callback(client: Client, query: CallbackQuery):
    """Disable showcase from callback"""
    global SHOWCASE_ENABLED
    try:
        SHOWCASE_ENABLED = False
        await save_showcase_settings()
        await query.answer("❌ Showcase disabled!")
        await showcase_status_handler(client, query.message)
        
    except Exception as e:
        await query.answer("❌ Failed to disable showcase!")

@bot.on_callback_query(filters.regex("^showcase_test$"))
async def showcase_test_callback(client: Client, query: CallbackQuery):
    """Test showcase from callback"""
    await query.answer("Testing showcase channel...")
    await test_showcase_handler(client, query.message)
async def test_showcase_handler(client: Client, message: Message):
    """Test showcase channel access"""
    try:
        if not SHOWCASE_CHANNEL:
            await message.reply("❌ Showcase channel not set! Use `/setshowcase` first.")
            return
            
        await message.reply("🔧 Testing showcase channel access...")
        
        # Test 1: Send message
        test_msg = await bot.send_message(
            SHOWCASE_CHANNEL,
            "✅ **Showcase Channel Test**\n\n"
            "This is a test message to verify bot access.\n"
            "If you can see this, the showcase channel is working properly!\n\n"
            f"**Channel ID:** `{SHOWCASE_CHANNEL}`\n"
            f"**Status:** {'🟢 ENABLED' if SHOWCASE_ENABLED else '🔴 DISABLED'}\n"
            f"**Format:** `{SHOWCASE_FORMAT}`",
            disable_notification=True
        )
        
        # Test 2: Send a small test file if possible
        try:
            # Create a simple test file
            test_file_path = "test_showcase.txt"
            with open(test_file_path, "w") as f:
                f.write("This is a test file for showcase channel verification.\n")
                f.write(f"Channel: {SHOWCASE_CHANNEL}\n")
                f.write(f"Time: {datetime.now().isoformat()}\n")
                f.write("✅ Showcase channel is working correctly!")
            
            await bot.send_document(
                SHOWCASE_CHANNEL,
                test_file_path,
                caption="📄 Test file for showcase channel",
                disable_notification=True
            )
            
            # Cleanup
            if ospath.exists(test_file_path):
                osremove(test_file_path)
                
        except Exception as file_error:
            logger.warning(f"Could not send test file: {str(file_error)}")
        
        await message.reply(
            f"✅ **Showcase Channel Test Successful!**\n\n"
            f"📢 **Channel:** `{SHOWCASE_CHANNEL}`\n"
            f"🟢 **Status:** Access verified\n"
            f"🔄 **Format:** `{SHOWCASE_FORMAT}`\n"
            f"🔧 **Enabled:** {'YES' if SHOWCASE_ENABLED else 'NO'}\n\n"
            f"🌟 The showcase channel is ready to receive encoded files!"
        )
        
        # Cleanup test message after delay
        await asyncio.sleep(10)
        await bot.delete_messages(SHOWCASE_CHANNEL, [test_msg.id])
        
    except Exception as e:
        logger.error(f"Showcase test error: {str(e)}")
        await message.reply(
            f"❌ **Showcase Channel Test Failed!**\n\n"
            f"**Error:** {str(e)}\n\n"
            f"**Troubleshooting:**\n"
            f"1. Check if bot is admin in channel `{SHOWCASE_CHANNEL}`\n"
            f"2. Verify channel ID is correct\n"
            f"3. Ensure bot has send message permission\n"
            f"4. For private channels, add bot as admin first"
        ) 
@bot.on_message(filters.command("ban") & filters.private)
@sudo_only
async def ban_user_handler(client: Client, message: Message):
    """Ban a user from using the bot"""
    try:
        if len(message.command) < 2:
            await message.reply(
                "❌ **Usage:** `/ban user_id [duration] [reason]`\n\n"
                "💡 **Examples:**\n"
                "• `/ban 123456789` - Permanent ban\n"
                "• `/ban 123456789 7d Spamming` - 7 day ban\n"
                "• `/ban 123456789 24h Violating rules` - 24 hour ban\n\n"
                "⏰ **Duration formats:**\n"
                "• 24h - 24 hours\n• 7d - 7 days\n• 30d - 30 days\n• 1y - 1 year\n\n"
                "👥 **Note:** Banned users cannot use any bot commands.",
                parse_mode=ParseMode.HTML
            )
            return
            
        user_id = int(message.command[1])
        
        # Check if trying to ban self or owner
        if user_id == message.from_user.id:
            await message.reply("❌ You cannot ban yourself!")
            return
            
        if user_id == BOT_OWNER_ID:
            await message.reply("❌ You cannot ban the bot owner!")
            return
            
        # Check if user is sudo
        if user_id in SUDO_USERS:
            await message.reply("❌ You cannot ban sudo users!")
            return
        
        # Parse duration and reason
        duration = None
        reason = "No reason provided"
        
        if len(message.command) >= 3:
            # Check if third parameter is a duration
            duration_str = message.command[2].lower()
            if any(x in duration_str for x in ['h', 'd', 'w', 'm', 'y']):
                duration = duration_str
                reason = ' '.join(message.command[3:]) if len(message.command) > 3 else "No reason provided"
            else:
                reason = ' '.join(message.command[2:])
        
        # Ban the user
        success, ban_data = await ban_user(user_id, message.from_user.id, reason, duration)
        
        if success:
            # Get user info for better display
            try:
                user = await bot.get_users(user_id)
                user_name = user.first_name or f"User {user_id}"
                user_mention = user.mention
            except:
                user_name = f"User {user_id}"
                user_mention = f"`{user_id}`"
            
            # Build ban message
            expires_at = ban_data.get('expires_at')
            if expires_at:
                expiry_time = datetime.fromisoformat(expires_at)
                expiry_str = expiry_time.strftime('%Y-%m-%d %H:%M:%S')
                ban_message = (
                    f"🚫 **User Banned Successfully**\n\n"
                    f"👤 **User:** {user_mention}\n"
                    f"🆔 **ID:** `{user_id}`\n"
                    f"📝 **Reason:** {reason}\n"
                    f"⏰ **Duration:** {duration} (until {expiry_str})\n"
                    f"👑 **Banned by:** {message.from_user.mention}\n"
                    f"🕒 **Time:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                )
            else:
                ban_message = (
                    f"🚫 **User Permanently Banned**\n\n"
                    f"👤 **User:** {user_mention}\n"
                    f"🆔 **ID:** `{user_id}`\n"
                    f"📝 **Reason:** {reason}\n"
                    f"⏰ **Duration:** Permanent\n"
                    f"👑 **Banned by:** {message.from_user.mention}\n"
                    f"🕒 **Time:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                )
            
            await message.reply(ban_message, parse_mode=ParseMode.HTML)
            
            # Notify the user if possible
            try:
                if expires_at:
                    user_notification = (
                        f"🚫 <b>You have been banned from using this bot</b>\n\n"
                        f"📝 <b>Reason:</b> {reason}\n"
                        f"⏰ <b>Duration:</b> {duration}\n"
                        f"📅 <b>Ban expires:</b> {expiry_str}\n\n"
                        f"<i>You will not be able to use any bot commands until the ban expires.</i>"
                    )
                else:
                    user_notification = (
                        f"🚫 <b>You have been permanently banned from using this bot</b>\n\n"
                        f"📝 <b>Reason:</b> {reason}\n\n"
                        f"<i>You will not be able to use any bot commands.</i>"
                    )
                
                await bot.send_message(user_id, user_notification, parse_mode=ParseMode.HTML)
            except Exception as e:
                logger.warning(f"Could not notify banned user {user_id}: {str(e)}")
                
        else:
            await message.reply("❌ Failed to ban user. Please try again.")
            
    except ValueError:
        await message.reply("❌ Invalid user ID format.")
    except Exception as e:
        logger.error(f"Ban user error: {str(e)}")
        await message.reply(f"❌ Error banning user: {str(e)}")

@bot.on_message(filters.command("unban") & filters.private)
@sudo_only
async def unban_user_handler(client: Client, message: Message):
    """Unban a user"""
    try:
        if len(message.command) < 2:
            await message.reply(
                "❌ **Usage:** `/unban user_id`\n\n"
                "💡 **Example:**\n"
                "• `/unban 123456789`\n\n"
                "👥 **Note:** This will allow the user to use the bot again.",
                parse_mode=ParseMode.HTML
            )
            return
            
        user_id = int(message.command[1])
        
        # Check if user is actually banned
        if not await is_user_banned(user_id):
            await message.reply(f"❌ User `{user_id}` is not banned.")
            return
        
        # Unban the user
        success = await unban_user(user_id)
        
        if success:
            # Get user info for better display
            try:
                user = await bot.get_users(user_id)
                user_name = user.first_name or f"User {user_id}"
                user_mention = user.mention
            except:
                user_name = f"User {user_id}"
                user_mention = f"`{user_id}`"
            
            await message.reply(
                f"✅ **User Unbanned Successfully**\n\n"
                f"👤 **User:** {user_mention}\n"
                f"🆔 **ID:** `{user_id}`\n"
                f"👑 **Unbanned by:** {message.from_user.mention}\n"
                f"🕒 **Time:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )
            
            # Notify the user if possible
            try:
                user_notification = (
                    f"✅ <b>Your ban has been lifted</b>\n\n"
                    f"You can now use the bot again.\n\n"
                    f"Please follow the bot rules to avoid future bans."
                )
                await bot.send_message(user_id, user_notification, parse_mode=ParseMode.HTML)
            except Exception as e:
                logger.warning(f"Could not notify unbanned user {user_id}: {str(e)}")
                
        else:
            await message.reply("❌ Failed to unban user. Please try again.")
            
    except ValueError:
        await message.reply("❌ Invalid user ID format.")
    except Exception as e:
        logger.error(f"Unban user error: {str(e)}")
        await message.reply(f"❌ Error unbanning user: {str(e)}")

@bot.on_message(filters.command("banlist") & filters.private)
@sudo_only
async def ban_list_handler(client: Client, message: Message):
    """List all banned users"""
    try:
        if not BANNED_USERS:
            await message.reply("ℹ️ No users are currently banned.")
            return
            
        ban_list = []
        current_time = time.time()
        
        for user_id, ban_data in BANNED_USERS.items():
            reason = ban_data.get('reason', 'No reason provided')
            banned_by = ban_data.get('banned_by', 'Unknown')
            expires_at = ban_data.get('expires_at')
            
            # Check if ban is expired
            if expires_at:
                expiry_time = datetime.fromisoformat(expires_at).timestamp()
                if current_time > expiry_time:
                    status = "❌ EXPIRED"
                else:
                    expiry_str = datetime.fromisoformat(expires_at).strftime('%Y-%m-%d %H:%M')
                    status = f"⏰ {expiry_str}"
            else:
                status = "🚫 PERMANENT"
                
            ban_list.append(f"• `{user_id}` - {reason} - {status} - by {banned_by}")
        
        list_text = (
            f"🚫 <b>Banned Users List</b>\n\n"
            f"Total: {len(BANNED_USERS)} users\n\n"
            f"{chr(10).join(ban_list)}"
        )
        
        # Split if too long
        if len(list_text) > 4000:
            parts = [list_text[i:i+4000] for i in range(0, len(list_text), 4000)]
            for part in parts:
                await message.reply(part, parse_mode=ParseMode.HTML)
        else:
            await message.reply(list_text, parse_mode=ParseMode.HTML)
            
    except Exception as e:
        logger.error(f"Ban list error: {str(e)}")
        await message.reply(f"❌ Error getting ban list: {str(e)}")
@bot.on_message(filters.command("reset") & (filters.private | filters.group))
@authorized_only
async def reset_handler(client: Client, message: Message):
    """Reset all settings to defaults with PROPER MongoDB handling"""
    try:
        chat_id = message.chat.id
        # 🎯 FIX: Complete default settings with PROPER None values
        default_settings = {
            "quality": "720p",
            "codec": "h264", 
            "preset": "veryfast",
            "upload_mode": "video",
            "metadata": {"title": ""},
            "pixel_format": "yuv420p",  # 🆕 Add pixel format default
            "watermark_enabled": False,
            "watermark": None,  # 🎯 EXPLICITLY SET TO None
            "thumbnail": None,  # 🎯 EXPLICITLY SET TO None
            "custom_crf": None,  # 🎯 EXPLICITLY SET TO None
            "subtitle_mode": "keep",
            "subtitle_file": None,  # 🎯 EXPLICITLY SET TO None
            "audio_mode": "keep", 
            "audio_file": None,  # 🎯 EXPLICITLY SET TO None
            "samples_enabled": False,
            "screenshots_enabled": False,
            "remname": None,  # 🎯 EXPLICITLY SET TO None
            "awaiting": None,
            "cancelled": False,
            "video_tune": "none", 
            "status": "idle",
            "all_qualities": False,  # 🎯 ADD THIS
            "selected_qualities": [],  # 🎯 ADD THIS
            "original_settings": {}  # 🎯 ADD THIS
        }
        # Update both MongoDB and session with PROPER None values
        await update_user_settings(chat_id, default_settings)
        # 🎯 ALSO DELETE ACTUAL FILES FROM DISK
        try:
            # Delete thumbnail file if exists
            thumb_path = f"thumbnails/thumb_{chat_id}.jpg"
            if ospath.exists(thumb_path):
                osremove(thumb_path)
            # Delete watermark file if exists  
            wm_path = f"watermarks/wm_{chat_id}.png"
            if ospath.exists(wm_path):
                osremove(wm_path)
            # Delete preview file if exists
            preview_path = f"previews/preview_{chat_id}.jpg"
            if ospath.exists(preview_path):
                osremove(preview_path)
        except Exception as file_error:
            logger.warning(f"File cleanup during reset failed: {str(file_error)}")
        await message.reply(
            "✅ **All Settings Reset to Defaults!**\n\n"
            "🔄 **Reset Values:**\n"
            "• Quality: 720p\n"
            "• Codec: H264\n" 
            "• Preset: veryfast\n"
            "• Upload Mode: Video\n"
            "• Watermark: Disabled & File Deleted\n"
            "• Thumbnail: File Deleted\n"
            "• CRF: Auto\n"
            "• REMNAME: Cleared\n"
            "• Samples: Disabled\n"
            "• Screenshots: Disabled\n"
            "• All custom files cleared\n\n"
          #  "💾 **Saved to:** MongoDB & Session\n"
            "🗑️ **Files deleted from disk**"
        )
        # Show updated settings
        await collect_settings(chat_id)
    except Exception as e:
        logger.error(f"Reset command error: {str(e)}")
        await message.reply(f"❌ Error resetting settings: {str(e)}")
async def set_dump_channel(chat_id: int, channel_id: str) -> bool:
    """Set dump channel for user - COMPATIBILITY FIX"""
    try:
        # Validate channel format
        if not channel_id.startswith('-100'):
            if channel_id.startswith('@'):
                # Handle username format
                try:
                    chat = await bot.get_chat(channel_id)
                    channel_id = str(chat.id)
                except Exception as e:
                    logger.error(f"Cannot get chat from username {channel_id}: {str(e)}")
                    return False
            else:
                # Assume it's a channel ID without -100 prefix
                channel_id = f"-100{channel_id}" if not channel_id.startswith('-') else channel_id
        
        # Convert to integer for validation
        try:
            channel_id_int = int(channel_id)
        except ValueError:
            logger.error(f"Invalid channel ID format: {channel_id}")
            return False
        
        # Test if bot has access to the channel
        try:
            chat = await bot.get_chat(channel_id_int)
            
            # Check if bot is admin in the channel - COMPATIBILITY FIX
            bot_member = await chat.get_member((await bot.get_me()).id)
            
            # Check permissions - compatible with both old and new Pyrogram versions
            can_send = False
            if hasattr(bot_member, 'privileges'):
                privileges = bot_member.privileges
                # New Pyrogram version
                if hasattr(privileges, 'can_send_messages'):
                    can_send = privileges.can_send_messages
                # Old Pyrogram version or alternative attribute names
                elif hasattr(privileges, 'can_send_messages'):
                    can_send = getattr(privileges, 'can_send_messages', False)
            elif hasattr(bot_member, 'can_send_messages'):
                # Direct attribute check
                can_send = bot_member.can_send_messages
            
            # If we can't determine permissions, try to send a test message
            if not can_send:
                try:
                    # Try to send a test message to check permissions
                    test_msg = await bot.send_message(
                        channel_id_int,
                        "✅ Bot has been granted access to this channel for file uploads.",
                        disable_notification=True
                    )
                    await bot.delete_messages(channel_id_int, [test_msg.id])
                    can_send = True
                    logger.info(f"✅ Bot can send messages in channel {channel_id}")
                except Exception as test_error:
                    logger.error(f"Bot cannot send messages in channel {channel_id}: {str(test_error)}")
                    return False
                
            logger.info(f"✅ Bot can access channel: {chat.title} ({chat.id})")
            
        except Exception as e:
            logger.error(f"Bot cannot access channel {channel_id}: {str(e)}")
            return False
        
        await update_user_settings(chat_id, {"dump_channel": channel_id})
        logger.info(f"✅ Dump channel set for {chat_id}: {channel_id}")
        return True
    except Exception as e:
        logger.error(f"Error setting dump channel: {str(e)}")
        return False

@bot.on_message(filters.command("setdump") & (filters.private | filters.group))
@authorized_only
@premium_required  # Add this line
async def set_dump_handler(client: Client, message: Message):
    """Set dump channel for uploaded files - SIMPLIFIED VERSION"""
    try:
        if len(message.command) < 2:
            await message.reply(
                "❌ **Usage:** `/setdump <channel_id_or_username>`\n\n"
                "💡 **How to set up:**\n"
                "1. Add @{} to your channel as **admin**\n"
                "2. Give permission to **send messages**\n"
                "3. Use any of these formats:\n\n"
                "**Examples:**\n"
                "• `/setdump -1001234567890` (Channel ID)\n"
                "• `/setdump @yourchannel` (Channel username)\n"
                "• `/setdump 1234567890` (Auto-add -100 prefix)\n\n"
                "⚠️ **Important:** Bot must be admin in the channel!".format((await bot.get_me()).username)
            )
            return
        
        chat_id = message.chat.id
        channel_input = message.command[1].strip()
        
        # Show processing message
        processing_msg = await message.reply("🔄 Checking channel access...")
        
        success = await set_dump_channel(chat_id, channel_input)
        
        if success:
            await processing_msg.edit_text(
                f"✅ **Dump Channel Set Successfully!**\n\n"
                f"📢 **Channel:** `{channel_input}`\n"
                f"📁 **All future uploads will be sent to this channel**\n\n"
                f"Use `/viewdump` to check your current dump channel\n"
                f"Use `/removedump` to remove the dump channel"
            )
        else:
            await processing_msg.edit_text(
                "❌ **Failed to set dump channel!**\n\n"
                "Please ensure:\n"
                "1. Bot is added to channel as **admin**\n"
                "2. Bot has **send message** permission\n"
                "3. Channel ID/username is correct\n\n"
                "**Troubleshooting:**\n"
                "• For private channels: Add bot as admin first\n"
                "• For usernames: Make sure channel is public\n"
                "• Try using channel ID instead of username\n\n"
                "**Get Channel ID:**\n"
                "1. Forward a message from channel to @userinfobot\n"
                "2. Use the numerical ID (add -100 prefix)"
            )
            
    except Exception as e:
        logger.error(f"Set dump handler error: {str(e)}")
        try:
            await processing_msg.edit_text(f"❌ Error setting dump channel: {str(e)}")
        except:
            await message.reply(f"❌ Error setting dump channel: {str(e)}")

@bot.on_message(filters.command("viewdump") & (filters.private | filters.group))
@authorized_only
@premium_required  # Add this line
async def view_dump_handler(client: Client, message: Message):
    """View current dump channel setting - SIMPLIFIED"""
    try:
        chat_id = message.chat.id
        dump_channel = await get_dump_channel(chat_id)
        
        if dump_channel:
            try:
                # Convert to int for API call
                channel_id_int = int(dump_channel)
                channel_info = await bot.get_chat(channel_id_int)
                channel_name = channel_info.title
                
                # Simple test to check if bot can send messages
                try:
                    test_msg = await bot.send_message(
                        channel_id_int,
                        "🔍 Testing bot access...",
                        disable_notification=True
                    )
                    await bot.delete_messages(channel_id_int, [test_msg.id])
                    status = "✅ Connected"
                except Exception as access_error:
                    status = f"❌ No access: {str(access_error)}"
                
                await message.reply(
                    f"📢 **Current Dump Channel**\n\n"
                    f"🏷️ **Name:** {channel_name}\n"
                    f"🆔 **ID:** `{dump_channel}`\n"
                    f"🔧 **Status:** {status}\n\n"
                    f"📁 All encoded files will be uploaded to this channel.\n\n"
                    f"Use `/removedump` to remove dump channel\n"
                    f"Use `/setdump <new_id>` to change dump channel"
                )
            except Exception as e:
                await message.reply(
                    f"📢 **Current Dump Channel**\n\n"
                    f"🆔 **Channel ID:** `{dump_channel}`\n\n"
                    f"⚠️ *Could not verify channel access*\n"
                    f"**Error:** {str(e)[:100]}\n\n"
                    f"Use `/removedump` to remove this dump channel"
                )
        else:
            await message.reply(
                "ℹ️ **No Dump Channel Configured**\n\n"
                "You haven't set up a dump channel yet.\n"
                "Encoded files will be uploaded to this chat.\n\n"
                "💡 **To set a dump channel:**\n"
                "Use `/setdump <channel_id_or_username>`\n\n"
                "**Benefits:**\n"
                "• Keep your chat clean\n"
                "• Organize all encoded files in one place\n"
                "• Easy file management"
            )
            
    except Exception as e:
        logger.error(f"View dump handler error: {str(e)}")
        await message.reply(f"❌ Error viewing dump channel: {str(e)}")

@bot.on_message(filters.command("testdump") & (filters.private | filters.group))
@authorized_only
async def test_dump_handler(client: Client, message: Message):
    """Test dump channel access"""
    try:
        chat_id = message.chat.id
        dump_channel = await get_dump_channel(chat_id)
        
        if not dump_channel:
            await message.reply("❌ No dump channel set. Use `/setdump` first.")
            return
            
        try:
            channel_id_int = int(dump_channel)
            
            # Test message
            test_msg = await bot.send_message(
                channel_id_int,
                "✅ **Dump Channel Test**\n\n"
                "This is a test message to verify bot access.\n"
                "If you can see this, the dump channel is working properly!",
                disable_notification=True
            )
            
            # Send success message
            await message.reply(
                f"✅ **Dump Channel Test Successful!**\n\n"
                f"📢 Message sent successfully to dump channel.\n"
                f"🆔 Channel ID: `{dump_channel}`\n\n"
                f"All future uploads will be sent to this channel."
            )
            
            # Delete test message after a delay
            await asyncio.sleep(5)
            await bot.delete_messages(channel_id_int, [test_msg.id])
            
        except Exception as e:
            await message.reply(
                f"❌ **Dump Channel Test Failed**\n\n"
                f"**Error:** {str(e)}\n\n"
                f"Please check:\n"
                f"1. Bot is admin in the channel\n"
                f"2. Bot has send message permission\n"
                f"3. Channel ID is correct\n\n"
                f"Use `/setdump` to update the channel settings."
            )
            
    except Exception as e:
        logger.error(f"Test dump handler error: {str(e)}")
        await message.reply(f"❌ Error testing dump channel: {str(e)}")
@bot.on_message(filters.command("removedump") & (filters.private | filters.group))
@authorized_only
@premium_required  # Add this line
async def remove_dump_handler(client: Client, message: Message):
    """Remove dump channel setting"""
    try:
        chat_id = message.chat.id
        current_dump = await get_dump_channel(chat_id)
        
        if not current_dump:
            await message.reply(
                "ℹ️ **No Dump Channel Set**\n\n"
                "You don't have a dump channel configured.\n"
                "Use `/setdump <channel_id>` to set one."
            )
            return
        
        success = await remove_dump_channel(chat_id)
        
        if success:
            await message.reply(
                f"✅ **Dump Channel Removed!**\n\n"
                f"📢 **Previous Channel:** `{current_dump}`\n"
                f"📁 **Files will now be uploaded to this chat**\n\n"
                "Use `/setdump <channel_id>` to set a new dump channel."
            )
        else:
            await message.reply("❌ Failed to remove dump channel. Please try again.")
            
    except Exception as e:
        logger.error(f"Remove dump handler error: {str(e)}")
        await message.reply(f"❌ Error removing dump channel: {str(e)}")
@bot.on_message(filters.command("settings") & (filters.private | filters.group))
@premium_required  # Add this line
@authorized_only
async def settings_command(client: Client, message: Message):
    """Show current settings"""
    await collect_settings(message.chat.id)
@bot.on_message(filters.command("queue") & (filters.private | filters.group))
@premium_required  # Add this line
@authorized_only
async def enhanced_queue_handler(client: Client, message: Message):
    """Show enhanced queue status with accurate time estimates"""
    try:
        user_id = message.from_user.id
        is_owner = user_id in SUDO_USERS
        queue_status = await global_queue_manager.get_queue_status_with_estimates()
        if is_owner:
            queue_info = await get_owner_queue_view_enhanced(queue_status)
        else:
            queue_info = await get_user_queue_view_enhanced(user_id, queue_status)

        # Add appropriate buttons
        buttons = []
        if is_owner:
            owner_buttons = []
            owner_buttons.append(InlineKeyboardButton("🔄 Refresh", callback_data="refresh_queue_enhanced"))
            if queue_status['queue_size'] > 0 or queue_status['active_tasks_count'] > 0:
                owner_buttons.append(InlineKeyboardButton("❌ Clear All", callback_data="clear_queue_confirm"))
            buttons.append(owner_buttons)
            buttons.append([
                InlineKeyboardButton("📊 System Info", callback_data="system_info"),
                InlineKeyboardButton("🧹 Cleanup", callback_data="cleanup_preview")
            ])
        else:
            user_buttons = []
            user_buttons.append(InlineKeyboardButton("🔄 Refresh", callback_data="refresh_queue_enhanced"))
            if user_id in active_tasks or await check_user_in_queue(user_id):
                user_buttons.append(InlineKeyboardButton("❌ Cancel My Tasks", callback_data=f"cancel_my_tasks_confirm_{user_id}"))
            buttons.append(user_buttons)
        buttons.append([InlineKeyboardButton("❌ Close", callback_data="close_queue")])
        await message.reply(
            queue_info,
            reply_markup=InlineKeyboardMarkup(buttons),
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        logger.error(f"Enhanced queue command error: {str(e)}")
        await message.reply(f"❌ Error getting queue status: {str(e)}")
async def format_task_progress(chat_id: int, task_info: dict, is_admin_view: bool = False) -> str:
    """Format task progress in a clean, organized way with progress bars - FIXED VERSION"""
    try:
        task_type = task_info.get('type', 'unknown')
        status = task_info.get('status', 'running')
        start_time = task_info.get('start_time', time.time())
        elapsed = time.time() - start_time
        
        # Format elapsed time
        elapsed_minutes = int(elapsed // 60)
        elapsed_seconds = int(elapsed % 60)
        elapsed_str = f"{elapsed_minutes:02d}:{elapsed_seconds:02d}"
        
        # Task type mapping
        task_display_names = {
            'download_compress_file_when_ready': '📥 Downloading File',
            'handle_torrent_download': '🧲 Downloading Torrent', 
            'start_processing': '🔧 Processing Files',
            'upload_file_with_progress': '📤 Uploading Files',
            'encode_with_progress': '🎬 Encoding Video',
            'process_batch': '🔧Batch ENCODING',
            'start_all_qualities_processing': '🎯 📥 Downloading',
            'handle_download_and_process': '⚡ Download & Process'
        }
        
        display_task_name = task_display_names.get(task_type, task_type.replace('_', ' ').title())
        
        # Get real progress data if available
        real_progress = task_info.get('real_progress', 0)
        real_speed = task_info.get('real_speed', 0)
        real_eta = task_info.get('real_eta', 0)
                # Calculate progress percentage
        if real_progress is not None:
            progress = real_progress
            remaining = real_eta
        else:
            estimated_total = task_info.get('estimated_duration', 1800)
            progress = min(99, (elapsed / estimated_total) * 100) if estimated_total > 0 else 0
            remaining = max(0, estimated_total - elapsed)
        
        # Format remaining time
        if remaining > 0:
            if remaining < 60:
                remaining_str = f"{int(remaining)}s"
            elif remaining < 3600:
                remaining_str = f"{int(remaining // 60)}m {int(remaining % 60)}s"
            else:
                hours = int(remaining // 3600)
                minutes = int((remaining % 3600) // 60)
                remaining_str = f"{hours}h {minutes}m"
        else:
            remaining_str = "calculating..."
        
        # Calculate progress percentage
        progress = real_progress if real_progress is not None else 0
        
        # Format speed
        speed_str = humanize.naturalsize(real_speed) + "/s" if real_speed > 0 else "0 B/s"
        
        # Create progress bar
        progress_bar = get_random_progress_bar(progress, length=12)
        
        # Get task-specific details
        task_details = await get_detailed_task_progress(chat_id, task_info)
        
        try:
            user = await bot.get_users(chat_id)
            user_name = user.first_name or f"User {chat_id}"
        except:
            user_name = f"User {chat_id}"
        
        # Build the progress display
        if is_admin_view:
            user_status = "👑" if chat_id in SUDO_USERS else "💎" if await is_premium_user(chat_id) else "👤"
            display = (
                f"{user_status} <b>{user_name[:15]}</b> (<code>{chat_id}</code>)\n"
                f"📝 <b>Task:</b> {display_task_name}\n"
                f"📊 <b>Progress:</b> {task_details}\n"
                f"{progress_bar} <b>{progress:.1f}%</b>\n"
                f"⏱️ <b>Time:</b> {elapsed_str} | ⏳ <b>ETA:</b> {remaining_str} \n"
                f"🚀 <b>Max⬇️/⬆️:</b> {speed_str}"
               # f"🆔 <b>Task ID:</b> <code>{task_info.get('task_id', 'N/A')}</code>"
            )
        else:
            # User view - more concise
            display = (
                f"🔧 <b>Current Task Progress</b>\n\n"
                f"📝 <b>Task:</b> {display_task_name}\n"
                f"📊 <b>Status:</b> {task_details}\n\n"
                f"{progress_bar} <b>{progress:.1f}%</b>\n\n"
                f"⏱️ <b>Elapsed:</b> {elapsed_str}\n"
                f"🚀 <b>Speed:</b> {speed_str}"
            )
        
        return display
        
    except Exception as e:
        logger.error(f"Progress formatting error: {str(e)}")
        return "🔄 Processing... (Error loading details)"
async def get_owner_queue_view_enhanced(queue_status: dict) -> str:
    """Get enhanced queue view for owner - FIXED TASK ID TRACKING"""
    
    cpu_usage = psutil.cpu_percent()
    memory_usage = psutil.virtual_memory().percent
    disk_usage = psutil.disk_usage('/').percent

    # 🎯 FIXED: Get accurate counts with task ID verification
    running_tasks_count = 0
    running_tasks_list = []
    queued_tasks_count = 0
    queued_tasks_list = []
    
    # Analyze active_tasks with task ID verification
    for chat_id, task_info in list(active_tasks.items()):
        task_status = task_info.get('status', 'unknown')
        task_id = task_info.get('task_id', 'unknown')
        
        if task_status == 'running':
            running_tasks_count += 1
            running_tasks_list.append({
                'chat_id': chat_id,
                'task_info': task_info,
                'task_id': task_id
            })
        elif task_status == 'queued':
            queued_tasks_count += 1
            queued_tasks_list.append({
                'chat_id': chat_id,
                'task_info': task_info,
                'task_id': task_id
            })

    queue_info = (
        "👑 <b>Enhanced Queue Overview</b>\n"
        "<blockquote>"
        "🔄 <b>Current Status</b>\n"
        f"• Queue Size: {queue_status['queue_size']} tasks waiting\n"
        f"• Running Tasks: {running_tasks_count}\n"
        f"• Queued Tasks: {queued_tasks_count}\n"
        f"• Current Task ID: {global_queue_manager.current_task or 'None'}\n"
        "</blockquote>\n"
    )
    
    # 🎯 FIXED: Show currently RUNNING tasks with task IDs
    current_task_display = ""
    
    if running_tasks_list:
        current_task_display += "<blockquote>"
        current_task_display += "🎯 <b>Currently Running</b>\n\n"
        
        for task_data in running_tasks_list:
            chat_id = task_data['chat_id']
            task_info = task_data['task_info']
            task_id = task_data['task_id']
            
            task_display = await format_task_progress(chat_id, task_info, is_admin_view=True)
            current_task_display += f"{task_display}\n"
            current_task_display += f"🆔 <b>Task ID:</b> <code>{task_id}</code>\n"
            current_task_display += "─" * 15 + "\n"
        
        current_task_display += "</blockquote>"
    else:
        current_task_display = "<blockquote>🎯 <b>Currently Running</b>\n🔧 <i>No tasks currently running</i></blockquote>\n\n"
    
    queue_info += current_task_display
    
    # 🎯 FIXED: Show waiting queue with task IDs
    waiting_tasks = []
    
    # Add tasks from active_tasks with status 'queued'
    for task_data in queued_tasks_list:
        chat_id = task_data['chat_id']
        task_info = task_data['task_info']
        task_id = task_data['task_id']
        
        waiting_tasks.append({
            'chat_id': chat_id,
            'user_status': "👑 Sudo" if chat_id in SUDO_USERS else "💎 Premium" if await is_premium_user(chat_id) else "👤 Normal",
            'task_type': task_info.get('type', 'unknown'),
            'estimated_duration': task_info.get('estimated_duration', 1800),
            'position': task_info.get('queue_position', 0),
            'task_id': task_id
        })
    
    # Add tasks from the actual queue
    for task in queue_status['queue_list']:
        # Avoid duplicates - only add if not already in waiting_tasks
        if not any(t['chat_id'] == task['chat_id'] for t in waiting_tasks):
            waiting_tasks.append({
                'chat_id': task['chat_id'],
                'user_status': task['user_status'],
                'task_type': task['task_type'],
                'estimated_duration': task['estimated_duration'],
                'position': task['position'],
                'task_id': task['task_id']
            })
    
    if waiting_tasks:
        # Sort by position
        waiting_tasks.sort(key=lambda x: x['position'])
        
        queue_info += "<blockquote>"
        queue_info += "📋 <b>Waiting Queue</b>\n"
        
        for task in waiting_tasks:
            position = task['position']
            user_status = task['user_status']
            task_type = task['task_type']
            task_id_short = task['task_id'][:8] if task.get('task_id') else 'unknown'
            
            # Task type mapping
            task_display_names = {
                'start_all_qualities_processing': '🎯 All Qualities Processing',
                'process_batch': '🔧 Batch Processing',
                'encode_with_progress': '🎬 Video Encoding',
                'handle_download_and_process': '⚡ Download & Process'
            }
            
            display_task_name = task_display_names.get(task_type, task_type.replace('_', ' ').title())
            
            # Try to get user info for better display
            try:
                user = await bot.get_users(task['chat_id'])
                user_name = user.first_name or f"User {task['chat_id']}"
            except:
                user_name = f"User {task['chat_id']}"
                
            # Show estimated wait time
            estimated_time = humanize.naturaldelta(task['estimated_duration'])
            
            queue_info += f"#{position} {user_status} <code>{user_name[:12]}</code>\n"
            queue_info += f"   └─ {display_task_name} (~{estimated_time})\n"
            queue_info += f"   🆔 <code>{task_id_short}</code>\n\n"
            
        queue_info += "</blockquote>\n"
    else:
        queue_info += "<blockquote>📋 <b>Waiting Queue:</b> Empty</blockquote>\n"
    
    # Add system info with task IDs for debugging
    current_task_id_short = global_queue_manager.current_task[:8] if global_queue_manager.current_task else 'None'
    
    queue_info += (
        "<blockquote>"
        "💻 <b>System Overview</b>\n"
        f"• CPU: {cpu_usage}%\n"
        f"• RAM: {memory_usage}%\n"
        f"• Disk: {disk_usage}%\n"
        f"• Total Tasks Tracked: {len(active_tasks)}\n"
        f"• Running: {running_tasks_count} | Queued: {queued_tasks_count}\n"
        f"• Current Task ID: <code>{current_task_id_short}</code>\n"
        "</blockquote>\n\n"
    )
    
    return queue_info
async def get_user_queue_view_enhanced(user_id: int, queue_status: dict) -> str:
    """Get enhanced queue view for regular user - FIXED VERSION"""
    queue_info = "👤 <b>Your Queue Status</b>\n\n"
    # 🎯 FIXED: Show ONLY currently RUNNING tasks (not queued ones)
    current_task_display = ""
    running_tasks_found = False
    
    if active_tasks:
        current_task_display += "<blockquote>"
        current_task_display += "🎯 <b>Currently Running</b>\n\n"
        
        running_count = 0
        for chat_id, task_info in list(active_tasks.items()):
            # 🎯 ONLY show tasks with status 'running' in this section
            if task_info.get('status') == 'running':
                running_count += 1
                running_tasks_found = True
                
                task_display = await format_task_progress(chat_id, task_info, is_admin_view=True)
                current_task_display += f"{task_display}\n"
              #  current_task_display += "─" * 35 + "\n\n"
        
        if not running_tasks_found:
            current_task_display += "🔧 <i>No tasks currently running</i>\n"
            current_task_display += "⏳ <i>Tasks may be starting up...</i>\n"
        
        current_task_display += "</blockquote>\n\n"
    else:
        current_task_display = "<blockquote>🎯 <b>Currently Running</b>\n🔧 <i>No active tasks found</i></blockquote>\n\n"
    
    queue_info += current_task_display
    # 🎯 FIXED: Show user's running tasks

    # 🎯 FIXED: User's queued tasks (only truly queued, not running)
    user_queued_tasks = []
    for task in queue_status['queue_list']:
        if task['chat_id'] == user_id:
            # Check if this task is actually running
            if user_id in active_tasks and active_tasks[user_id].get('status') == 'running':
                continue  # Skip if it's actually running
            user_queued_tasks.append(task)
    
    if user_queued_tasks:
        queue_info += "<blockquote>"
        queue_info += "📋 <b>Your Queued Tasks</b>\n"
        
        # Calculate user's position and wait time
        user_position = user_queued_tasks[0]['position'] if user_queued_tasks else 0
        user_wait_time = queue_status['current_task_remaining']
        
        # Add wait time from tasks before user in queue
        for task in queue_status['queue_list']:
            if task['chat_id'] == user_id:
                break
            user_wait_time += task['estimated_duration']
        
        queue_info += f"📍 <b>Position in Queue:</b> #{user_position}\n"
        queue_info += f"⏳ <b>Estimated Wait:</b> {humanize.naturaldelta(user_wait_time)}\n\n"
        
        # Show queued task details
        for task in user_queued_tasks[:3]:  # Show first 3
            task_type = task['task_type']
            estimated_time = humanize.naturaldelta(task['estimated_duration'])
            
            task_display_names = {
                'start_all_qualities_processing': '🎯 All Qualities Processing',
                'process_batch': '🔧 Batch Processing', 
                'encode_with_progress': '🎬 Video Encoding',
                'handle_download_and_process': '⚡ Download & Process'
            }
            
            display_task_name = task_display_names.get(task_type, task_type.replace('_', ' ').title())
            queue_info += f"• {display_task_name} (~{estimated_time})\n"
                
        if len(user_queued_tasks) > 3:
            queue_info += f"• ... and {len(user_queued_tasks) - 3} more\n"
        queue_info += "</blockquote>\n"
    else:
        queue_info += "<blockquote>📋 <b>Queued Tasks:</b> No tasks waiting</blockquote>\n"
    
    # User's priority information
    user_priority = "👑 Sudo" if user_id in SUDO_USERS else "💎 Premium" if await is_premium_user(user_id) else "👤 Normal"
    queue_info += f"<blockquote>🎯 <b>Your Priority:</b> {user_priority}</blockquote>\n"
    
    # Queue overview
    queue_info += (
        "<blockquote>"
        "📊 <b>Queue Overview</b>\n"
        f"• Active Tasks: {queue_status['active_tasks_count']}\n"
        f"• Waiting in Queue: {queue_status['queue_size']}\n"
        "</blockquote>"
    )
    
    return queue_info
async def update_queue_status_with_estimates(self) -> dict:
    """Update queue status with accurate tracking of BOTH active and queued tasks"""
    queue_list = []
    current_time = time.time()
    
    # Convert priority queue to list safely
    try:
        temp_queue = list(self.queue._queue)
    except:
        temp_queue = []
    
    # Track ALL tasks - both active and queued
    all_tasks_estimates = {}
    
    # First, track active tasks with real progress
    for chat_id, task_info in active_tasks.items():
        task_status = task_info.get('status', 'processing')
        start_time = task_info.get('start_time', current_time)
        elapsed = current_time - start_time
        
        # For active tasks, calculate real progress
        if task_status == 'running':
            # Your existing progress calculation logic...
            if 'downloader' in task_info:
                downloader = task_info['downloader']
                progress = downloader.progress
                if progress > 0:
                    total_estimated = (elapsed / progress) * 100 if progress > 0 else 0
                    remaining = max(0, total_estimated - elapsed)
                    all_tasks_estimates[chat_id] = {
                        'remaining': remaining,
                        'progress': progress,
                        'status': 'running'
                    }
            else:
                estimated_total = task_info.get('estimated_duration', 1800)
                progress = min(99, (elapsed / estimated_total) * 100) if estimated_total > 0 else 0
                remaining = max(0, estimated_total - elapsed)
                all_tasks_estimates[chat_id] = {
                    'remaining': remaining,
                    'progress': progress,
                    'status': 'running'
                }
        else:
            # For queued but active tasks
            all_tasks_estimates[chat_id] = {
                'remaining': task_info.get('estimated_duration', 1800),
                'progress': 0,
                'status': task_status
            }
    
    # Process queue items (waiting tasks)
    for idx, (priority, task_id, task_callable, task_data) in enumerate(temp_queue):
        chat_id = task_data.get('chat_id', 0)
        user_status = "👑 Sudo" if chat_id in SUDO_USERS else "💎 Premium" if await is_premium_user(chat_id) else "👤 Normal"
        
        # Use real progress if available, otherwise use estimation
        estimated_duration = task_data.get('estimated_duration', 0)
        
        # If this task is currently active, use real progress
        if chat_id in all_tasks_estimates:
            task_estimate = all_tasks_estimates[chat_id]
            estimated_duration = task_estimate['remaining']
        
        queue_list.append({
            'position': idx + 1,
            'task_id': task_id,
            'chat_id': chat_id,
            'user_status': user_status,
            'priority': priority,
            'estimated_duration': estimated_duration,
            'task_type': task_data.get('type', 'unknown')
        })
    
    # Calculate wait times considering BOTH active and queued tasks
    current_task_remaining = 0
    active_task_count = 0
    
    # Calculate remaining time for currently running tasks
    for chat_id, estimate in all_tasks_estimates.items():
        if estimate['status'] == 'running':
            current_task_remaining += estimate['remaining']
            active_task_count += 1
    
    # Calculate queue wait time based on ACTUAL remaining times
    queue_wait_time = current_task_remaining
    
    for task in queue_list:
        chat_id = task['chat_id']
        if chat_id in all_tasks_estimates:
            queue_wait_time += all_tasks_estimates[chat_id]['remaining']
        else:
            queue_wait_time += task['estimated_duration']
    
    return {
        'current_task': self.current_task,
        'current_task_remaining': current_task_remaining,
        'queue_size': self.queue.qsize(),
        'queue_wait_time': queue_wait_time,
        'queue_list': queue_list,
        'active_tasks_count': active_task_count,  # Only count running tasks
        'all_active_tasks': len(active_tasks),    # Count all active tasks
        'active_task_estimates': all_tasks_estimates
    }
async def get_detailed_task_progress(chat_id: int, task_info: dict) -> str:
    """Get detailed progress information for a specific task - FIXED VERSION"""
    try:
        task_type = task_info.get('type', 'unknown')
        session = await get_user_settings(chat_id)
        
        if not session:
            return "Loading settings..."
        
        # 🎯 FIXED: Proper task type mapping with accurate status
        if task_type == 'download_compress_file_when_ready':
            if 'downloader' in task_info:
                downloader = task_info['downloader']
                progress = downloader.progress
                speed = humanize.naturalsize(downloader.download_speed) + "/s" if downloader.download_speed > 0 else "0 B/s"
                return f"📥 Downloading {progress:.1f}% | 🚀 {speed}"
            else:
                return "📥 Starting file download..."
        
        elif task_type == 'handle_torrent_download':
            if 'downloader' in task_info:
                downloader = task_info['downloader']
                progress = downloader.progress
                speed = humanize.naturalsize(downloader.download_speed) + "/s" if downloader.download_speed > 0 else "0 B/s"
                peers = downloader.peers
                return f"📥 Downloading {progress:.1f}% | 🚀 {speed} | 👥 {peers} peers"
            else:
                return "📥 Starting torrent download..."
        
        # 🎯 FIXED: Proper encoding task detection
        elif task_type in ['encode_with_progress', 'start_processing', 'process_batch']:
            # Check if it's All Qualities mode
            if task_info.get('all_qualities', False):
                selected_qualities = session.get('selected_qualities', [])
                quality_display = f"{len(selected_qualities)} qualities"
                current_file = task_info.get('current_file', 1)
                total_files = task_info.get('total_files', 1)
                
                # Get real encoding progress
                real_progress = task_info.get('real_progress', 0)
                if real_progress > 0:
                    return f"🎬 Encoding {quality_display} | 📁 {current_file}/{total_files} files | {real_progress:.1f}%"
                else:
                    return f"🎬 Starting {quality_display} encoding | 📁 {current_file}/{total_files} files"
            else:
                quality = session.get('quality', 'Unknown')
                current_file = task_info.get('current_file', 1)
                total_files = task_info.get('total_files', 1)
                real_progress = task_info.get('real_progress', 0)
                
                if real_progress > 0:
                    return f"🎬 Encoding {quality.upper()} | 📁 {current_file}/{total_files} files | {real_progress:.1f}%"
                else:
                    return f"🎬 Starting {quality.upper()} encoding | 📁 {current_file}/{total_files} files"
        
        # 🎯 FIXED: Handle All Qualities processing specifically
        elif task_type == 'start_all_qualities_processing':
            selected_qualities = session.get('selected_qualities', [])
            files = session.get('files', [])
            total_files = len(files) if files else 1
            return f"🎯 Processing {total_files} files in {len(selected_qualities)} qualities"
        
        elif task_type == 'handle_download_and_process':
            return "⚡ Downloading & Processing"
        
        # 🎯 FIXED: Handle upload tasks
        elif task_type == 'upload_file_with_progress':
            files = session.get('files', [])
            total_files = len(files) if files else 1
            current_file = task_info.get('current_file', 0)
            return f"📤 Uploading {current_file + 1}/{total_files} files"
        
        # Default fallback with better task names
        task_display_names = {
            'start_all_qualities_processing': '🎯 All Qualities Processing',
            'handle_download_and_process': '⚡ Download & Process', 
            'process_batch': '🔧 Batch Processing',
            'encode_with_progress': '🎬 Video Encoding',
            'upload_file_with_progress': '📤 File Upload'
        }
        
        return task_display_names.get(task_type, f"🔄 {task_type.replace('_', ' ').title()}")
                
    except Exception as e:
        logger.warning(f"Error getting detailed progress: {str(e)}")
        return "🔄 Processing..."
@bot.on_callback_query(filters.regex("^cancel_my_tasks$"))
async def cancel_my_tasks_handler(client: Client, query: CallbackQuery):
    """Cancel only the current user's tasks"""
    try:
        user_id = query.from_user.id
        # Count user's active tasks
        user_task_count = 0
        for task_chat_id in list(active_tasks.keys()):
            if task_chat_id == user_id:
                user_task_count += 1
        if user_task_count == 0:
            await query.answer("❌ You have no active tasks to cancel!", show_alert=True)
            return
        buttons = [
            [
                InlineKeyboardButton("✅ Yes, Cancel My Tasks", callback_data=f"cancel_my_tasks_confirm_{user_id}"),
                InlineKeyboardButton("❌ No, Keep Running", callback_data="cancel_cancel")
            ]
        ]
        await query.message.edit_text(
            f"⚠️ **Cancel Your Tasks**\n\n"
            f"Are you sure you want to cancel YOUR tasks?\n\n"
            f"📊 **Your active tasks:** {user_task_count}\n\n"
            f"This will stop:\n"
            f"• Your current downloads\n"
            f"• Your encoding processes\n"
            f"• Your uploads in progress\n\n"
            f"**Other users' tasks will continue running.**",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        await query.answer()
    except Exception as e:
        await query.answer("Error!")
@bot.on_callback_query(filters.regex(r"^cancel_my_tasks_confirm_(\d+)$"))
async def cancel_my_tasks_confirm_handler(client: Client, query: CallbackQuery):
    """Confirm cancellation of user's own tasks from queue menu - FIXED VERSION"""
    try:
        user_id = int(query.data.split("_")[4])
        # Verify it's the same user
        if query.from_user.id != user_id:
            await query.answer("❌ You can only cancel your own tasks!", show_alert=True)
            return
        await query.answer("🛑 Cancelling your tasks...")
        # Get task counts before cancellation
        active_before = user_id in active_tasks
        queued_before = await count_user_queued_tasks(user_id)
        # Cancel user's tasks
        success = await cancel_user_tasks(user_id)
        if success:
            # Get task counts after cancellation
            active_after = user_id in active_tasks
            queued_after = await count_user_queued_tasks(user_id)
            result_text = "✅ **Your Tasks Cancelled Successfully!**\n\n"
            if active_before and not active_after:
                result_text += "• Stopped active task\n"
            if queued_before > queued_after:
                result_text += f"• Removed {queued_before - queued_after} queued task(s)\n"
            result_text += "\nYou can start new tasks whenever you're ready."
            await query.message.edit_text(result_text)
            logger.info(f"✅ User {user_id} cancelled their tasks from queue menu")
        else:
            await query.message.edit_text("❌ Failed to cancel tasks. They may have already completed.")
    except Exception as e:
        logger.error(f"Cancel my tasks confirm error: {str(e)}")
        await query.message.edit_text("❌ Error cancelling tasks!")
        await query.answer("Error!")
@bot.on_callback_query(filters.regex("^cancel_my_tasks_no$"))
async def cancel_my_tasks_no_handler(client: Client, query: CallbackQuery):
    """Cancel the cancellation of user's tasks"""
    await query.message.delete()
    await query.answer("Cancellation cancelled")
@bot.on_callback_query(filters.regex("^close_queue$"))
async def close_queue_handler(client: Client, query: CallbackQuery):
    """Close queue message with confirmation"""
    try:
        await query.answer("Queue closed")
        await query.message.delete()
    except Exception as e:
        await query.answer("Error closing queue")
@bot.on_callback_query(filters.regex("^refresh_queue_enhanced$"))
async def refresh_queue_enhanced_handler(client: Client, query: CallbackQuery):
    """Refresh enhanced queue status with proper error handling"""
    try:
        await query.answer("🔄 Refreshing queue...")
        user_id = query.from_user.id
        is_owner = user_id in SUDO_USERS
        
        # Get fresh queue status
        queue_status = await global_queue_manager.get_queue_status_with_estimates()
        
        if is_owner:
            queue_info = await get_owner_queue_view_enhanced(queue_status)
        else:
            queue_info = await get_user_queue_view_enhanced(user_id, queue_status)


        # Same button layout as enhanced_queue_handler
        buttons = []
        if is_owner:
            owner_buttons = []
            owner_buttons.append(InlineKeyboardButton("🔄 Refresh", callback_data="refresh_queue_enhanced"))
            if queue_status['queue_size'] > 0 or queue_status['active_tasks_count'] > 0:
                owner_buttons.append(InlineKeyboardButton("❌ Clear All", callback_data="clear_queue_confirm"))
            buttons.append(owner_buttons)
            buttons.append([
                InlineKeyboardButton("📊 System Info", callback_data="system_info"),
                InlineKeyboardButton("🧹 Cleanup", callback_data="cleanup_preview")
            ])
        else:
            user_buttons = []
            user_buttons.append(InlineKeyboardButton("🔄 Refresh", callback_data="refresh_queue_enhanced"))
            if user_id in active_tasks or await check_user_in_queue(user_id):
                user_buttons.append(InlineKeyboardButton("❌ Cancel My Tasks", callback_data=f"cancel_my_tasks_confirm_{user_id}"))
            buttons.append(user_buttons)
        buttons.append([InlineKeyboardButton("❌ Close", callback_data="close_queue")])

        try:
            await query.message.edit_text(
                queue_info,
                reply_markup=InlineKeyboardMarkup(buttons),
                parse_mode=ParseMode.HTML
            )
        except MessageNotModified:
            # This is fine - content hasn't changed
            await query.answer("✅ Queue status is up to date")
        except Exception as e:
            logger.warning(f"Edit message failed: {str(e)}")
            await query.answer("⚠️ Could not update display")
            
    except Exception as e:
        logger.error(f"Enhanced refresh queue error: {str(e)}")
        await query.answer("❌ Error refreshing queue!")
@bot.on_callback_query(filters.regex("^cancel_cancel$"))
async def cancel_cancel_handler(client: Client, query: CallbackQuery):
    """Cancel the cancellation request"""
    await query.message.delete()
    await query.answer("Cancellation cancelled")
@bot.on_callback_query(filters.regex("^clear_queue_confirm$"))
async def clear_queue_confirm_handler(client: Client, query: CallbackQuery):
    """Confirm queue clearance - Bot owner only"""
    try:
        # Check if user is bot owner
        if query.from_user.id not in SUDO_USERS:
            await query.answer("❌ Only bot owner can clear queue!", show_alert=True)
            return
        buttons = [
            [
                InlineKeyboardButton("✅ Yes, Clear All", callback_data="clear_queue_yes"),
                InlineKeyboardButton("❌ No, Keep Queue", callback_data="clear_queue_no")
            ]
        ]
        await query.message.edit_text(
            "⚠️ **Clear Queue Confirmation**\n\n"
            "Are you sure you want to clear ALL tasks from the queue?\n\n"
            "This will:\n"
            "• Stop current encoding/download tasks\n"
            "• Remove all pending tasks\n"
            "• Cancel all user operations\n\n"
            "**This action cannot be undone!**",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        await query.answer()
    except Exception as e:
        await query.answer("Error!")
@bot.on_callback_query(filters.regex("^clear_queue_yes$"))
async def clear_queue_yes_handler(client: Client, query: CallbackQuery):
    """Clear all tasks from queue - Bot owner only"""
    try:
        # Check if user is bot owner
        if query.from_user.id not in SUDO_USERS:
            await query.answer("❌ Only bot owner can clear queue!", show_alert=True)
            return
        # Cancel all active tasks using user-specific cancellation
        cancelled_users = set()
        for chat_id in list(active_tasks.keys()):
            await cancel_user_tasks(chat_id)
            cancelled_users.add(chat_id)
        # Clear queue properly
        queue_size = global_queue_manager.queue.qsize()
        for _ in range(queue_size):
            try:
                global_queue_manager.queue.get_nowait()
                global_queue_manager.queue.task_done()
            except:
                break
        # Reset current task
        global_queue_manager.current_task = None
        global_queue_manager.current_process = None
        await query.message.edit_text(
            "✅ **Queue Cleared Successfully**\n\n"
            f"🗑️ **Tasks Removed:** {queue_size} pending tasks\n"
            f"🛑 **Active Tasks:** {len(cancelled_users)} users affected\n"
            f"👥 **Users Cancelled:** {len(cancelled_users)}\n\n"
            "Users will need to restart their operations."
        )
        await query.answer("Queue cleared!")
        logger.info(f"Queue cleared by {query.from_user.id}, affected {len(cancelled_users)} users")
    except Exception as e:
        await query.message.edit_text(f"❌ Error clearing queue: {str(e)}")
        await query.answer("Error!")
@bot.on_callback_query(filters.regex("^clear_queue_no$"))
async def clear_queue_no_handler(client: Client, query: CallbackQuery):
    """Cancel queue clearance"""
    await query.message.delete()
    await query.answer("Queue clearance cancelled")
@bot.on_callback_query(filters.regex("^system_info$"))
async def system_info_handler(client: Client, query: CallbackQuery):
    """Show detailed system information with active tasks - FIXED VERSION"""
    try:
        # System information
        cpu_usage = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        boot_time = datetime.fromtimestamp(psutil.boot_time())
        uptime = datetime.now() - boot_time
        # Active tasks details
        active_tasks_count = len(active_tasks)  # 🎯 ADD THIS
        active_tasks_info = "🔧 **Active Tasks:**\n"
        if active_tasks:
            for chat_id, task_info in list(active_tasks.items())[:5]:  # Show first 5
                task_type = task_info.get('type', 'unknown')
                status = task_info.get('status', 'running')
                start_time = task_info.get('start_time', time.time())
                elapsed = time.time() - start_time
                elapsed_str = f"{int(elapsed // 60)}m {int(elapsed % 60)}s"
                active_tasks_info += f"• User {chat_id} - {task_type} - {elapsed_str}\n"
            if len(active_tasks) > 5:
                active_tasks_info += f"• ... and {len(active_tasks) - 5} more\n"
        else:
            active_tasks_info += "• No active tasks\n"
        system_info = (
            f"🖥️ **System Information**\n\n"
            f"💻 **CPU Usage:** {cpu_usage}%\n"
            f"🧠 **Memory:** {memory.percent}% ({humanize.naturalsize(memory.used)} / {humanize.naturalsize(memory.total)})\n"
            f"💾 **Disk:** {disk.percent}% ({humanize.naturalsize(disk.used)} / {humanize.naturalsize(disk.total)})\n"
            f"⏰ **Uptime:** {str(uptime).split('.')[0]}\n"
            f"🤖 **Bot Status:** {'🔒 Sudo only' if BOT_AUTHORIZED else '🔓 All users'}\n"
            f"⚡ **Active Tasks:** {active_tasks_count}\n\n"  # 🎯 ADDED active tasks count
            f"{active_tasks_info}\n"
            f"🕒 **Last Updated:** {datetime.now().strftime('%H:%M:%S')}"
        )
        buttons = [
            [InlineKeyboardButton("🧹 Run Cleanup", callback_data="cleanup_preview")],
            [InlineKeyboardButton("🔙 Back to Queue", callback_data="refresh_queue")],
            [InlineKeyboardButton("🔄 Refresh", callback_data="system_info")],
            [InlineKeyboardButton("❌ Close", callback_data="close_queue")]
        ]
        await query.message.edit_text(
            system_info,
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        await query.answer()
    except Exception as e:
        await query.answer("Error getting system info!")
@bot.on_message(filters.command("tasklist") & filters.private)
@sudo_only
async def task_list_handler(client: Client, message: Message):
    """List all active tasks with IDs - Sudo only"""
    try:
        if not active_tasks:
            await message.reply("ℹ️ No active tasks found.")
            return
        
        task_list = "📋 **Active Tasks List**\n\n"
        
        for chat_id, task_info in active_tasks.items():
            task_id = task_info.get('task_id', 'Unknown')
            task_type = task_info.get('type', 'Unknown')
            status = task_info.get('status', 'Unknown')
            
            # Get user info
            try:
                user = await bot.get_users(chat_id)
                user_display = f"{user.first_name} (@{user.username})" if user.username else user.first_name
            except:
                user_display = f"User {chat_id}"
            
            task_list += f"👤 **User:** {user_display}\n"
            task_list += f"🆔 **Task ID:** `{task_id}`\n"
            task_list += f"📝 **Type:** {task_type}\n"
            task_list += f"📊 **Status:** {status}\n"
            task_list += "─" * 20 + "\n\n"
        
        # Add buttons for quick actions
        buttons = [
            [InlineKeyboardButton("🛑 Cancel All Tasks", callback_data="confirm_cancel_all_owner")],
            [InlineKeyboardButton("📊 Queue Status", callback_data="refresh_queue_enhanced")],
            [InlineKeyboardButton("❌ Close", callback_data="close_menu")]
        ]
        
        await message.reply(
            task_list,
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        
    except Exception as e:
        logger.error(f"Task list error: {str(e)}")
        await message.reply(f"❌ Error getting task list: {str(e)}")
@bot.on_message(filters.command("canceluser") & filters.private)
@sudo_only
async def cancel_user_tasks_handler(client: Client, message: Message):
    """Cancel all tasks for a specific user - Sudo only"""
    try:
        if len(message.command) < 2:
            await message.reply(
                "❌ **Usage:** `/canceluser <user_id>`\n\n"
                "💡 **Examples:**\n"
                "• `/canceluser 123456789`\n"
                "• `/canceluser @username`\n\n"
                "⚠️ **This will cancel ALL tasks for the specified user.**"
            )
            return
        
        user_input = message.command[1].strip()
        
        # Parse user ID
        try:
            if user_input.startswith('@'):
                # Get user by username
                user = await bot.get_users(user_input)
                target_user_id = user.id
            else:
                target_user_id = int(user_input)
        except (ValueError, Exception):
            await message.reply("❌ Invalid user ID or username!")
            return
        
        # Get user info
        try:
            user = await bot.get_users(target_user_id)
            user_display = f"{user.first_name} (@{user.username})" if user.username else user.first_name
        except:
            user_display = f"User {target_user_id}"
        
        # Check if user has active tasks
        has_active_tasks = target_user_id in active_tasks
        queued_count = await count_user_queued_tasks(target_user_id)
        
        if not has_active_tasks and queued_count == 0:
            await message.reply(f"ℹ️ User {user_display} has no active or queued tasks.")
            return
        
        # Show confirmation
        await message.reply(
            f"🛑 **Cancel User Tasks**\n\n"
            f"👤 **User:** {user_display}\n"
            f"🆔 **User ID:** `{target_user_id}`\n"
            f"📊 **Active Tasks:** {1 if has_active_tasks else 0}\n"
            f"📋 **Queued Tasks:** {queued_count}\n\n"
            f"⚠️ **This will cancel ALL tasks for this user!**\n\n"
            f"Are you sure?",
            reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("✅ Yes, Cancel All", callback_data=f"confirm_cancel_user_{target_user_id}"),
                    InlineKeyboardButton("❌ No, Keep Running", callback_data="cancel_cancel")
                ]
            ])
        )
        
    except Exception as e:
        logger.error(f"Cancel user tasks error: {str(e)}")
        await message.reply(f"❌ Error: {str(e)}")

@bot.on_callback_query(filters.regex(r"^confirm_cancel_user_(\d+)$"))
async def confirm_cancel_user_handler(client: Client, query: CallbackQuery):
    """Confirm cancellation of all tasks for a specific user"""
    try:
        target_user_id = int(query.data.split("_")[3])
        
        # Get user info
        try:
            user = await bot.get_users(target_user_id)
            user_display = f"{user.first_name} (@{user.username})" if user.username else user.first_name
        except:
            user_display = f"User {target_user_id}"
        
        # Cancel user's tasks
        success = await cancel_user_tasks(target_user_id)
        
        if success:
            await query.message.edit_text(
                f"✅ **User Tasks Cancelled**\n\n"
                f"👤 **User:** {user_display}\n"
                f"🆔 **User ID:** `{target_user_id}`\n"
                f"👑 **Cancelled by:** {query.from_user.mention}\n\n"
                f"All tasks for this user have been stopped."
            )
            
            logger.info(f"👑 Sudo {query.from_user.id} cancelled all tasks for user {target_user_id}")
        else:
            await query.message.edit_text("❌ Failed to cancel user tasks.")
            
    except Exception as e:
        logger.error(f"Confirm cancel user error: {str(e)}")
        await query.message.edit_text("❌ Error cancelling user tasks!")
@bot.on_message(filters.command("authorize") & filters.private)
@sudo_only
async def authorize_handler(client: Client, message: Message):
    """Toggle bot authorization on/off with MongoDB storage"""
    global BOT_AUTHORIZED
    try:
        if len(message.command) > 1:
            action = message.command[1].lower()
            if action in ["on", "enable", "true", "1"]:
                BOT_AUTHORIZED = True
                status = "🔒 **Bot Authorization ENABLED**\n\nOnly sudo users can use the bot."
            elif action in ["off", "disable", "false", "0"]:
                BOT_AUTHORIZED = False
                status = "🔓 **Bot Authorization DISABLED**\n\nAll users can use the bot."
            elif action == "status":
                await authorize_status_handler(client, message)
                return
            else:
                await message.reply(
                    "❌ Invalid command. Usage:\n"
                    "`/authorize on` - Restrict to sudo users only\n"
                    "`/authorize off` - Allow all users\n"
                    "`/authorize status` - Check status"
                )
                return
        else:
            # Toggle current status
            BOT_AUTHORIZED = not BOT_AUTHORIZED
            status = "🔒 **Bot Authorization ENABLED**\n\nOnly sudo users can use the bot." if BOT_AUTHORIZED else "🔓 **Bot Authorization DISABLED**\n\nAll users can use the bot."
        # Save to MongoDB
        await save_bot_settings()  # THIS IS THE MISSING FUNCTION
        await message.reply(
            f"{status}\n\n"
            f"🆔 **By:** {message.from_user.mention}\n"
            f"💾 **Saved to:** MongoDB\n"
            f"🕒 **Time:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        logger.info(f"Bot authorization set to {BOT_AUTHORIZED} by {message.from_user.id}")
    except Exception as e:
        logger.error(f"Authorize command error: {str(e)}")
        await message.reply(f"❌ Error: {str(e)}")
@bot.on_message(filters.command("authorize status") & filters.private)
async def authorize_status_handler(client: Client, message: Message):
    """Check bot authorization status with MongoDB info"""
    global BOT_AUTHORIZED
    # Load current status from MongoDB to ensure we have latest
    await load_bot_settings()
    status_text = (
        f"🤖 **Bot Authorization Status**\n\n"
        f"🔐 **Mode:** {'🔒 RESTRICTED (Sudo only)' if BOT_AUTHORIZED else '🔓 PUBLIC (All users)'}\n"
        f"👥 **Sudo Users:** {len(SUDO_USERS)}\n"
      #  f"💾 **Storage:** MongoDB\n"
      #  f"🕒 **Last Updated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
    )
    if BOT_AUTHORIZED:
        status_text += "Only authorized sudo users can use the bot."
    else:
        status_text += "All users can use the bot normally."
    await message.reply(status_text)
# Fix the cancel_all handler
@bot.on_message(filters.command("cancel_all") & filters.private)
@sudo_only
async def cancel_all_handler(client: Client, message: Message):
    """Cancel ALL tasks - Owner only - FIXED VERSION"""
    try:
        user_id = message.from_user.id
        if user_id not in SUDO_USERS:
            await message.reply("❌ Only bot owner can cancel all tasks!")
            return
        total_active = len(active_tasks)
        total_queued = global_queue_manager.queue.qsize()
        if total_active == 0 and total_queued == 0:
            await message.reply("ℹ️ No active tasks found to cancel.")
            return
        # Show confirmation with detailed counts
        await message.reply(
            f"👑 **Cancel ALL Tasks**\n\n"
            f"📊 **Current System Status:**\n"
            f"• Active Tasks: {total_active}\n"
            f"• Queued Tasks: {total_queued}\n"
            f"• Affected Users: {len(active_tasks)}\n\n"
            "⚠️ **This will cancel EVERYTHING:**\n"
            "• All active downloads/encoding\n"
            "• Entire task queue\n"
            "• All user operations\n\n"
            "**This action affects ALL users and cannot be undone!**",
            reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("✅ Yes, Cancel EVERYTHING", callback_data="confirm_cancel_all_owner"),
                    InlineKeyboardButton("❌ No, Keep Running", callback_data="cancel_cancel_all")
                ]
            ])
        )
    except Exception as e:
        logger.error(f"Cancel all error: {str(e)}")
        await message.reply(f"❌ Error cancelling all tasks: {str(e)}")
# Add missing cancel_all confirmation handler
@bot.on_callback_query(filters.regex("^confirm_cancel_all_owner$"))
async def confirm_cancel_all_owner_handler(client: Client, query: CallbackQuery):
    """Handle confirmation of cancel ALL tasks (owner only) - FIXED VERSION"""
    try:
        # Check if user is owner
        if query.from_user.id not in SUDO_USERS:
            await query.answer("❌ Only bot owner can cancel all tasks!", show_alert=True)
            return
        await query.answer("🛑 Cancelling ALL tasks...")
        # Get counts before cancellation
        active_before = len(active_tasks)
        queued_before = global_queue_manager.queue.qsize()
        users_before = list(active_tasks.keys())
        # Cancel all tasks for every user
        cancelled_users = set()
        for chat_id in list(active_tasks.keys()):
            success = await cancel_user_tasks(chat_id)
            if success:
                cancelled_users.add(chat_id)
        # Clear the entire queue
        queue_cleared = await clear_entire_queue()
        # Build result message
        result_text = "👑 **All Tasks Cancelled Successfully!**\n\n"
        if active_before > 0:
            result_text += f"• Stopped {active_before} active task(s)\n"
        if queued_before > 0:
            result_text += f"• Cleared {queued_before} queued task(s)\n"
        if cancelled_users:
            result_text += f"• Affected {len(cancelled_users)} user(s)\n"
        result_text += "\n✅ All operations have been stopped system-wide."
        await query.message.edit_text(result_text)
        logger.info(f"👑 Owner {query.from_user.id} cancelled ALL tasks: {active_before} active, {queued_before} queued")
    except Exception as e:
        logger.error(f"Confirm cancel all owner error: {str(e)}")
        await query.message.edit_text("❌ Error cancelling all tasks!")
        await query.answer("Error!")
# Add missing cancel_all cancellation handler
@bot.on_callback_query(filters.regex("^cancel_cancel_all$"))
async def cancel_cancel_all_handler(client: Client, query: CallbackQuery):
    """Handle cancellation of cancel_all request"""
    await query.message.delete()
    await query.answer("Cancellation cancelled")
@bot.on_message(filters.command("addsudo") & filters.private)
@sudo_only
async def add_sudo_handler(client: Client, message: Message):
    """Add a user to sudo users list with MongoDB storage"""
    try:
        if len(message.command) < 2:
            await message.reply(
                "❌ Please provide a user ID to add as sudo\n\n"
                "Usage: `/addsudo <user_id>`\n"
                "Example: `/addsudo 123456789`"
            )
            return
        try:
            new_sudo_id = int(message.command[1])
        except ValueError:
            await message.reply("❌ User ID must be a number!")
            return
        if new_sudo_id in SUDO_USERS:
            await message.reply(f"❌ User `{new_sudo_id}` is already a sudo user!")
            return
        # Add to sudo users
        SUDO_USERS.append(new_sudo_id)
        # Save to MongoDB
        await save_sudo_users()
        await message.reply(
            f"✅ **Sudo User Added Successfully!**\n\n"
            f"👤 **User ID:** `{new_sudo_id}`\n"
            f"👑 **Added by:** {message.from_user.mention}\n"
            f"📊 **Total Sudo Users:** {len(SUDO_USERS)}\n"
            f"💾 **Saved to:** MongoDB\n"
            f"🕒 **Time:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        logger.info(f"Sudo user added: {new_sudo_id} by {message.from_user.id}")
    except Exception as e:
        logger.error(f"Add sudo error: {str(e)}")
        await message.reply(f"❌ Error adding sudo user: {str(e)}")
@bot.on_message(filters.command("rmsudo") & filters.private)
@sudo_only
async def remove_sudo_handler(client: Client, message: Message):
    """Remove a user from sudo users list with MongoDB storage"""
    try:
        if len(message.command) < 2:
            await message.reply(
                "❌ Please provide a user ID to remove from sudo\n\n"
                "Usage: `/rmsudo <user_id>`\n"
                "Example: `/rmsudo 123456789`"
            )
            return
        try:
            remove_sudo_id = int(message.command[1])
        except ValueError:
            await message.reply("❌ User ID must be a number!")
            return
        if remove_sudo_id == BOT_OWNER_ID:
            await message.reply("❌ Cannot remove the bot owner from sudo users!")
            return
        if remove_sudo_id not in SUDO_USERS:
            await message.reply(f"❌ User `{remove_sudo_id}` is not a sudo user!")
            return
        # Remove from sudo users
        SUDO_USERS.remove(remove_sudo_id)
        # Save to MongoDB
        await save_sudo_users()
        await message.reply(
            f"✅ **Sudo User Removed Successfully!**\n\n"
            f"👤 **User ID:** `{remove_sudo_id}`\n"
            f"👑 **Removed by:** {message.from_user.mention}\n"
            f"📊 **Total Sudo Users:** {len(SUDO_USERS)}\n"
            f"💾 **Saved to:** MongoDB\n"
            f"🕒 **Time:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        logger.info(f"Sudo user removed: {remove_sudo_id} by {message.from_user.id}")
    except Exception as e:
        logger.error(f"Remove sudo error: {str(e)}")
        await message.reply(f"❌ Error removing sudo user: {str(e)}")
@bot.on_message(filters.command("cleanup") & (filters.private | filters.group))
@sudo_only
async def cleanup_handler(client: Client, message: Message):
    """Full cleanup with enhanced preview - FIXED VERSION"""
    try:
        user_id = message.from_user.id
        is_owner = user_id in SUDO_USERS
        # Check authorization
        if not is_owner:
            await message.reply("❌ Only sudo users can use cleanup command!")
            return
        # Show initial scanning message
        scan_msg = await message.reply("🔍 **Scanning storage for cleanup candidates...**\n\nPlease wait while I check all folders...")
        # Get detailed preview
        preview_info = await get_cleanup_preview()
        # Delete scanning message
        try:
            await scan_msg.delete()
        except:
            pass
        # Show cleanup confirmation with detailed preview
        cleanup_msg = await message.reply(
            preview_info,
            reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("✅ Clean Now", callback_data="cleanup_confirm_all"),
                    InlineKeyboardButton("🔄 Rescan", callback_data="cleanup_preview")
                ],
                [
                    InlineKeyboardButton("📊 Storage Info", callback_data="storage_info"),
                    InlineKeyboardButton("❌ Cancel", callback_data="cleanup_cancel")
                ]
            ])
        )
    except Exception as e:
        logger.error(f"Cleanup command error: {str(e)}")
        await message.reply(f"❌ Error: {str(e)}")
@bot.on_callback_query(filters.regex("^cleanup_confirm_all$"))
async def cleanup_confirm_all_handler(client: Client, query: CallbackQuery):
    """Confirm and execute full cleanup with progress"""
    try:
        # Check if user is sudo
        if query.from_user.id not in SUDO_USERS:
            await query.answer("❌ Only sudo users can perform cleanup!", show_alert=True)
            return
        # Show cleanup in progress
        progress_msg = await query.message.edit_text(
            "🧹 **Starting FULL Storage Cleanup**\n\n"
            "🔍 Scanning and deleting files...\n"
            "⏳ This may take a few moments...\n\n"
            "📂 Processing folders:\n"
            "• downloads\n• encoded\n• tempwork\n• previews\n• screenshots\n• samples"
        )
        # Get disk usage before cleanup
        disk_before = psutil.disk_usage('/')
        # Perform cleanup with folder-by-folder progress
        total_deleted = 0
        total_freed = 0
        folders = ["downloads", "encoded", "tempwork", "previews", "screenshots", "samples", "subtitles", "audio"]
        for i, folder in enumerate(folders):
            try:
                # Update progress
                await progress_msg.edit_text(
                    f"🧹 **Cleaning Up...** ({i+1}/{len(folders)})\n\n"
                    f"📂 Currently cleaning: `{folder}`\n"
                    f"✅ Completed: {i} folders\n"
                    f"🗑️ Files deleted: {total_deleted}\n"
                    f"💾 Space freed: {humanize.naturalsize(total_freed)}\n\n"
                    "⏳ Please wait..."
                )
                # Clean this folder
                folder_deleted, folder_freed = await cleanup_folder(folder)
                total_deleted += folder_deleted
                total_freed += folder_freed
                # Small delay to show progress
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"Error cleaning {folder}: {str(e)}")
                continue
        # Get disk usage after cleanup
        disk_after = psutil.disk_usage('/')
        actual_freed = disk_after.free - disk_before.free
        # Show results
        if total_deleted == 0:
            result_text = (
                "✅ **Storage is Already Clean!**\n\n"
                "No files older than 1 hour were found.\n"
                "All temporary files are within retention period."
            )
        else:
            result_text = (
                f"✅ **Full Cleanup Complete!** 🎉\n\n"
                f"📊 **Cleanup Results:**\n"
                f"• 🗑️ Files/Folders deleted: {total_deleted}\n"
                f"• 💾 Space freed: {humanize.naturalsize(total_freed)}\n"
                f"• 📈 Actual disk gain: {humanize.naturalsize(actual_freed) if actual_freed > 0 else '0 Bytes'}\n"
                f"• 📂 Folders processed: {len(folders)}\n\n"
               # f"🕒 **Completed at:** {datetime.now().strftime('%H:%M:%S')}\n"
                f"_All temporary storage has been cleaned._"
            )
        # Add action buttons
        buttons = [
            [InlineKeyboardButton("🔄 Rescan", callback_data="cleanup_preview")],
            [InlineKeyboardButton("📊 Storage Info", callback_data="storage_info")],
            [InlineKeyboardButton("❌ Close", callback_data="cleanup_cancel")]
        ]
        await progress_msg.edit_text(
            result_text,
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        await query.answer("Cleanup completed!")
        logger.info(f"🧹 Cleanup completed: {total_deleted} items, {humanize.naturalsize(total_freed)} freed")
    except Exception as e:
        logger.error(f"Cleanup execution error: {str(e)}")
        await query.message.edit_text(f"❌ Cleanup failed: {str(e)}")
        await query.answer("Error!")
async def cleanup_folder(folder: str) -> Tuple[int, int]:
    """Cleanup a specific folder and return (deleted_count, freed_space)"""
    try:
        if not ospath.exists(folder):
            return 0, 0
        deleted_count = 0
        freed_space = 0
        current_time = time.time()
        for item in os.listdir(folder):
            item_path = ospath.join(folder, item)
            try:
                if ospath.isfile(item_path):
                    file_age = current_time - ospath.getmtime(item_path)
                    if file_age > 3600:  # 1 hour
                        file_size = ospath.getsize(item_path)
                        osremove(item_path)
                        freed_space += file_size
                        deleted_count += 1
                elif ospath.isdir(item_path):
                    dir_age = current_time - ospath.getmtime(item_path)
                    if dir_age > 3600:  # 1 hour
                        dir_size = 0
                        # Calculate directory size
                        for root, dirs, files in os.walk(item_path):
                            for file in files:
                                try:
                                    file_path = ospath.join(root, file)
                                    dir_size += ospath.getsize(file_path)
                                except:
                                    continue
                        # Delete directory
                        shutil.rmtree(item_path, ignore_errors=True)
                        freed_space += dir_size
                        deleted_count += 1  # Count directory as 1 item
            except Exception as e:
                continue
        return deleted_count, freed_space
    except Exception as e:
        logger.error(f"Error cleaning folder {folder}: {str(e)}")
        return 0, 0
@bot.on_callback_query(filters.regex("^storage_info$"))
async def storage_info_handler(client: Client, query: CallbackQuery):
    """Show detailed storage information"""
    try:
        if query.from_user.id not in SUDO_USERS:
            await query.answer("❌ Only sudo users can view storage info!", show_alert=True)
            return
        await query.answer("📊 Getting storage info...")
        # Get disk usage
        disk = psutil.disk_usage('/')
        # Scan all bot folders
        bot_folders = ["downloads", "encoded", "tempwork", "previews", "screenshots", "samples", "thumbnails", "watermarks", "subtitles", "audio"]
        storage_info = "💾 **Storage Information**\n\n"
        storage_info += f"🖥️ **System Disk:**\n"
        storage_info += f"• Total: {humanize.naturalsize(disk.total)}\n"
        storage_info += f"• Used: {humanize.naturalsize(disk.used)} ({disk.percent}%)\n"
        storage_info += f"• Free: {humanize.naturalsize(disk.free)}\n\n"
        storage_info += "📁 **Bot Folders:**\n"
        total_bot_size = 0
        folder_details = []
        for folder in bot_folders:
            if ospath.exists(folder):
                folder_size = 0
                file_count = 0
                for root, dirs, files in os.walk(folder):
                    for file in files:
                        try:
                            file_path = ospath.join(root, file)
                            folder_size += ospath.getsize(file_path)
                            file_count += 1
                        except:
                            continue
                total_bot_size += folder_size
                if folder_size > 0:
                    folder_details.append({
                        'name': folder,
                        'size': folder_size,
                        'files': file_count
                    })
        # Sort folders by size
        folder_details.sort(key=lambda x: x['size'], reverse=True)
        for folder_info in folder_details:
            storage_info += f"• **{folder_info['name']}:** {humanize.naturalsize(folder_info['size'])} ({folder_info['files']} files)\n"
        storage_info += f"\n📈 **Total Bot Usage:** {humanize.naturalsize(total_bot_size)}\n"
        storage_info += f"📅 **Last checked:** {datetime.now().strftime('%H:%M:%S')}"
        buttons = [
            [InlineKeyboardButton("🗑️ Cleanup Preview", callback_data="cleanup_preview")],
            [InlineKeyboardButton("🔄 Refresh", callback_data="storage_info")],
            [InlineKeyboardButton("❌ Close", callback_data="cleanup_cancel")]
        ]
        await query.message.edit_text(
            storage_info,
            reply_markup=InlineKeyboardMarkup(buttons)
        )
    except Exception as e:
        logger.error(f"Storage info error: {str(e)}")
        await query.answer("Error getting storage info!")
@bot.on_callback_query(filters.regex("^cleanup_preview$"))
async def cleanup_preview_handler(client: Client, query: CallbackQuery):
    """Show preview of files that will be deleted"""
    try:
        if query.from_user.id not in SUDO_USERS:
            await query.answer("❌ Only sudo users can preview cleanup!", show_alert=True)
            return
        await query.message.edit_text("🔍 Scanning for cleanup candidates...")
        preview_info = await get_cleanup_preview()
        buttons = [
            [
                InlineKeyboardButton("✅ Clean Now", callback_data="cleanup_confirm_all"),
                InlineKeyboardButton("🔄 Rescan", callback_data="cleanup_preview")
            ],
            [
                InlineKeyboardButton("❌ Cancel", callback_data="cleanup_cancel")
            ]
        ]
        await query.message.edit_text(
            preview_info,
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        await query.answer("Preview generated!")
    except Exception as e:
        logger.error(f"Cleanup preview error: {str(e)}")
        await query.message.edit_text(f"❌ Preview failed: {str(e)}")
async def get_cleanup_preview():
    """Generate detailed preview of files that will be cleaned up - FIXED VERSION"""
    try:
        folders_to_scan = ["downloads", "encoded", "tempwork", "previews", "screenshots", "samples"]
        current_time = time.time()
        cleanup_candidates = []
        total_size = 0
        total_files = 0
        for folder in folders_to_scan:
            if not ospath.exists(folder):
                continue
            folder_size = 0
            folder_files = 0
            folder_file_list = []
            # Scan files in folder
            for item in os.listdir(folder):
                item_path = ospath.join(folder, item)
                try:
                    if ospath.isfile(item_path):
                        file_age = current_time - ospath.getmtime(item_path)
                        if file_age > 3600:  # 1 hour
                            file_size = ospath.getsize(item_path)
                            folder_size += file_size
                            folder_files += 1
                            folder_file_list.append({
                                'name': item,
                                'size': file_size,
                                'age': file_age
                            })
                    elif ospath.isdir(item_path):
                        dir_age = current_time - ospath.getmtime(item_path)
                        if dir_age > 3600:  # 1 hour
                            dir_size = 0
                            dir_file_count = 0
                            dir_files = []
                            for root, dirs, files in os.walk(item_path):
                                for file in files:
                                    try:
                                        file_path = ospath.join(root, file)
                                        file_size = ospath.getsize(file_path)
                                        dir_size += file_size
                                        dir_file_count += 1
                                        dir_files.append({
                                            'name': f"{item}/{file}",
                                            'size': file_size,
                                            'age': current_time - ospath.getmtime(file_path)
                                        })
                                    except Exception as e:
                                        continue
                            folder_size += dir_size
                            folder_files += dir_file_count
                            folder_file_list.extend(dir_files)
                except Exception as e:
                    continue
            if folder_files > 0:
                # Sort files by size (largest first) for display
                folder_file_list.sort(key=lambda x: x['size'], reverse=True)
                cleanup_candidates.append({
                    'folder': folder,
                    'files': folder_files,
                    'size': folder_size,
                    'file_list': folder_file_list[:10]  # Show top 10 largest files
                })
                total_size += folder_size
                total_files += folder_files
        if not cleanup_candidates:
            return "✅ **No files to clean up!**\n\nAll temporary files are within the 1-hour retention period."
        # Build detailed preview message
        preview_text = "🗑️ **Cleanup Preview** 🔍\n\n"
        preview_text += "📂 **Files older than 1 hour that will be deleted:**\n\n"
        for candidate in cleanup_candidates:
            preview_text += f"📁 **{candidate['folder'].upper()}:**\n"
            preview_text += f"   📊 Total: {candidate['files']} files, {humanize.naturalsize(candidate['size'])}\n"
            # Show top files in this folder
            if candidate['file_list']:
                preview_text += "   📄 **Largest files:**\n"
                for i, file_info in enumerate(candidate['file_list'][:5]):  # Show top 5 files per folder
                    file_name = file_info['name']
                    if len(file_name) > 30:
                        file_name = file_name[:27] + "..."
                    file_size = humanize.naturalsize(file_info['size'])
                    file_age = humanize.naturaltime(file_info['age']).replace('ago', 'old')
                    preview_text += f"      {i+1}. `{file_name}` - {file_size} ({file_age})\n"
            preview_text += "\n"
        preview_text += f"📈 **SUMMARY:**\n"
        preview_text += f"• 📂 Folders to clean: {len(cleanup_candidates)}\n"
        preview_text += f"• 📄 Total files: {total_files}\n"
        preview_text += f"• 💾 Total size: {humanize.naturalsize(total_size)}\n"
        preview_text += f"• ⏰ Age threshold: 1 hour+\n\n"
        preview_text += "⚠️ **This action cannot be undone!**"
        return preview_text
    except Exception as e:
        logger.error(f"Cleanup preview generation error: {str(e)}")
        return f"❌ Error generating preview: {str(e)}"
@bot.on_callback_query(filters.regex("^cleanup_refresh$"))
async def cleanup_refresh_handler(client: Client, query: CallbackQuery):
    """Refresh cleanup status"""
    try:
        if query.from_user.id not in SUDO_USERS:
            await query.answer("❌ Only sudo users can refresh cleanup!", show_alert=True)
            return
        await query.answer("Refreshing...")
        # Get current disk usage
        disk = psutil.disk_usage('/')
        preview_info = await get_cleanup_preview()
        status_text = (
            f"💾 **Current Disk Usage:**\n"
            f"• Total: {humanize.naturalsize(disk.total)}\n"
            f"• Used: {humanize.naturalsize(disk.used)} ({disk.percent}%)\n"
            f"• Free: {humanize.naturalsize(disk.free)}\n\n"
            f"{preview_info}"
        )
        buttons = [
            [
                InlineKeyboardButton("✅ Clean Now", callback_data="cleanup_confirm_all"),
                InlineKeyboardButton("🔄 Rescan", callback_data="cleanup_refresh")
            ],
            [
                InlineKeyboardButton("📊 System Info", callback_data="system_info"),
                InlineKeyboardButton("❌ Close", callback_data="cleanup_cancel")
            ]
        ]
        await query.message.edit_text(
            status_text,
            reply_markup=InlineKeyboardMarkup(buttons)
        )
    except Exception as e:
        await query.answer("Error refreshing!")
@bot.on_callback_query(filters.regex("^cleanup_cancel$"))
async def cleanup_cancel_handler(client: Client, query: CallbackQuery):
    """Cancel cleanup operation"""
    await query.message.delete()
    await query.answer("Cleanup cancelled")
async def cleanup_storage():
    """ACTUALLY clean storage - FIXED VERSION"""
    try:
        folders_to_clean = ["downloads", "encoded", "tempwork", "previews", "screenshots", "samples"]
        total_freed = 0
        deleted_files = 0
        current_time = time.time()
        cleanup_report = []
        for folder in folders_to_clean:
            if not ospath.exists(folder):
                continue
            folder_freed = 0
            folder_deleted_files = 0
            # Clean files in folder
            for item in os.listdir(folder):
                item_path = ospath.join(folder, item)
                try:
                    if ospath.isfile(item_path):
                        file_age = current_time - ospath.getmtime(item_path)
                        if file_age > 3600:  # 1 hour
                            file_size = ospath.getsize(item_path)
                            osremove(item_path)
                            folder_freed += file_size
                            folder_deleted_files += 1
                            logger.info(f"🧹 Deleted file: {item_path}")
                    elif ospath.isdir(item_path):
                        dir_age = current_time - ospath.getmtime(item_path)
                        if dir_age > 3600:  # 1 hour
                            # Calculate directory size before deletion
                            dir_size = 0
                            for root, dirs, files in os.walk(item_path):
                                for file in files:
                                    try:
                                        file_path = ospath.join(root, file)
                                        dir_size += ospath.getsize(file_path)
                                    except:
                                        continue
                            # Delete the directory
                            shutil.rmtree(item_path, ignore_errors=True)
                            folder_freed += dir_size
                            folder_deleted_files += 1  # Count directory as 1 item
                            logger.info(f"🧹 Deleted directory: {item_path}")
                except Exception as e:
                    logger.warning(f"Failed to delete {item_path}: {str(e)}")
                    continue
            if folder_freed > 0:
                cleanup_report.append({
                    'folder': folder,
                    'files': folder_deleted_files,
                    'size': folder_freed
                })
                total_freed += folder_freed
                deleted_files += folder_deleted_files
        # Log detailed cleanup report
        if cleanup_report:
            logger.info("🧹 Cleanup Report:")
            for report in cleanup_report:
                logger.info(f"  📁 {report['folder']}: {report['files']} items, {humanize.naturalsize(report['size'])}")
            logger.info(f"🧹 Total: {deleted_files} items, {humanize.naturalsize(total_freed)} freed")
        else:
            logger.info("🧹 No files needed cleanup")
        return deleted_files, total_freed
    except Exception as e:
        logger.error(f"Cleanup failed: {str(e)}")
        return 0, 0
@bot.on_message(filters.command("canceltask") & filters.private)
@sudo_only
async def cancel_task_by_id_handler(client: Client, message: Message):
    """Cancel specific task by task ID - Sudo only"""
    try:
        if len(message.command) < 2:
            await message.reply(
                "❌ **Usage:** `/canceltask <task_id>`\n\n"
                "💡 **Examples:**\n"
                "• `/canceltask abc123def`\n"
                "• `/canceltask 12345678`\n\n"
                "📋 **To get task IDs:**\n"
                "• Use `/queue` to see active tasks with IDs\n"
                "• Use `/status` for detailed task information"
            )
            return
        
        task_id = message.command[1].strip()
        
        # Find which user owns this task
        chat_id = task_id_to_user.get(task_id)
        
        if not chat_id:
            await message.reply(f"❌ Task ID `{task_id}` not found!")
            return
        
        # Get task info before cancellation
        task_info = active_tasks.get(chat_id, {})
        task_type = task_info.get('type', 'unknown')
        user_info = await get_user_display_info(chat_id)
        
        # Cancel the task
        success = await cancel_specific_task(chat_id, task_id)
        
        if success:
            await message.reply(
                f"✅ **Task Cancelled by Sudo**\n\n"
                f"🆔 **Task ID:** `{task_id}`\n"
                f"👤 **User:** {user_info}\n"
                f"📝 **Task Type:** {task_type}\n"
                f"👑 **Cancelled by:** {message.from_user.mention}\n"
                f"🕒 **Time:** {datetime.now().strftime('%H:%M:%S')}"
            )
            
            # Notify the user if possible
            try:
                await bot.send_message(
                    chat_id,
                    f"⚠️ **Your task was cancelled by admin**\n\n"
                    f"🆔 **Task ID:** `{task_id}`\n"
                    f"📝 **Task:** {task_type}\n"
                    f"👑 **Admin:** {message.from_user.mention}\n\n"
                    f"Your task has been stopped by administrator."
                )
            except Exception as e:
                logger.warning(f"Could not notify user {chat_id}: {str(e)}")
                
        else:
            await message.reply(f"❌ Failed to cancel task `{task_id}`")
            
    except Exception as e:
        logger.error(f"Cancel task by ID error: {str(e)}")
        await message.reply(f"❌ Error cancelling task: {str(e)}")

async def get_user_display_info(chat_id: int) -> str:
    """Get user information for display"""
    try:
        user = await bot.get_users(chat_id)
        return f"{user.first_name} (@{user.username})" if user.username else f"{user.first_name} ({chat_id})"
    except:
        return f"User {chat_id}"
@bot.on_callback_query(filters.regex(r"^confirm_cancel_task_(.+)$"))
async def confirm_cancel_task_handler(client: Client, query: CallbackQuery):
    """Handle confirmation of task cancellation by ID"""
    try:
        task_id = query.data.split("_")[3]
        
        # Verify user is sudo
        if query.from_user.id not in SUDO_USERS:
            await query.answer("❌ Only sudo users can cancel tasks by ID!", show_alert=True)
            return
        
        # Find and cancel the task
        cancelled = False
        target_chat_id = None
        task_info = None
        
        for chat_id, task_data in active_tasks.items():
            if task_data.get('task_id') == task_id:
                target_chat_id = chat_id
                task_info = task_data
                break
        
        if target_chat_id and task_info:
            # Cancel using the user's cancellation event
            user_event = await get_user_cancellation_event(target_chat_id)
            user_event.set()
            
            # Remove from active tasks
            async with task_lock:
                if target_chat_id in active_tasks:
                    del active_tasks[target_chat_id]
            
            # Try to cancel in queue manager if it's the current task
            if global_queue_manager.current_task == task_id:
                await global_queue_manager.cancel_current_task()
            
            cancelled = True
            
            # Notify the user
            try:
                await bot.send_message(
                    target_chat_id,
                    f"🛑 **Your Task Was Cancelled by Admin**\n\n"
                    f"📋 **Task ID:** `{task_id}`\n"
                    f"🔧 **Task Type:** {task_info.get('type', 'unknown')}\n"
                    f"👑 **Cancelled by:** {query.from_user.mention}\n\n"
                    f"Your task has been stopped by an administrator.\n"
                    f"You can start a new task whenever you're ready."
                )
            except Exception as notify_error:
                logger.warning(f"Could not notify user {target_chat_id}: {str(notify_error)}")
        
        if cancelled:
            await query.message.edit_text(
                f"✅ **Task Cancelled Successfully**\n\n"
                f"📋 **Task ID:** `{task_id}`\n"
                f"👤 **User ID:** `{target_chat_id}`\n"
                f"🔧 **Task Type:** {task_info.get('type', 'unknown')}\n"
                f"👑 **Cancelled by:** {query.from_user.mention}\n\n"
                f"🕒 **Time:** {datetime.now().strftime('%H:%M:%S')}"
            )
            logger.info(f"👑 Sudo user {query.from_user.id} cancelled task {task_id} for user {target_chat_id}")
        else:
            await query.message.edit_text(
                f"❌ **Task Not Found**\n\n"
                f"Task `{task_id}` was not found or may have already completed."
            )
        
        await query.answer("Task cancelled!")
        
    except Exception as e:
        logger.error(f"Confirm cancel task error: {str(e)}")
        await query.message.edit_text("❌ Error cancelling task!")
        await query.answer("Error!")
async def cancel_specific_task(chat_id: int, task_id: str) -> bool:
    """Cancel a specific task by task ID"""
    try:
        logger.info(f"🛑 Sudo cancelling task {task_id} for user {chat_id}")
        
        # Check if this is the current task
        if global_queue_manager.current_task == task_id:
            await global_queue_manager.cancel_current_task()
            logger.info(f"✅ Cancelled current task {task_id}")
            return True
        
        # Check if task is in active tasks
        if chat_id in active_tasks:
            current_task_info = active_tasks[chat_id]
            if current_task_info.get('task_id') == task_id:
                # Set user cancellation event
                user_event = await get_user_cancellation_event(chat_id)
                user_event.set()
                
                # Remove from active tasks
                async with task_lock:
                    if chat_id in active_tasks:
                        del active_tasks[chat_id]
                
                # Cleanup
                await cleanup_progress_messages(chat_id)
                await update_user_settings(chat_id, {"status": "idle", "cancelled": True})
                
                # Clear event after delay
                await asyncio.sleep(2)
                user_event.clear()
                
                logger.info(f"✅ Cancelled active task {task_id}")
                return True
        
        # Remove from task ID tracking
        if task_id in task_id_to_user:
            del task_id_to_user[task_id]
        
        logger.info(f"✅ Task {task_id} cancellation processed")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error cancelling specific task {task_id}: {str(e)}")
        return False
@bot.on_callback_query(filters.regex("^cancel_cancel_task$"))
async def cancel_cancel_task_handler(client: Client, query: CallbackQuery):
    """Cancel the task cancellation request"""
    await query.message.delete()
    await query.answer("Cancellation cancelled")
@bot.on_message(filters.command("help") & (filters.private | filters.group))
@authorized_only
async def help_handler(client: Client, message: Message):
    """Show help with all available commands - ENHANCED BLOCKQUOTE STYLE"""
    try:
        user_id = message.from_user.id
        is_sudo = user_id in SUDO_USERS
        is_premium = await is_premium_user(user_id)
        can_use_bot = await can_user_use_bot(user_id)
        
        help_text = (
            "🤖 <b>Video Encoder Bot - Complete Help Guide</b>\n\n"
            
            "<blockquote expandable>"
            "👤 <b>Basic Commands</b>\n"
            "• <code>/start</code> - Start the bot & main menu\n"
            "• <code>/help</code> - Show this help message\n"  
            "• <code>/status</code> - Check bot status & queue\n"
            "• <code>/premium_info</code> - Check premium status\n"
            "• <code>/queue</code> - View current queue status\n"
            "</blockquote>\n\n"
        )
        
        # Encoding commands section
        if can_use_bot:
            help_text += (
                "<blockquote expandable>"
                "🎬 <b>Encoding Commands</b>\n"
                "• <code>/magnet magnet_link</code> - Process torrent/magnet\n"
                "• <code>/compress</code> - Reply to video file to encode\n"
                "• <code>/cancel</code> - Cancel your current tasks\n"
                "• <code>/settings</code> - Show current settings\n"
                "• <code>/reset</code> - Reset settings to defaults\n"
                "</blockquote>\n\n"
            )
        else:
            help_text += (
                "<blockquote>"
                "🎬 <b>Encoding Commands</b> 🔒\n"
                "<i>Premium required for encoding features</i>\n"
                "Use <code>/premium_info</code> to check your status\n"
                "</blockquote>\n\n"
            )
        
        # Premium features section
        help_text += (
            "<blockquote expandable>"
            "💎 <b>Premium Benefits</b>\n"
           # "• Unlimited encoding access\n" 
            "• Priority queue position\n"
            "• All quality options available\n"
           # "• H.265/AV1 codec support\n"
            "• No restrictions or limits\n"
            "• Advanced features unlocked\n"
            "</blockquote>\n\n"
        )
        
        # Features overview
        help_text += (
            "<blockquote expandable>"
            "⚡ <b>Advanced Features</b>\n"
            "• All Qualities Mode (multiple versions)\n"
            "• Batch processing & torrent support\n"
            "• H.264, H.265, AV1 codec support\n"
            "• 8 video tune optimizations\n"
            "• Professional watermark system\n"
            "• Custom CRF quality tuning\n"
            "• Hard subtitle burning\n"
            "• Real-time progress tracking\n"
            "</blockquote>\n\n"
        )
        
        # Sudo commands section
        if is_sudo:
            help_text += (
                "<blockquote>"
                "👑 <b>Sudo Commands</b>\n"
                "• <code>/authorize on/off</code> - Toggle bot access\n"
                "• <code>/addsudo user_id</code> - Add sudo user\n"
                "• <code>/rmsudo user_id</code> - Remove sudo user\n"
                "• <code>/sudolist</code> - List sudo users\n"
                "• <code>/cleanup</code> - Cleanup storage\n"
                "• <code>/cancel_all</code> - Cancel all tasks\n"
                "• <code>/ban user_id</code> - Ban user\n"
                "• <code>/unban user_id</code> - Unban user\n"
                "</blockquote>\n\n"
            )
        
        # Contact information
        help_text += (
            "<blockquote>"
            "📞 <b>Contact & Support</b>\n"
            "• @SunDGodBot \n"
            "• @LuffyDSunGodBot\n\n"
            "🛠️ <b>Need Help?</b>\n"
            "Contact us for:\n"
            "• Encoding issues\n"
            "• Feature explanations\n" 
            "• Bug reports\n"
            "• Premium access\n"
            "</blockquote>\n\n"
        )
        
        # User status information
        status_info = "👤 <b>Your Status:</b> "
        if is_sudo:
            status_info += "👑 Sudo User"
        elif is_premium:
            status_info += "💎 Premium User" 
        elif can_use_bot:
            status_info += "✅ Basic Access"
        else:
            status_info += "🔒 Restricted (Premium Required)"
        
        help_text += f"{status_info}\n"
        
        await message.reply(
            help_text,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True
        )        
    except Exception as e:
        logger.error(f"Help command error: {str(e)}")
        await message.reply(
            "❌ Error showing help! Please contact @Rbotz for support.",
            parse_mode=ParseMode.HTML
        )
@bot.on_callback_query(filters.regex("^admin_panel$"))
async def admin_panel_handler(client: Client, query: CallbackQuery):
    """Enhanced admin panel with size limits management - FIXED VERSION"""
    try:
        user_id = query.from_user.id
        if user_id not in SUDO_USERS:
            await query.answer("❌ Admin access required!", show_alert=True)
            return
        # Get current system info
        cpu_usage = psutil.cpu_percent()
        memory_usage = psutil.virtual_memory().percent
        disk_usage = psutil.disk_usage('/').percent
        queue_status = global_queue_manager.get_queue_status()
        admin_text = (
            "👑 <b>Admin Control Panel</b>\n\n"
            "<blockquote>"
            f"🖥️ <b>System Status</b>\n"
            f"• CPU: {cpu_usage}%\n"
            f"• RAM: {memory_usage}%\n"
            f"• Disk: {disk_usage}%\n"
            f"• Tasks: {len(active_tasks)} active, {queue_status['queue_size']} queued\n\n"
            f"📏 <b>Size Limits</b>\n"
            f"• Torrent: {humanize.naturalsize(TORRENT_SIZE_LIMIT)}\n"
            f"• Single File: {humanize.naturalsize(SINGLE_FILE_SIZE_LIMIT)}\n\n"
            f"🔐 <b>Authorization</b>\n"
            f"• Mode: {'🔒 Sudo Only' if BOT_AUTHORIZED else '🔓 Public'}\n"
            f"• Sudo Users: {len(SUDO_USERS)}\n"
            "</blockquote>"
        )
        buttons = [
            [
                InlineKeyboardButton("📏 Size Limits", callback_data="size_limits_menu"),
                InlineKeyboardButton("👥 User Management", callback_data="user_management")
            ],
            [
                InlineKeyboardButton("🔄 System Control", callback_data="system_control"),
                InlineKeyboardButton("📊 Statistics", callback_data="admin_stats")
            ],
            [
                InlineKeyboardButton("🔐 Auth Settings", callback_data="auth_settings"),
                InlineKeyboardButton("🧹 Cleanup", callback_data="admin_cleanup")
            ],
            [
                InlineKeyboardButton("💎 Premium Management", callback_data="premium_management"),
                InlineKeyboardButton("🛑 Cancel All Tasks", callback_data="confirm_cancel_all_owner")
            ],
            [
                InlineKeyboardButton("🔙 Back to Start", callback_data="back_to_start"),
                InlineKeyboardButton("❌ Close", callback_data="close_menu")
            ]
        ]
        await query.message.edit_text(
            admin_text,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        await query.answer()
    except Exception as e:
        logger.error(f"Admin panel error: {str(e)}")
        await query.answer("Error loading admin panel!")
# Fix system control panel
@bot.on_callback_query(filters.regex("^system_control$"))
async def system_control_handler(client: Client, query: CallbackQuery):
    """System control panel - FIXED VERSION"""
    try:
        user_id = query.from_user.id
        if user_id not in SUDO_USERS:
            await query.answer("❌ Admin access required!", show_alert=True)
            return
        # Get system info
        cpu_usage = psutil.cpu_percent()
        memory_usage = psutil.virtual_memory().percent
        disk_usage = psutil.disk_usage('/').percent
        queue_status = global_queue_manager.get_queue_status()
        system_text = (
            "🔄 <b>System Control Panel</b>\n\n"
            "<blockquote>"
            f"🖥️ <b>System Status</b>\n"
            f"• CPU Usage: {cpu_usage}%\n"
            f"• Memory Usage: {memory_usage}%\n"
            f"• Disk Usage: {disk_usage}%\n"
            f"• Active Tasks: {len(active_tasks)}\n"
            f"• Queued Tasks: {queue_status['queue_size']}\n\n"
            "⚡ <b>System Controls:</b>\n"
            "</blockquote>"
        )
        buttons = [
            [
                InlineKeyboardButton("🛑 Stop All Tasks", callback_data="confirm_cancel_all_owner"),
                InlineKeyboardButton("🧹 Force Cleanup", callback_data="cleanup_confirm_all")
            ],
            [
                InlineKeyboardButton("📊 Queue Management", callback_data="queue_management"),
                InlineKeyboardButton("🔄 Restart Bot", callback_data="restart_bot_confirm")
            ],
            [
                InlineKeyboardButton("🔙 Back to Admin", callback_data="admin_panel"),
                InlineKeyboardButton("📊 Bot Status", callback_data="bot_status")
            ]
        ]
        await query.message.edit_text(
            system_text,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        await query.answer()
    except Exception as e:
        logger.error(f"System control error: {str(e)}")
        await query.answer("Error loading system control!")
@bot.on_callback_query(filters.regex("^size_limits_menu$"))
async def size_limits_menu_handler(client: Client, query: CallbackQuery):
    """Size limits management menu"""
    try:
        user_id = query.from_user.id
        if user_id not in SUDO_USERS:
            await query.answer("❌ Admin access required!", show_alert=True)
            return
        limits_text = (
            "📏 <b>Size Limits Management</b>\n\n"
            "<blockquote>"
            "Configure maximum allowed sizes for downloads:\n\n"
            f"• <b>Current Torrent Limit:</b> {humanize.naturalsize(TORRENT_SIZE_LIMIT)}\n"
            f"• <b>Current Single File Limit:</b> {humanize.naturalsize(SINGLE_FILE_SIZE_LIMIT)}\n\n"
            "💡 <b>Recommended Limits:</b>\n"
            "• Torrent: 1-10GB depending on server capacity\n"
            "• Single File: 2-8GB for processing efficiency\n"
            "</blockquote>"
        )
        buttons = [
            [
                InlineKeyboardButton("🔧 Set Torrent Limit", callback_data="set_torrent_limit"),
                InlineKeyboardButton("🔧 Set File Limit", callback_data="set_file_limit")
            ],
            [
                InlineKeyboardButton("🔙 Back to Admin", callback_data="admin_panel"),
                InlineKeyboardButton("❌ Close", callback_data="close_menu")
            ]
        ]
        await query.message.edit_text(
            limits_text,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        await query.answer()
    except Exception as e:
        logger.error(f"Size limits menu error: {str(e)}")
        await query.answer("Error loading size limits menu!")
@bot.on_callback_query(filters.regex("^set_torrent_limit$"))
async def set_torrent_limit_handler(client: Client, query: CallbackQuery):
    """Set torrent size limit"""
    try:
        user_id = query.from_user.id
        if user_id not in SUDO_USERS:
            await query.answer("❌ Admin access required!", show_alert=True)
            return
        limit_text = (
            "🔧 <b>Set Torrent Size Limit</b>\n\n"
            "<blockquote>"
            "Enter new torrent size limit in GB (1-100):\n\n"
            f"<b>Current Limit:</b> {humanize.naturalsize(TORRENT_SIZE_LIMIT)}\n\n"
            "💡 <b>Examples:</b>\n"
            "• <code>5</code> for 5GB\n"
            "• <code>10</code> for 10GB\n"
            "• <code>25</code> for 25GB\n"
            "</blockquote>"
        )
        buttons = [
            [InlineKeyboardButton("🔙 Back to Limits", callback_data="size_limits_menu")],
            [InlineKeyboardButton("❌ Cancel", callback_data="admin_panel")]
        ]
        # Store state for input handling
        await update_user_settings(user_id, {"awaiting": "torrent_limit"})
        await query.message.edit_text(
            limit_text,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        await query.answer()
    except Exception as e:
        logger.error(f"Set torrent limit error: {str(e)}")
        await query.answer("Error setting torrent limit!")
@bot.on_callback_query(filters.regex("^set_file_limit$"))
async def set_file_limit_handler(client: Client, query: CallbackQuery):
    """Set single file size limit"""
    try:
        user_id = query.from_user.id
        if user_id not in SUDO_USERS:
            await query.answer("❌ Admin access required!", show_alert=True)
            return
        limit_text = (
            "🔧 <b>Set Single File Size Limit</b>\n\n"
            "<blockquote>"
            "Enter new single file size limit in GB (1-50):\n\n"
            f"<b>Current Limit:</b> {humanize.naturalsize(SINGLE_FILE_SIZE_LIMIT)}\n\n"
            "💡 <b>Examples:</b>\n"
            "• <code>2</code> for 2GB\n"
            "• <code>4</code> for 4GB\n"
            "• <code>8</code> for 8GB\n"
            "</blockquote>"
        )
        buttons = [
            [InlineKeyboardButton("🔙 Back to Limits", callback_data="size_limits_menu")],
            [InlineKeyboardButton("❌ Cancel", callback_data="admin_panel")]
        ]
        # Store state for input handling
        await update_user_settings(user_id, {"awaiting": "file_limit"})
        await query.message.edit_text(
            limit_text,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        await query.answer()
    except Exception as e:
        logger.error(f"Set file limit error: {str(e)}")
        await query.answer("Error setting file limit!")
@bot.on_callback_query(filters.regex("^user_management$"))
async def user_management_handler(client: Client, query: CallbackQuery):
    """User management panel"""
    try:
        user_id = query.from_user.id
        if user_id not in SUDO_USERS:
            await query.answer("❌ Admin access required!", show_alert=True)
            return
        user_text = (
            "👥 <b>User Management</b>\n\n"
            "<blockquote>"
            f"• Total Sudo Users: {len(SUDO_USERS)}\n"
            f"• Current Active Users: {len(active_tasks)}\n"
            f"• Authorization Mode: {'🔒 Restricted' if BOT_AUTHORIZED else '🔓 Public'}\n\n"
            "Manage user access and permissions:\n"
            "</blockquote>"
        )
        buttons = [
            [
                InlineKeyboardButton("➕ Add Sudo User", callback_data="add_sudo_user"),
                InlineKeyboardButton("➖ Remove Sudo User", callback_data="remove_sudo_user")
            ],
            [
                InlineKeyboardButton("📋 List Sudo Users", callback_data="list_sudo_users"),
                InlineKeyboardButton("🔄 Toggle Auth", callback_data="toggle_auth")
            ],
            [
                InlineKeyboardButton("🔙 Back to Admin", callback_data="admin_panel"),
                InlineKeyboardButton("❌ Close", callback_data="close_menu")
            ]
        ]
        await query.message.edit_text(
            user_text,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        await query.answer()
    except Exception as e:
        logger.error(f"User management error: {str(e)}")
        await query.answer("Error loading user management!")
# Add restart bot confirmation
@bot.on_callback_query(filters.regex("^restart_bot_confirm$"))
async def restart_bot_confirm_handler(client: Client, query: CallbackQuery):
    """Confirm bot restart"""
    try:
        if query.from_user.id not in SUDO_USERS:
            await query.answer("❌ Admin access required!", show_alert=True)
            return
        buttons = [
            [
                InlineKeyboardButton("✅ Yes, Restart Bot", callback_data="restart_bot_now"),
                InlineKeyboardButton("❌ No, Cancel", callback_data="system_control")
            ]
        ]
        await query.message.edit_text(
            "🔄 <b>Restart Bot Confirmation</b>\n\n"
            "<blockquote>"
            "Are you sure you want to restart the bot?\n\n"
            "⚠️ <b>This will:</b>\n"
            "• Stop all current tasks\n"
            "• Clear the task queue\n"
            "• Temporarily make bot unavailable\n"
            "• Restart all services\n\n"
            "The bot will be back online in about 10-30 seconds."
            "</blockquote>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        await query.answer()
    except Exception as e:
        logger.error(f"Restart confirmation error: {str(e)}")
        await query.answer("Error!")
@bot.on_callback_query(filters.regex("^restart_bot_now$"))
async def restart_bot_now_handler(client: Client, query: CallbackQuery):
    """Restart the bot"""
    try:
        if query.from_user.id not in SUDO_USERS:
            await query.answer("❌ Admin access required!", show_alert=True)
            return
        await query.message.edit_text("🔄 <b>Restarting Bot...</b>\n\nPlease wait 10-30 seconds...", parse_mode=ParseMode.HTML)
        # Save settings before restart
        if db is not None:
            await save_sudo_users()
            await save_bot_settings()
            await save_size_limits()
        logger.info(f"👑 Bot restart initiated by {query.from_user.id}")
        # Restart the bot
        import sys
        import os
        os.execv(sys.executable, [sys.executable] + sys.argv)
    except Exception as e:
        logger.error(f"Restart error: {str(e)}")
        await query.message.edit_text("❌ Failed to restart bot!")
@bot.on_callback_query(filters.regex("^admin_stats$"))
async def admin_stats_handler(client: Client, query: CallbackQuery):
    """Admin statistics panel"""
    try:
        user_id = query.from_user.id
        if user_id not in SUDO_USERS:
            await query.answer("❌ Admin access required!", show_alert=True)
            return
        # Get statistics (you might want to store these in DB)
        stats_text = (
            "📊 <b>Admin Statistics</b>\n\n"
            "<blockquote>"
            "📈 <b>Usage Statistics</b>\n"
            "• Total Tasks Processed: Coming soon\n"
            "• Successful Encodings: Coming soon\n"
            "• Failed Tasks: Coming soon\n"
            "• Average Processing Time: Coming soon\n\n"
            "💾 <b>Storage Statistics</b>\n"
            "• Total Files Processed: Coming soon\n"
            "• Total Data Processed: Coming soon\n"
            "• Cleanup Efficiency: Coming soon\n"
            "</blockquote>"
            "💡 <i>Statistics tracking will be implemented in future update</i>"
        )
        buttons = [
            [InlineKeyboardButton("🔄 Refresh Stats", callback_data="admin_stats")],
            [InlineKeyboardButton("🔙 Back to Admin", callback_data="admin_panel")],
            [InlineKeyboardButton("❌ Close", callback_data="close_menu")]
        ]
        await query.message.edit_text(
            stats_text,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        await query.answer()
    except Exception as e:
        logger.error(f"Admin stats error: {str(e)}")
        await query.answer("Error loading statistics!")
@bot.on_callback_query(filters.regex("^auth_settings$"))
async def auth_settings_handler(client: Client, query: CallbackQuery):
    """Authorization settings panel"""
    try:
        user_id = query.from_user.id
        if user_id not in SUDO_USERS:
            await query.answer("❌ Admin access required!", show_alert=True)
            return
        auth_text = (
            "🔐 <b>Authorization Settings</b>\n\n"
            "<blockquote>"
            f"• Current Mode: {'🔒 RESTRICTED (Sudo only)' if BOT_AUTHORIZED else '🔓 PUBLIC (All users)'}\n"
            f"• Sudo Users Count: {len(SUDO_USERS)}\n"
            f"• Bot Owner: {BOT_OWNER_ID}\n\n"
            "Manage bot access permissions:\n"
            "</blockquote>"
        )
        buttons = [
            [
                InlineKeyboardButton("🔒 Enable Restricted", callback_data="enable_restricted"),
                InlineKeyboardButton("🔓 Enable Public", callback_data="enable_public")
            ],
            [
                InlineKeyboardButton("👥 Manage Sudo Users", callback_data="user_management"),
                InlineKeyboardButton("📋 View Sudo List", callback_data="list_sudo_users")
            ],
            [
                InlineKeyboardButton("🔙 Back to Admin", callback_data="admin_panel"),
                InlineKeyboardButton("❌ Close", callback_data="close_menu")
            ]
        ]
        await query.message.edit_text(
            auth_text,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        await query.answer()
    except Exception as e:
        logger.error(f"Auth settings error: {str(e)}")
        await query.answer("Error loading auth settings!")
@bot.on_callback_query(filters.regex("^admin_cleanup$"))
async def admin_cleanup_handler(client: Client, query: CallbackQuery):
    """Admin cleanup panel"""
    try:
        user_id = query.from_user.id
        if user_id not in SUDO_USERS:
            await query.answer("❌ Admin access required!", show_alert=True)
            return
        cleanup_text = (
            "🧹 <b>Admin Cleanup Tools</b>\n\n"
            "<blockquote>"
            "Advanced cleanup and maintenance tools:\n\n"
            "• <b>Storage Cleanup:</b> Remove temporary files\n"
            "• <b>Cache Cleanup:</b> Clear generated files\n"
            "• <b>DB Maintenance:</b> Optimize database\n"
            "• <b>Log Rotation:</b> Manage log files\n"
            "</blockquote>"
        )
        buttons = [
            [
                InlineKeyboardButton("💾 Storage Cleanup", callback_data="storage_cleanup"),
                InlineKeyboardButton("🗑️ Cache Cleanup", callback_data="cache_cleanup")
            ],
            [
                InlineKeyboardButton("📊 DB Maintenance", callback_data="db_maintenance"),
                InlineKeyboardButton("📋 Log Management", callback_data="log_management")
            ],
            [
                InlineKeyboardButton("🔙 Back to Admin", callback_data="admin_panel"),
                InlineKeyboardButton("❌ Close", callback_data="close_menu")
            ]
        ]
        await query.message.edit_text(
            cleanup_text,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        await query.answer()
    except Exception as e:
        logger.error(f"Admin cleanup error: {str(e)}")
        await query.answer("Error loading cleanup tools!")
@bot.on_callback_query(filters.regex("^enable_restricted$"))
async def enable_restricted_handler(client: Client, query: CallbackQuery):
    """Enable restricted mode"""
    try:
        user_id = query.from_user.id
        if user_id not in SUDO_USERS:
            await query.answer("❌ Admin access required!", show_alert=True)
            return
        global BOT_AUTHORIZED
        BOT_AUTHORIZED = True
        await save_bot_settings()
        await query.answer("✅ Restricted mode enabled - Only sudo users can use the bot")
        await auth_settings_handler(client, query)
    except Exception as e:
        logger.error(f"Enable restricted error: {str(e)}")
        await query.answer("Error enabling restricted mode!")
@bot.on_callback_query(filters.regex("^enable_public$"))
async def enable_public_handler(client: Client, query: CallbackQuery):
    """Enable public mode"""
    try:
        user_id = query.from_user.id
        if user_id not in SUDO_USERS:
            await query.answer("❌ Admin access required!", show_alert=True)
            return
        global BOT_AUTHORIZED
        BOT_AUTHORIZED = False
        await save_bot_settings()
        await query.answer("✅ Public mode enabled - All users can use the bot")
        await auth_settings_handler(client, query)
    except Exception as e:
        logger.error(f"Enable public error: {str(e)}")
        await query.answer("Error enabling public mode!")
@bot.on_callback_query(filters.regex("^add_sudo_user$"))
async def add_sudo_user_handler(client: Client, query: CallbackQuery):
    """Add sudo user interface"""
    try:
        user_id = query.from_user.id
        if user_id not in SUDO_USERS:
            await query.answer("❌ Admin access required!", show_alert=True)
            return
        add_text = (
            "➕ <b>Add Sudo User</b>\n\n"
            "<blockquote>"
            "To add a sudo user, use the command:\n"
            "```\n"
            "/addsudo USER_ID\n"
            "```\n\n"
            "Or send the user ID now:\n"
            "</blockquote>"
        )
        buttons = [
            [InlineKeyboardButton("🔙 Back to User Management", callback_data="user_management")],
            [InlineKeyboardButton("❌ Cancel", callback_data="admin_panel")]
        ]
        # Store state for input handling
        await update_user_settings(user_id, {"awaiting": "add_sudo"})
        await query.message.edit_text(
            add_text,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        await query.answer()
    except Exception as e:
        logger.error(f"Add sudo user error: {str(e)}")
        await query.answer("Error loading add sudo interface!")
@bot.on_callback_query(filters.regex("^remove_sudo_user$"))
async def remove_sudo_user_handler(client: Client, query: CallbackQuery):
    """Remove sudo user interface"""
    try:
        user_id = query.from_user.id
        if user_id not in SUDO_USERS:
            await query.answer("❌ Admin access required!", show_alert=True)
            return
        remove_text = (
            "➖ <b>Remove Sudo User</b>\n\n"
            "<blockquote>"
            "To remove a sudo user, use the command:\n"
            "```\n"
            "/rmsudo USER_ID\n"
            "```\n\n"
            "Or send the user ID now:\n"
            "</blockquote>"
        )
        buttons = [
            [InlineKeyboardButton("🔙 Back to User Management", callback_data="user_management")],
            [InlineKeyboardButton("❌ Cancel", callback_data="admin_panel")]
        ]
        # Store state for input handling
        await update_user_settings(user_id, {"awaiting": "remove_sudo"})
        await query.message.edit_text(
            remove_text,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        await query.answer()
    except Exception as e:
        logger.error(f"Remove sudo user error: {str(e)}")
        await query.answer("Error loading remove sudo interface!")
@bot.on_callback_query(filters.regex("^list_sudo_users$"))
async def list_sudo_users_handler(client: Client, query: CallbackQuery):
    """List all sudo users"""
    try:
        user_id = query.from_user.id
        if user_id not in SUDO_USERS:
            await query.answer("❌ Admin access required!", show_alert=True)
            return
        sudo_list = "\n".join([f"• `{user_id}`" for user_id in SUDO_USERS])
        list_text = (
            "📋 <b>Sudo Users List</b>\n\n"
            "<blockquote>"
            f"Total Sudo Users: {len(SUDO_USERS)}\n\n"
            f"{sudo_list}\n\n"
            "👑 <b>Bot Owner:</b> \n"
            f"• `{BOT_OWNER_ID}`\n"
            "</blockquote>"
        )
        buttons = [
            [InlineKeyboardButton("🔄 Refresh List", callback_data="list_sudo_users")],
            [InlineKeyboardButton("🔙 Back to User Management", callback_data="user_management")],
            [InlineKeyboardButton("❌ Close", callback_data="admin_panel")]
        ]
        await query.message.edit_text(
            list_text,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        await query.answer()
    except Exception as e:
        logger.error(f"List sudo users error: {str(e)}")
        await query.answer("Error loading sudo users list!")
@bot.on_callback_query(filters.regex("^bot_status$"))
async def bot_status_handler(client: Client, query: CallbackQuery):
    """Comprehensive bot status information with user count"""
    try:
        # Get system information
        cpu_usage = psutil.cpu_percent()
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        boot_time = datetime.fromtimestamp(psutil.boot_time())
        uptime = datetime.now() - boot_time
        # Get bot information
        queue_status = global_queue_manager.get_queue_status()
        bot_info = await bot.get_me()
        # Get user statistics
        total_users = await get_total_users_count()
        active_users = len(active_tasks)
        premium_users = len([uid for uid in PREMIUM_USERS if await is_premium_user(uid)])
        sudo_users = len(SUDO_USERS)
        status_text = (
            "📊 <b>Bot Status Overview</b>\n\n"
            "<blockquote>"
            "🤖 <b>Bot Information</b>\n"
            f"• Username: @{bot_info.username}\n"
            f"• ID: {bot_info.id}\n"
            f"• Uptime: {get_bot_uptime()}\n\n"
            "👥 <b>User Statistics</b>\n"
            f"• Total Users: {total_users}\n"
            f"• Active Now: {active_users}\n"
            f"• Premium Users: {premium_users}\n"
            f"• Sudo Users: {sudo_users}\n\n"
            "⚡ <b>System Status</b>\n"
            f"• CPU Usage: {cpu_usage}%\n"
            f"• Memory: {memory.percent}% ({humanize.naturalsize(memory.used)}/{humanize.naturalsize(memory.total)})\n"
            f"• Disk: {disk.percent}% ({humanize.naturalsize(disk.used)}/{humanize.naturalsize(disk.total)})\n"
            f"• System Uptime: {str(uptime).split('.')[0]}\n\n"
            "🔄 <b>Task Status</b>\n"
            f"• Active Tasks: {len(active_tasks)}\n"
            f"• Queued Tasks: {queue_status['queue_size']}\n"
            f"• Current Task: {queue_status['current_task'] or 'None'}\n\n"
            "🔐 <b>Authorization Status</b>\n"
            f"• Mode: {'🔒 RESTRICTED' if BOT_AUTHORIZED else '🔓 PUBLIC'}\n"
            f"• Size Limits: {humanize.naturalsize(TORRENT_SIZE_LIMIT)} torrent, "
            f"{humanize.naturalsize(SINGLE_FILE_SIZE_LIMIT)} file\n"
            "</blockquote>"
        )
        buttons = [
            [InlineKeyboardButton("🔄 Refresh Status", callback_data="bot_status")],
            [InlineKeyboardButton("📢 Broadcast", callback_data="broadcast_menu")],
            [InlineKeyboardButton("👑 Admin Panel", callback_data="admin_panel")],
            [InlineKeyboardButton("🔙 Back to Start", callback_data="back_to_start")],
            [InlineKeyboardButton("❌ Close", callback_data="close_menu")]
        ]
        await query.message.edit_text(
            status_text,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        await query.answer()
    except Exception as e:
        logger.error(f"Bot status error: {str(e)}")
        await query.answer("Error loading bot status!")
async def get_total_users_count() -> int:
    """Get total number of unique users who have interacted with the bot"""
    try:
        # Combine users from sessions, active tasks, and MongoDB if available
        all_users = set()
        # Add users from sessions
        for chat_id in session_manager.sessions.keys():
            all_users.add(chat_id)
        # Add users from active tasks
        for chat_id in active_tasks.keys():
            all_users.add(chat_id)
        # Add users from MongoDB collections if available
        if user_settings is not None:
            try:
                # Get unique users from user_settings collection
                pipeline = [
                    {"$group": {"_id": "$chat_id"}},
                    {"$count": "total_users"}
                ]
                result = await user_settings.aggregate(pipeline).to_list(length=1)
                if result and 'total_users' in result[0]:
                    # We can't get individual IDs from aggregation, so we'll use the count
                    # and assume some overlap with existing users
                    mongo_count = result[0]['total_users']
                    # Add some estimated unique users from MongoDB
                    estimated_mongo_users = max(0, mongo_count - len(all_users))
                    return len(all_users) + estimated_mongo_users
            except Exception as e:
                logger.warning(f"Could not get user count from MongoDB: {str(e)}")
        return len(all_users)
    except Exception as e:
        logger.error(f"Error getting total users count: {str(e)}")
        # Fallback to session count
        return len(session_manager.sessions)
# Add broadcast menu handler
@bot.on_callback_query(filters.regex("^broadcast_menu$"))
async def broadcast_menu_handler(client: Client, query: CallbackQuery):
    """Broadcast menu from status page"""
    try:
        user_id = query.from_user.id
        if user_id not in SUDO_USERS:
            await query.answer("❌ Only sudo users can send broadcasts!", show_alert=True)
            return
        total_users = await get_total_users_count()
        await query.message.edit_text(
            f"📢 **Broadcast Message**\n\n"
            f"👥 Total users: {total_users}\n\n"
            f"To send a broadcast message, use the command:\n"
            f"`/broadcast your message here`\n\n"
            f"💡 **Tips:**\n"
            f"• Keep messages clear and concise\n"
            f"• Use for important announcements\n"
            f"• Avoid spamming users\n\n"
            f"⚠️ **This will send to all {total_users} users!**",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Back to Status", callback_data="bot_status")],
                [InlineKeyboardButton("❌ Close", callback_data="close_menu")]
            ])
        )
        await query.answer()
    except Exception as e:
        logger.error(f"Broadcast menu error: {str(e)}")
        await query.answer("Error loading broadcast menu!")
@bot.on_callback_query(filters.regex("^queue_management$"))
async def queue_management_handler(client: Client, query: CallbackQuery):
    """Queue management panel"""
    try:
        user_id = query.from_user.id
        if user_id not in SUDO_USERS:
            await query.answer("❌ Admin access required!", show_alert=True)
            return
        queue_status = global_queue_manager.get_queue_status()
        # Get active tasks details
        active_tasks_info = ""
        if active_tasks:
            active_tasks_info = "\n<b>Active Tasks:</b>\n"
            for chat_id, task_info in list(active_tasks.items())[:5]:  # Show first 5
                task_type = task_info.get('type', 'unknown')
                status = task_info.get('status', 'running')
                start_time = task_info.get('start_time', time.time())
                elapsed = time.time() - start_time
                elapsed_str = f"{int(elapsed // 60)}m {int(elapsed % 60)}s"
                active_tasks_info += f"• User {chat_id}: {task_type} ({elapsed_str})\n"
            if len(active_tasks) > 5:
                active_tasks_info += f"• ... and {len(active_tasks) - 5} more\n"
        else:
            active_tasks_info = "\n<b>Active Tasks:</b> None\n"
        queue_text = (
            "📋 <b>Queue Management</b>\n\n"
            "<blockquote>"
            f"• Current Task: {queue_status['current_task'] or 'None'}\n"
            f"• Queue Size: {queue_status['queue_size']} tasks\n"
            f"• Active Tasks: {len(active_tasks)}\n"
            f"{active_tasks_info}"
            "</blockquote>"
        )
        buttons = [
            [
                InlineKeyboardButton("🛑 Stop All", callback_data="stop_all_tasks"),
                InlineKeyboardButton("🧹 Clear Queue", callback_data="clear_queue_confirm")
            ],
            [
                InlineKeyboardButton("🔄 Refresh", callback_data="queue_management"),
                InlineKeyboardButton("📊 Full Queue", callback_data="refresh_queue")
            ],
            [
                InlineKeyboardButton("🔙 Back to System", callback_data="system_control"),
                InlineKeyboardButton("❌ Close", callback_data="close_menu")
            ]
        ]
        await query.message.edit_text(
            queue_text,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        await query.answer()
    except Exception as e:
        logger.error(f"Queue management error: {str(e)}")
        await query.answer("Error loading queue management!")
@bot.on_callback_query(filters.regex("^admin_cleanup$"))
async def admin_cleanup_handler(client: Client, query: CallbackQuery):
    """Admin cleanup panel"""
    try:
        user_id = query.from_user.id
        if user_id not in SUDO_USERS:
            await query.answer("❌ Admin access required!", show_alert=True)
            return
        await query.answer("Opening cleanup tools...")
        await cleanup_handler(client, query.message)
    except Exception as e:
        logger.error(f"Admin cleanup error: {str(e)}")
        await query.answer("Error loading cleanup tools!")
@bot.on_callback_query(filters.regex("^close_menu$"))
async def close_menu_handler(client: Client, query: CallbackQuery):
    """Close any menu"""
    try:
        await query.message.delete()
        await query.answer("Menu closed")
    except Exception as e:
        await query.answer("Error closing menu")
# Add safe image sending function
async def safe_send_photo(chat_id, photo_url, caption, reply_markup=None):
    """Safely send photo with fallback to text"""
    try:
        return await bot.send_photo(
            chat_id=chat_id,
            photo=photo_url,
            caption=caption,
            parse_mode=ParseMode.HTML,
            reply_markup=reply_markup
        )
    except Exception as e:
        logger.warning(f"Failed to send photo {photo_url}, falling back to text: {str(e)}")
        return await bot.send_message(
            chat_id=chat_id,
            text=caption,
            parse_mode=ParseMode.HTML,
            reply_markup=reply_markup
        )
async def cancel_user_tasks(chat_id: int) -> bool:
    """Cancel all tasks for a specific user - COMPLETELY FIXED VERSION"""
    try:
        logger.info(f"🛑 Cancelling tasks for user {chat_id}")
        # Set user-specific cancellation event
        user_event = await get_user_cancellation_event(chat_id)
        user_event.set()
        # Cancel current task if it belongs to this user
        current_task_chat_id = None
        async with task_lock:
            for task_chat_id, task_info in active_tasks.items():
                if task_chat_id == chat_id:
                    current_task_chat_id = task_chat_id
                    break
        # If current task belongs to this user, cancel it
        if current_task_chat_id and global_queue_manager.current_task:
            await global_queue_manager.cancel_current_task()
        # Remove user from active tasks
        async with task_lock:
            if chat_id in active_tasks:
                del active_tasks[chat_id]
        # Remove user's tasks from queue
        await remove_user_from_queue(chat_id)
        # Cleanup progress messages
        await cleanup_progress_messages(chat_id)
        # Reset user session status
        await update_user_settings(chat_id, {
            "status": "idle",
            "cancelled": True
        })
        # Clear the cancellation event after a short delay
        await asyncio.sleep(2)
        user_event.clear()
        logger.info(f"✅ Successfully cancelled all tasks for user {chat_id}")
        return True
    except Exception as e:
        logger.error(f"❌ Error cancelling user tasks for {chat_id}: {str(e)}")
        return False
async def remove_user_from_queue(chat_id: int) -> int:
    """Remove user's tasks from global queue - FIXED VERSION"""
    try:
        # Since we can't directly modify the queue, we'll create a new one
        temp_queue = asyncio.Queue()
        removed_count = 0
        # Process all items in current queue
        while not global_queue_manager.queue.empty():
            try:
                # Get task from queue
                task_data = global_queue_manager.queue.get_nowait()
                if task_data and len(task_data) >= 3:  # (task_id, task_func, args, kwargs)
                    args = task_data[2]  # arguments tuple
                    task_chat_id = args[0] if args and len(args) > 0 else None
                    # Keep only tasks that don't belong to this user
                    if task_chat_id != chat_id:
                        await temp_queue.put(task_data)
                        logger.debug(f"✅ Kept task for user {task_chat_id}")
                    else:
                        removed_count += 1
                        logger.info(f"🗑️ Removed queued task for user {chat_id}")
                global_queue_manager.queue.task_done()
            except asyncio.QueueEmpty:
                break
            except Exception as e:
                logger.warning(f"Error processing queue item: {str(e)}")
                continue
        # Replace the old queue with the new one
        global_queue_manager.queue = temp_queue
        if removed_count > 0:
            logger.info(f"✅ Removed {removed_count} queued tasks for user {chat_id}")
        return removed_count
    except Exception as e:
        logger.error(f"❌ Error removing user from queue: {str(e)}")
        return 0
@bot.on_message(filters.command("cancel"))
@authorized_only
async def cancel_handler(client: Client, message: Message):
    """Cancel command - User can cancel their own tasks only - FIXED VERSION"""
    try:
        user_id = message.from_user.id
        chat_id = message.chat.id
        logger.info(f"🛑 CANCEL command from user: {user_id}")
        # 🎯 FIX: First clear any awaiting state to prevent input conflicts
        session = await get_user_settings(chat_id)
        if session and "awaiting" in session:
            await update_user_settings(chat_id, {"awaiting": None})
            await message.reply("✅ Input state cleared.")
            # Don't return here - continue to check for tasks to cancel
        # Check if user has any active or queued tasks
        has_active_tasks = chat_id in active_tasks
        has_queued_tasks = await check_user_in_queue(chat_id)
        if not has_active_tasks and not has_queued_tasks:
            await message.reply("ℹ️ You don't have any active or queued tasks to cancel.")
            return
        # Show cancellation confirmation with task details
        task_details = await get_user_task_details(chat_id)
        cancel_msg = await message.reply(
            f"🛑 **Cancel Your Tasks**\n\n"
            f"{task_details}\n\n"
            "Are you sure you want to cancel ALL your tasks?\n\n"
            "This will stop:\n"
            "• Current downloads/encoding\n" 
            "• All queued tasks\n"
            "• Uploads in progress\n\n"
            "**This action cannot be undone!**",
            reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("✅ Yes, Cancel My Tasks", callback_data=f"confirm_cancel_my_{chat_id}"),
                    InlineKeyboardButton("❌ No, Keep Running", callback_data="cancel_cancel")
                ]
            ])
        )
    except Exception as e:
        logger.error(f"Cancel command error: {str(e)}")
        await message.reply(f"❌ Error processing cancel request: {str(e)}")
# Update the existing check_user_in_queue function
async def check_user_in_queue(chat_id: int) -> bool:
    """Check if user has tasks in enhanced queue"""
    try:
        queue_status = await global_queue_manager.get_queue_status_with_estimates()
        # Check in queue list
        for task in queue_status['queue_list']:
            if task['chat_id'] == chat_id:
                return True
        # Check in active tasks
        return chat_id in active_tasks
    except Exception as e:
        logger.error(f"Error checking user in enhanced queue: {str(e)}")
        return False
async def get_user_task_details(chat_id: int) -> str:
    """Get details of user's active and queued tasks"""
    details = []
    # Active tasks
    if chat_id in active_tasks:
        task_info = active_tasks[chat_id]
        task_type = task_info.get('type', 'unknown')
        status = task_info.get('status', 'running')
        details.append(f"• **Active:** {task_type} ({status})")
    # Queued tasks
    queued_count = await count_user_queued_tasks(chat_id)
    if queued_count > 0:
        details.append(f"• **Queued:** {queued_count} task(s) waiting")
    if details:
        return "📊 **Your Tasks:**\n" + "\n".join(details)
    else:
        return "📊 **Your Tasks:** None"
@bot.on_callback_query(filters.regex(r"^confirm_cancel_my_(\d+)$"))
async def confirm_cancel_my_handler(client: Client, query: CallbackQuery):
    """Handle confirmation of cancel user's own tasks - FIXED VERSION"""
    try:
        chat_id = int(query.data.split("_")[3])
        # Verify it's the same user
        if query.from_user.id != chat_id:
            await query.answer("❌ You can only cancel your own tasks!", show_alert=True)
            return
        await query.answer("🛑 Cancelling your tasks...")
        # Get task counts before cancellation
        active_before = chat_id in active_tasks
        queued_before = await count_user_queued_tasks(chat_id)
        # Cancel user's tasks
        success = await cancel_user_tasks(chat_id)
        if success:
            # Get task counts after cancellation
            active_after = chat_id in active_tasks
            queued_after = await count_user_queued_tasks(chat_id)
            result_text = "✅ **Your Tasks Cancelled Successfully!**\n\n"
            if active_before and not active_after:
                result_text += "• Stopped active task\n"
            if queued_before > queued_after:
                result_text += f"• Removed {queued_before - queued_after} queued task(s)\n"
            if not active_before and queued_before == 0:
                result_text = "❌ No tasks were found to cancel. They may have already completed."
            else:
                result_text += "\nYou can start new tasks whenever you're ready."
            await query.message.edit_text(result_text)
            logger.info(f"✅ User {chat_id} cancelled their tasks successfully")
        else:
            await query.message.edit_text("❌ Failed to cancel tasks. They may have already completed.")
    except Exception as e:
        logger.error(f"Confirm cancel my error: {str(e)}")
        await query.message.edit_text("❌ Error cancelling tasks!")
        await query.answer("Error!")
# Update the existing count_user_queued_tasks function
async def count_user_queued_tasks(chat_id: int) -> int:
    """Count how many tasks a user has in enhanced queue"""
    try:
        queue_status = await global_queue_manager.get_queue_status_with_estimates()
        count = 0
        for task in queue_status['queue_list']:
            if task['chat_id'] == chat_id:
                count += 1
        return count
    except Exception as e:
        logger.error(f"Error counting user queued tasks in enhanced queue: {str(e)}")
        return 0
@bot.on_message(filters.command("cancel_all") & filters.private)
@sudo_only
async def cancel_all_handler(client: Client, message: Message):
    """Cancel ALL tasks - Owner only"""
    try:
        user_id = message.from_user.id
        if user_id not in SUDO_USERS:
            await message.reply("❌ Only bot owner can cancel all tasks!")
            return
        total_active = len(active_tasks)
        total_queued = global_queue_manager.queue.qsize()
        if total_active == 0 and total_queued == 0:
            await message.reply("ℹ️ No active tasks found to cancel.")
            return
        # Show confirmation with detailed counts
        cancel_msg = await message.reply(
            f"👑 **Cancel ALL Tasks**\n\n"
            f"📊 **Current System Status:**\n"
            f"• Active Tasks: {total_active}\n"
            f"• Queued Tasks: {total_queued}\n"
            f"• Affected Users: {len(active_tasks)}\n\n"
            "⚠️ **This will cancel EVERYTHING:**\n"
            "• All active downloads/encoding\n"
            "• Entire task queue\n"
            "• All user operations\n\n"
            "**This action affects ALL users and cannot be undone!**",
            reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("✅ Yes, Cancel EVERYTHING", callback_data="confirm_cancel_all_owner"),
                    InlineKeyboardButton("❌ No, Keep Running", callback_data="cancel_cancel")
                ]
            ])
        )
    except Exception as e:
        logger.error(f"Cancel all error: {str(e)}")
        await message.reply(f"❌ Error cancelling all tasks: {str(e)}")
@bot.on_callback_query(filters.regex("^confirm_cancel_all_owner$"))
async def confirm_cancel_all_owner_handler(client: Client, query: CallbackQuery):
    """Handle confirmation of cancel ALL tasks (owner only)"""
    try:
        # Check if user is owner
        if query.from_user.id not in SUDO_USERS:
            await query.answer("❌ Only bot owner can cancel all tasks!", show_alert=True)
            return
        # Get counts before cancellation
        active_before = len(active_tasks)
        queued_before = global_queue_manager.queue.qsize()
        users_before = list(active_tasks.keys())
        # Cancel all tasks for every user
        cancelled_users = set()
        for chat_id in list(active_tasks.keys()):
            success = await cancel_user_tasks(chat_id)
            if success:
                cancelled_users.add(chat_id)
        # Clear the entire queue
        queue_cleared = await clear_entire_queue()
        # Build result message
        result_text = "👑 **All Tasks Cancelled**\n\n"
        if active_before > 0:
            result_text += f"• Stopped {active_before} active task(s)\n"
        if queued_before > 0:
            result_text += f"• Cleared {queued_before} queued task(s)\n"
        if cancelled_users:
            result_text += f"• Affected {len(cancelled_users)} user(s)\n"
        result_text += "\nAll operations have been stopped system-wide."
        await query.message.edit_text(result_text)
        await query.answer("All tasks cancelled!")
        logger.info(f"👑 Owner {query.from_user.id} cancelled ALL tasks: {active_before} active, {queued_before} queued")
    except Exception as e:
        logger.error(f"Confirm cancel all owner error: {str(e)}")
        await query.answer("Error cancelling all tasks!")
async def clear_entire_queue():
    """Clear the entire task queue"""
    try:
        cleared_count = 0
        while not global_queue_manager.queue.empty():
            try:
                await global_queue_manager.queue.get()
                global_queue_manager.queue.task_done()
                cleared_count += 1
            except:
                break
        # Reset current task
        global_queue_manager.current_task = None
        global_queue_manager.current_process = None
        logger.info(f"🧹 Cleared entire queue: {cleared_count} tasks")
        return cleared_count
    except Exception as e:
        logger.error(f"Error clearing entire queue: {str(e)}")
        return 0     
@bot.on_callback_query(filters.regex(r"^confirm_cancel_all_(\d+)$"))
async def confirm_cancel_all_handler(client: Client, query: CallbackQuery):
    """Handle confirmation of cancel all tasks"""
    try:
        chat_id = int(query.data.split("_")[3])
        success = await cancel_user_tasks(chat_id)
        if success:
            await query.message.edit_text(
                "✅ **All Tasks Cancelled**\n\n"
                "Your current tasks have been stopped:\n"
                "• Downloads cancelled\n"
                "• Encoding stopped\n" 
                "• Uploads terminated\n\n"
                "You can start new tasks whenever you're ready."
            )
            await query.answer("Tasks cancelled successfully")
            logger.info(f"✅ All tasks cancelled for user {chat_id}")
        else:
            await query.message.edit_text("❌ Failed to cancel tasks. They may have already completed.")
            await query.answer("Cancellation failed")
    except Exception as e:
        logger.error(f"Confirm cancel error: {str(e)}")
        await query.answer("Error cancelling tasks")
@bot.on_callback_query(filters.regex(r"^cancel_cancel_(\d+)$"))
async def cancel_cancel_handler(client: Client, query: CallbackQuery):
    """Handle cancellation of cancel request"""
    try:
        chat_id = int(query.data.split("_")[2])
        await query.message.edit_text("ℹ️ **Cancellation Cancelled**\n\nYour tasks will continue running.")
        await query.answer("Cancellation cancelled")
    except Exception as e:
        await query.answer("Error")
@bot.on_callback_query(filters.regex("^close_start$"))
async def close_start_handler(client: Client, query: CallbackQuery):
    """Close start menu"""
    try:
        await query.message.delete()
        await query.answer("Menu closed")
    except Exception as e:
        await query.answer("Error closing menu")
# Update all photo sending calls to use safe_send_photo
@bot.on_message(filters.command("start") & (filters.private | filters.group))
@handle_floodwait
async def start_handler(client: Client, message: Message):
    """Enhanced start command with clean design"""
    try:
        user_id = message.from_user.id
        is_sudo = user_id in SUDO_USERS
        # Clean buttons layout
        buttons = [
            [
                InlineKeyboardButton("🚀 ꜱᴛᴀʀᴛ ᴇɴᴄᴏᴅɪɴɢ", callback_data="start_encoding"),
                InlineKeyboardButton("⚙️ ʙᴏᴛ ꜰᴇᴀᴛᴜʀᴇꜱ", callback_data="show_features")
            ],
            [
                InlineKeyboardButton("📚 ʜᴏᴡ ᴛᴏ ᴜꜱᴇ", callback_data="how_to_use")
            ]
        ]
        # Add sudo button if applicable
        if is_sudo:
            buttons.append([InlineKeyboardButton("👑 Admin Panel", callback_data="admin_panel")])
        buttons.append([InlineKeyboardButton("Close", callback_data="close_start")])
        start_text = (
            "<blockquote>🌟 <b>Welcome to Advanced Video Encoder Bot</b></blockquote>\n"
            "<blockquote>⚡ <b>Quick Features:</b>\n"
            "• Multi-quality encoding\n"
            "• Torrent & file support\n"
            "• Batch processing\n"
            "• 8 video tune optimizations\n"
            "• Advanced watermark system\n"
            "• Real-time progress\n\n"
            "🎯 <b>Formats:</b> MKV, MP4, AVI, MOV, WEBM\n"
            "🔧 <b>Codecs:</b> H.264 H.265 AV1</blockquote>\n"
            "<blockquote>Maintained by @Rbotz</blockquote>"
        )
        # Edit existing message or send new
        if hasattr(start_handler, 'start_messages'):
            if user_id in start_handler.start_messages:
                try:
                    await bot.edit_message_media(
                        chat_id=user_id,
                        message_id=start_handler.start_messages[user_id],
                        media=InputMediaPhoto(
                            media=START_PICS["main"],
                            caption=start_text,
                            parse_mode=ParseMode.HTML
                        ),
                        reply_markup=InlineKeyboardMarkup(buttons)
                    )
                    return
                except:
                    # Message doesn't exist, create new
                    pass
        msg = await message.reply_photo(
            photo=START_PICS["main"],
            caption=start_text,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        # Store message ID for editing
        if not hasattr(start_handler, 'start_messages'):
            start_handler.start_messages = {}
        start_handler.start_messages[user_id] = msg.id
    except Exception as e:
        logger.error(f"Start error: {e}")
        await message.reply("❌ Failed to load menu. Please try again.")
@bot.on_callback_query(filters.regex("^back_to_start$"))
async def back_to_start_handler(client: Client, query: CallbackQuery):
    """Back to start - edits same message"""
    try:
        user_id = query.from_user.id
        is_sudo = user_id in SUDO_USERS
        buttons = [
            [
                InlineKeyboardButton("🚀 ꜱᴛᴀʀᴛ ᴇɴᴄᴏᴅɪɴɢ", callback_data="start_encoding"),
                InlineKeyboardButton("⚙️ ʙᴏᴛ ꜰᴇᴀᴛᴜʀᴇꜱ", callback_data="show_features")
            ],
            [
                InlineKeyboardButton("📚 ʜᴏᴡ ᴛᴏ ᴜꜱᴇ", callback_data="how_to_use")
            ]
        ]
        if is_sudo:
            buttons.append([InlineKeyboardButton("👑 Admin Panel", callback_data="admin_panel")])
        buttons.append([InlineKeyboardButton("❌ Close", callback_data="close_start")])
        start_text = (
                "<blockquote>🌟 <b>Welcome to Advanced Video Encoder Bot</b></blockquote>\n"
                "<blockquote>⚡ <b>Quick Features:</b>\n"
                "• Multi-quality encoding\n"
                "• Torrent & file support\n"
                "• Batch processing\n"
                "• 8 video tune optimizations\n"
                "• Advanced watermark system\n"
                "• Real-time progress\n\n"
                "🎯 <b>Formats:</b> MKV, MP4, AVI, MOV, WEBM\n"
                "🔧 <b>Codecs:</b> H.264 H.265 AV1</blockquote>\n"
                "<blockquote>Maintained by @Rbotz</blockquote>"

            )
        await query.message.edit_media(
            InputMediaPhoto(
                media=START_PICS["main"],
                caption=start_text,
                parse_mode=ParseMode.HTML
            ),
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        await query.answer("🏠 Main menu")
    except Exception as e:
        logger.error(f"Back to start error: {str(e)}")
        await query.answer("Error!")
# Add premium info callback handler
@bot.on_callback_query(filters.regex("^premium_info_start$"))
async def premium_info_start_handler(client: Client, query: CallbackQuery):
    """Show premium information from start menu"""
    user_id = query.from_user.id
    premium_info = await get_premium_info(user_id)
    buttons = [
        [InlineKeyboardButton("🔙 Back to Start", callback_data="back_to_start")],
        [InlineKeyboardButton("❌ Close", callback_data="close_menu")]
    ]
    await query.message.edit_text(
        premium_info,
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(buttons)
    )
    await query.answer()
@bot.on_callback_query(filters.regex("^show_features$"))
async def show_features_handler(client: Client, query: CallbackQuery):
    """Clean features overview"""
    features_text = (
        "⚡ **Advanced Video Encoder Bot - Features"
        "<blockquote expandable>\n"
        "🎯 Core Encoding Features:**\n"
        "• All Qualities Mode** - Create multiple versions (480p, 720p, 1080p, No-Encode)\n"
        "• Batch processing & torrent support\n"
        "• H.264 & H.265 codec support\n"
        "• Custom CRF quality tuning\n"
        "• 9 encoding presets (ultrafast to veryslow)\n\n"
        "🎨 **Advanced Video Tuning:**\n"  
        "• Animation-optimized encoding\n"
        "• Film & grain preservation\n"
        "• Fast decode for mobile devices\n"
        "• Zero latency for streaming\n"
        "• PSNR/SSIM quality metrics\n\n"
        "💧 Professional Watermark System:\n"
        "• 6 Position Options**: Top/Bottom corners, Center, Random\n"
        "• 5 Size Settings**: 5% to 25% of video width\n" 
        "• 5 Opacity Levels**: 30% to 100% transparency\n"
        "• Anti-Piracy Mode**: Random position changing\n"
        "• Real-time Preview**: Visual placement preview\n\n"
        "🔧 Additional Features:\n"
        "• Hard subtitle burning\n"
        "• Audio track management\n"
        "• Custom thumbnail support\n"
        "• Filename pattern remapping\n"
        "• Sample & screenshot generation\n"
        "• Real-time progress tracking\n"
        "</blockquote>"
    )
    buttons = [
        [
            InlineKeyboardButton("🚀 ꜱᴛᴀʀᴛ ᴇɴᴄᴏᴅɪɴɢ", callback_data="start_encoding"),
            InlineKeyboardButton("🔙 ꜱᴛᴀʀᴛ", callback_data="back_to_start")
        ]
    ]
    await query.message.edit_media(
        InputMediaPhoto(
            media=START_PICS["features"],
            caption=features_text,
            parse_mode=ParseMode.HTML
        ),
        reply_markup=InlineKeyboardMarkup(buttons)
    )
    await query.answer()  
@bot.on_callback_query(filters.regex("^how_to_use$"))
async def how_to_use_handler(client: Client, query: CallbackQuery):
    """Simple how-to guide"""
    how_to_text = (
        "<blockquote expandable>\n"
        "📚 <b>Quick Start Guide</b>\n\n"
        "🔧 <b>Method 1 - Torrent:</b>\n"
        "<code>/magnet magnet_link</code>\n\n"
        "🔧 <b>Method 2 - File:</b>\n"
        "Reply to file + <code>/compress</code>\n\n"
        "⚡ <b>Settings:</b>\n"
        "• Choose quality\n"
        "• Select codec\n"
        "• Set preset\n"
        "• Add watermark (optional)\n"
        "</blockquote>"
    )
    buttons = [
        [
            InlineKeyboardButton("ꜱᴛᴀʀᴛ ɴᴏᴡ", callback_data="start_encoding"),
            InlineKeyboardButton(" ꜰᴇᴀᴛᴜʀᴇꜱ", callback_data="show_features")
        ],
        [
            InlineKeyboardButton("ʙᴀᴄᴋ ᴛᴏ ꜱᴛᴀʀᴛ", callback_data="back_to_start"),
            InlineKeyboardButton("ᴄʟᴏꜱᴇ", callback_data="close_menu")
        ]
    ]
    await query.message.edit_media(
        InputMediaPhoto(
            media=START_PICS["help"],
            caption=how_to_text,
            parse_mode=ParseMode.HTML
        ),
        reply_markup=InlineKeyboardMarkup(buttons)
    )
    await query.answer()
@bot.on_callback_query(filters.regex("^start_encoding$"))
async def start_encoding_handler(client: Client, query: CallbackQuery):
    """Clean encoding start options"""
    encoding_text = (
        "<blockquote expandable>\n"
        "🚀 <b>Start Encoding</b>\n\n"
        "📥 <b>Choose Method:</b>\n"
        "• Torrent/Magnet links\n"
        "• Direct file upload\n\n"
        "🎯 <b>Quality Options:</b>\n"
        "• All Qualities (480p, 720p, 1080p, No-Encode)\n\n"
        "⚡ <b>Quick Tips:</b>\n"
        "• Use recommended settings first\n"
        "• Check file sizes\n"
        "• Monitor progress\n"
        "</blockquote>"

    )
    buttons = [
        [
            InlineKeyboardButton("🧲 USE MAGNET", callback_data="use_magnet"),
            InlineKeyboardButton("📁 UPLOAD FILE", callback_data="use_compress")
        ],
        [
            InlineKeyboardButton("⚙️ QUALITY SETTINGS", callback_data="all_qualities_" + str(query.from_user.id)),
            InlineKeyboardButton("🔙 BACK", callback_data="back_to_start")
        ],
        [
            InlineKeyboardButton("❌ CLOSE", callback_data="close_menu")
        ]
    ]
    await query.message.edit_media(
        InputMediaPhoto(
            media=START_PICS["encoding"],
            caption=encoding_text,
            parse_mode=ParseMode.HTML
        ),
        reply_markup=InlineKeyboardMarkup(buttons)
    )
    await query.answer()
@bot.on_callback_query(filters.regex("^use_magnet$"))
async def use_magnet_handler(client: Client, query: CallbackQuery):
    """Quick magnet start"""
    await query.message.edit_text(
        "🧲 <b>Send Magnet Link</b>\n\n"
        "Use: <code>/magnet magnet_link</code>\n\n"
        "Example:\n"
        "<code>/magnet magnet:?xt=urn:btih:EXAMPLE</code>",
        parse_mode=ParseMode.HTML,
        reply_markup = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("🔙 ʙᴀᴄᴋ", callback_data="start_encoding"),
                InlineKeyboardButton("❌ ᴄʟᴏꜱᴇ", callback_data="close_menu")
            ]
        ])
    )
    await query.answer()
@bot.on_callback_query(filters.regex("^use_compress$"))
async def use_compress_handler(client: Client, query: CallbackQuery):
    """Quick compress start"""
    await query.message.edit_text(
        "📁 <b>Upload File</b>\n\n"
        "1. Send or reply to video file\n"
        "2. Use: <code>/compress</code>\n\n"
        "Supported: MKV, MP4, AVI, MOV, WEBM",
        parse_mode=ParseMode.HTML,
        reply_markup = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("🔙 ʙᴀᴄᴋ", callback_data="start_encoding"),
                InlineKeyboardButton("❌ ᴄʟᴏꜱᴇ", callback_data="close_menu")
            ]
        ])
    )
    await query.answer()
@bot.on_callback_query(filters.regex("^close_start$"))
async def close_start_handler(client: Client, query: CallbackQuery):
    """Close start menu and clean storage"""
    try:
        user_id = query.from_user.id
        # Clean up stored message
        if hasattr(start_handler, 'start_messages'):
            if user_id in start_handler.start_messages:
                del start_handler.start_messages[user_id]
        await query.message.delete()
        await query.answer("Menu closed")
    except Exception as e:
        await query.answer("Error closing menu")
@bot.on_callback_query(filters.regex("^close_menu$"))
async def close_menu_handler(client: Client, query: CallbackQuery):
    """Close any menu"""
    try:
        await query.message.delete()
        await query.answer("Menu closed")
    except Exception as e:
        await query.answer("Error closing menu")
@bot.on_callback_query(filters.regex("^help_support$"))
async def help_support_handler(client: Client, query: CallbackQuery):
    """Help and support information"""
    help_text = (
        "❓ <b>Help & Support Center</b>\n\n"
        "<blockquote expandable>\n"
        "🆘 <b>Common Solutions:</b>\n\n"
        "🔧 <b>Encoding Issues:</b>\n"
        "• Use medium preset for stability\n"
        "• Try H.264 codec for compatibility\n"
        "• Check file size limits\n"
        "• Ensure proper file formats\n\n"
        "📊 <b>Performance Tips:</b>\n"
        "• Smaller files process faster\n"
        "• Use faster presets for large files\n"
        "• Monitor queue status\n"
        "• Cancel stuck tasks if needed\n\n"
        "⚙️ <b>Settings Guidance:</b>\n"
        "• Start with recommended settings\n"
        "• Experiment with one setting at a time\n"
        "• Use preview feature to check results\n"
        "</blockquote>"
    )
    buttons = [
        [
            InlineKeyboardButton("🔙 Back to Start", callback_data="back_to_start"),
            InlineKeyboardButton("❌ Close", callback_data="close_menu")
        ]
    ]
    await query.message.reply_photo(
        photo=START_PICS["help"],
        caption=help_text,
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(buttons)
    )
    await query.message.delete()
    await query.answer()
@bot.on_callback_query(filters.regex("^premium_management$"))
async def premium_management_handler(client: Client, query: CallbackQuery):
    """Premium management panel in admin"""
    try:
        user_id = query.from_user.id
        if user_id not in SUDO_USERS:
            await query.answer("❌ Admin access required!", show_alert=True)
            return
        # Count active and expired premiums
        current_time = time.time()
        active_premiums = 0
        expired_premiums = 0
        for user_data in PREMIUM_USERS.values():
            if user_data.get('expiry', 0) > current_time:
                active_premiums += 1
            else:
                expired_premiums += 1
        premium_text = (
            "💎 <b>Premium Management</b>\n\n"
            "<blockquote>"
            f"• Total Premium Users: {len(PREMIUM_USERS)}\n"
            f"• Active Premiums: {active_premiums}\n"
            f"• Expired Premiums: {expired_premiums}\n"
            f"• Authorization Mode: {'🔒 Restricted' if BOT_AUTHORIZED else '🔓 Public'}\n\n"
            "Manage premium user access and subscriptions:\n"
            "</blockquote>"
        )
        buttons = [
            [
                InlineKeyboardButton("➕ Add Premium", callback_data="add_premium_menu"),
                InlineKeyboardButton("➖ Remove Premium", callback_data="remove_premium_menu")
            ],
            [
                InlineKeyboardButton("📋 Premium List", callback_data="premium_list_admin"),
                InlineKeyboardButton("🔄 Clean Expired", callback_data="clean_expired_premium")
            ],
            [
                InlineKeyboardButton("🔙 Back to Admin", callback_data="admin_panel"),
                InlineKeyboardButton("❌ Close", callback_data="close_menu")
            ]
        ]
        await query.message.edit_text(
            premium_text,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        await query.answer()
    except Exception as e:
        logger.error(f"Premium management error: {str(e)}")
        await query.answer("Error loading premium management!")
@bot.on_callback_query(filters.regex("^add_premium_menu$"))
async def add_premium_menu_handler(client: Client, query: CallbackQuery):
    """Add premium user interface"""
    try:
        user_id = query.from_user.id
        if user_id not in SUDO_USERS:
            await query.answer("❌ Admin access required!", show_alert=True)
            return
        add_text = (
            "➕ <b>Add Premium User</b>\n\n"
            "<blockquote>"
            "To add premium user, use the command:\n"
            "```\n"
            "/addpremium USER_ID DURATION PLAN_NAME\n"
            "```\n\n"
            "💡 <b>Examples:</b>\n"
            "• <code>/addpremium 123456789 30d Pro</code>\n"
            "• <code>/addpremium 123456789 7d Basic</code>\n"
            "• <code>/addpremium 123456789 24h Trial</code>\n\n"
            "Or send user ID now to continue:\n"
            "</blockquote>"
        )
        buttons = [
            [InlineKeyboardButton("🔙 Back to Premium", callback_data="premium_management")],
            [InlineKeyboardButton("❌ Cancel", callback_data="admin_panel")]
        ]
        # Store state for input handling
        await update_user_settings(user_id, {"awaiting": "add_premium_user"})
        await query.message.edit_text(
            add_text,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        await query.answer()
    except Exception as e:
        logger.error(f"Add premium menu error: {str(e)}")
        await query.answer("Error loading add premium menu!")
@bot.on_callback_query(filters.regex("^remove_premium_menu$"))
async def remove_premium_menu_handler(client: Client, query: CallbackQuery):
    """Remove premium user interface"""
    try:
        user_id = query.from_user.id
        if user_id not in SUDO_USERS:
            await query.answer("❌ Admin access required!", show_alert=True)
            return
        remove_text = (
            "➖ <b>Remove Premium User</b>\n\n"
            "<blockquote>"
            "To remove premium user, use the command:\n"
            "```\n"
            "/removepremium USER_ID\n"
            "```\n\n"
            "Or send user ID now to continue:\n"
            "</blockquote>"
        )
        buttons = [
            [InlineKeyboardButton("🔙 Back to Premium", callback_data="premium_management")],
            [InlineKeyboardButton("❌ Cancel", callback_data="admin_panel")]
        ]
        # Store state for input handling
        await update_user_settings(user_id, {"awaiting": "remove_premium_user"})
        await query.message.edit_text(
            remove_text,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        await query.answer()
    except Exception as e:
        logger.error(f"Remove premium menu error: {str(e)}")
        await query.answer("Error loading remove premium menu!")
@bot.on_callback_query(filters.regex("^premium_list_admin$"))
async def premium_list_admin_handler(client: Client, query: CallbackQuery):
    """Premium list in admin panel"""
    try:
        user_id = query.from_user.id
        if user_id not in SUDO_USERS:
            await query.answer("❌ Admin access required!", show_alert=True)
            return
        if not PREMIUM_USERS:
            await query.message.edit_text(
                "ℹ️ <b>No Premium Users</b>\n\n"
                "There are no premium users currently.",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔙 Back", callback_data="premium_management")
                ]])
            )
            return
        premium_list = []
        current_time = time.time()
        for user_id, data in PREMIUM_USERS.items():
            expiry = data.get('expiry', 0)
            remaining = expiry - current_time
            if remaining <= 0:
                status = "❌ EXPIRED"
            else:
                days = int(remaining // 86400)
                hours = int((remaining % 86400) // 3600)
                status = f"✅ {days}d {hours}h"
            plan = data.get('plan', 'Custom')
            added_by = data.get('added_by', 'Unknown')
            premium_list.append(f"• `{user_id}` - {plan} - {status} - by {added_by}")
        list_text = (
            f"💎 <b>Premium Users List</b>\n\n"
            f"Total: {len(PREMIUM_USERS)} users\n\n"
            f"{chr(10).join(premium_list[:15])}"  # Show first 15
        )
        if len(PREMIUM_USERS) > 15:
            list_text += f"\n\n... and {len(PREMIUM_USERS) - 15} more users"
        buttons = [
            [InlineKeyboardButton("🔄 Refresh", callback_data="premium_list_admin")],
            [InlineKeyboardButton("🔙 Back to Premium", callback_data="premium_management")],
            [InlineKeyboardButton("❌ Close", callback_data="close_menu")]
        ]
        await query.message.edit_text(
            list_text,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        await query.answer()
    except Exception as e:
        logger.error(f"Premium list admin error: {str(e)}")
        await query.answer("Error loading premium list!")
@bot.on_callback_query(filters.regex("^clean_expired_premium$"))
async def clean_expired_premium_handler(client: Client, query: CallbackQuery):
    """Clean expired premium users"""
    try:
        user_id = query.from_user.id
        if user_id not in SUDO_USERS:
            await query.answer("❌ Admin access required!", show_alert=True)
            return
        current_time = time.time()
        expired_count = 0
        for user_id in list(PREMIUM_USERS.keys()):
            if PREMIUM_USERS[user_id].get('expiry', 0) < current_time:
                await revoke_premium(user_id)
                expired_count += 1
        if expired_count > 0:
            await query.answer(f"✅ Cleaned {expired_count} expired premium users!")
        else:
            await query.answer("ℹ️ No expired premium users found!")
        await premium_management_handler(client, query)
    except Exception as e:
        logger.error(f"Clean expired premium error: {str(e)}")
        await query.answer("Error cleaning expired premium users!")
@bot.on_message(filters.command("premium_info") & (filters.private | filters.group))
async def premium_info_handler(client: Client, message: Message):
    """Show premium status information"""
    user_id = message.from_user.id
    premium_info = await get_premium_info(user_id)
    await message.reply(premium_info, parse_mode=ParseMode.HTML)
@bot.on_message(filters.command("addpremium") & filters.private)
@sudo_only
async def add_premium_handler(client: Client, message: Message):
    """Add premium to user"""
    try:
        if len(message.command) < 4:
            await message.reply(
                "❌ <b>Usage:</b> <code>/addpremium user_id duration plan_name</code>\n\n"
                "💡 <b>Examples:</b>\n"
                "<code>/addpremium 123456789 30d Pro</code> - 30 days Pro plan\n"
                "<code>/addpremium 123456789 7d Basic</code> - 7 days Basic plan\n"
                "<code>/addpremium 123456789 24h Trial</code> - 24 hours Trial\n\n"
                "⏰ <b>Duration formats:</b>\n"
                "• 24h - 24 hours\n"
                "• 7d - 7 days\n"
                "• 30d - 30 days\n"
                "• 90d - 90 days\n"
                "• 365d - 1 year",
                parse_mode=ParseMode.HTML
            )
            return
        user_id = int(message.command[1])
        duration_str = message.command[2].lower()
        plan_name = ' '.join(message.command[3:])
        # Parse duration
        if duration_str.endswith('h'):
            hours = int(duration_str[:-1])
            seconds = hours * 3600
        elif duration_str.endswith('d'):
            days = int(duration_str[:-1])
            seconds = days * 86400
        else:
            await message.reply("❌ Invalid duration format. Use 'h' for hours or 'd' for days.")
            return
        expiry_time = time.time() + seconds
        expiry_date = datetime.fromtimestamp(expiry_time)
        premium_data = {
            "expiry": expiry_time,
            "added_by": message.from_user.id,
            "plan": plan_name,
            "added_date": datetime.now().isoformat()
        }
        # Add to premium users
        PREMIUM_USERS[user_id] = premium_data
        await save_premium_user(user_id, premium_data)
        await message.reply(
            f"✅ <b>Premium Added Successfully!</b>\n\n"
            f"👤 <b>User ID:</b> <code>{user_id}</code>\n"
            f"💎 <b>Plan:</b> {plan_name}\n"
            f"⏰ <b>Duration:</b> {duration_str}\n"
            f"📅 <b>Expires:</b> {expiry_date.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"👑 <b>Added by:</b> {message.from_user.mention}",
            parse_mode=ParseMode.HTML
        )
        # Notify the user if possible
        try:
            await bot.send_message(
                user_id,
                f"🎉 <b>You've been granted Premium Access!</b>\n\n"
                f"💎 <b>Plan:</b> {plan_name}\n"
                f"⏰ <b>Duration:</b> {duration_str}\n"
                f"📅 <b>Expires:</b> {expiry_date.strftime('%Y-%m-%d %H:%M:%S')}\n\n"
                f"✨ <b>Now you can:</b>\n"
                f"• Use /magnet and /compress commands\n"
                f"• Access all encoding features\n"
                f"• Enjoy priority processing\n\n"
                f"Use /premium_info to check your status.",
                parse_mode=ParseMode.HTML
            )
        except Exception as e:
            logger.warning(f"Could not notify user {user_id} about premium: {str(e)}")
    except ValueError:
        await message.reply("❌ Invalid user ID or duration format.")
    except Exception as e:
        logger.error(f"Add premium error: {str(e)}")
        await message.reply(f"❌ Error adding premium: {str(e)}")
@bot.on_message(filters.command("removepremium") & filters.private)
@sudo_only
async def remove_premium_handler(client: Client, message: Message):
    """Remove premium from user"""
    try:
        if len(message.command) < 2:
            await message.reply(
                "❌ <b>Usage:</b> <code>/removepremium user_id</code>\n\n"
                "💡 <b>Example:</b>\n"
                "<code>/removepremium 123456789</code>",
                parse_mode=ParseMode.HTML
            )
            return
        user_id = int(message.command[1])
        if user_id not in PREMIUM_USERS:
            await message.reply(f"❌ User <code>{user_id}</code> is not a premium user.", parse_mode=ParseMode.HTML)
            return
        await revoke_premium(user_id)
        await message.reply(
            f"✅ <b>Premium Removed Successfully!</b>\n\n"
            f"👤 <b>User ID:</b> <code>{user_id}</code>\n"
            f"👑 <b>Removed by:</b> {message.from_user.mention}",
            parse_mode=ParseMode.HTML
        )
    except ValueError:
        await message.reply("❌ Invalid user ID format.")
    except Exception as e:
        logger.error(f"Remove premium error: {str(e)}")
        await message.reply(f"❌ Error removing premium: {str(e)}")
async def revoke_premium(user_id: int):
    """Revoke premium access from user"""
    if user_id in PREMIUM_USERS:
        del PREMIUM_USERS[user_id]
        await remove_premium_user(user_id)
        # Notify user if possible
        try:
            await bot.send_message(
                user_id,
                "🔒 <b>Premium Access Revoked</b>\n\n"
                "Your premium access has been revoked by admin.\n\n"
                "If you believe this is a mistake, please contact the administrator.",
                parse_mode=ParseMode.HTML
            )
        except Exception as e:
            logger.warning(f"Could not notify user {user_id} about premium revocation: {str(e)}")
@bot.on_message(filters.command("premiumlist") & filters.private)
@sudo_only
async def premium_list_handler(client: Client, message: Message):
    """List all premium users"""
    try:
        if not PREMIUM_USERS:
            await message.reply("ℹ️ No premium users found.")
            return
        premium_list = []
        current_time = time.time()
        for user_id, data in PREMIUM_USERS.items():
            expiry = data.get('expiry', 0)
            remaining = expiry - current_time
            if remaining <= 0:
                status = "❌ EXPIRED"
            else:
                days = int(remaining // 86400)
                hours = int((remaining % 86400) // 3600)
                status = f"✅ {days}d {hours}h"
            plan = data.get('plan', 'Custom')
            added_by = data.get('added_by', 'Unknown')
            premium_list.append(f"• `{user_id}` - {plan} - {status} - by {added_by}")
        list_text = (
            f"💎 <b>Premium Users List</b>\n\n"
            f"Total: {len(PREMIUM_USERS)} users\n\n"
            f"{chr(10).join(premium_list)}"
        )
        # Split if too long
        if len(list_text) > 4000:
            parts = [list_text[i:i+4000] for i in range(0, len(list_text), 4000)]
            for part in parts:
                await message.reply(part, parse_mode=ParseMode.HTML)
        else:
            await message.reply(list_text, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Premium list error: {str(e)}")
        await message.reply(f"❌ Error getting premium list: {str(e)}")
@bot.on_message(filters.command("broadcast") & filters.private)
@sudo_only
async def broadcast_handler(client: Client, message: Message):
    """Simple broadcast without confirmation"""
    try:
        if not message.reply_to_message:
            await message.reply(
                "❌ **Usage:** Reply to a message with `/broadcast`\n\n"
                "Example:\n"
                "1. Write your broadcast message\n"
                "2. Reply to it with `/broadcast`"
            )
            return
        
        broadcast_text = message.reply_to_message.text or message.reply_to_message.caption
        if not broadcast_text:
            await message.reply("❌ The replied message must contain text.")
            return
        
        # Get all users from sessions and active tasks
        all_users = set()
        
        # Add users from sessions
        for chat_id in session_manager.sessions.keys():
            all_users.add(chat_id)
        
        # Add users from active tasks
        for chat_id in active_tasks.keys():
            all_users.add(chat_id)
        
        # Add users from MongoDB if available
        if user_settings is not None:
            try:
                users_from_db = await user_settings.distinct("chat_id")
                for user_id in users_from_db:
                    all_users.add(user_id)
            except Exception as e:
                logger.warning(f"Could not get users from DB: {str(e)}")
        
        total_users = len(all_users)
        if total_users == 0:
            await message.reply("❌ No users found to broadcast.")
            return
        
        # Send broadcast immediately
        progress_msg = await message.reply(f"📢 Broadcasting to {total_users} users...")
        
        success_count = 0
        failed_count = 0
        
        for user_id in all_users:
            try:
                await bot.send_message(user_id, f"{broadcast_text}")
                success_count += 1
                await asyncio.sleep(0.1)  # Small delay to avoid flooding
            except Exception as e:
                failed_count += 1
                logger.warning(f"Failed to send broadcast to {user_id}: {str(e)}")
        
        await progress_msg.edit_text(
            f"✅ **Broadcast Complete**\n\n"
            f"📊 **Results:**\n"
            f"• ✅ Successful: {success_count}\n"
            f"• ❌ Failed: {failed_count}\n"
            f"• 👥 Total: {total_users}"
        )
        
    except Exception as e:
        logger.error(f"Broadcast error: {str(e)}")
        await message.reply(f"❌ Broadcast failed: {str(e)}")
@bot.on_message(filters.command("magnet") & (filters.private | filters.group))
@authorized_only
@handle_floodwait
async def magnet_handler(client: Client, message: Message):
    """Handle magnet link command with queuing"""
    try:
        if len(message.command) < 2:
            await message.reply(
                "❌ Please provide a magnet link after the command\n"
                "Example: /magnet magnet:?xt=urn:btih:EXAMPLE"
            )
            return
        magnet_link = ' '.join(message.command[1:]).strip()
        if magnet_link.startswith("magnet?"):
            magnet_link = magnet_link.replace("magnet?", "magnet:?", 1)
        if not ("magnet:" in magnet_link and "xt=urn:btih:" in magnet_link):
            await message.reply(
                "⚠️ Invalid magnet link format\n\n"
                "A valid magnet link should:\n"
                "1. Start with 'magnet:'\n"
                "2. Contain 'xt=urn:btih:'\n"
                "3. Example: /magnet magnet:?xt=urn:btih:EXAMPLE\n\n"
                "Please check your link and try again."
            )
            return
        chat_id = message.chat.id
        logger.info(f"Processing magnet link from {chat_id}: {magnet_link[:60]}...")
        await update_user_settings(chat_id, {
            "magnet_link": magnet_link,
            "status": "received_magnet",
            "task_type": "magnet",  # Set task type as magnet
            "created_at": time.time(),
            "metadata": {"title": ""},
            "upload_mode": "document",
            "quality": "all_qualities",  # 🎯 Force All Qualities
            "all_qualities": True,      # 🎯 Enable All Qualities flag
            "selected_qualities": ["480p"],  # 🎯 Default qualities
            "codec": "h264",
            "preset": "medium",
            "awaiting": None,
            "subtitle_mode": "keep",
            "audio_mode": "keep",
            "video_tune": "none", 
            "samples_enabled": False,
            "screenshots_enabled": False,
            "custom_crf": None,
            "watermark_enabled": False,
            "cancelled": False
        })
        await ask_for_quality(chat_id)
    except Exception as e:
        logger.error(f"Error in magnet handler: {str(e)}", exc_info=True)
        error_msg = (
            "❌ Failed to process magnet link\n\n"
            f"Error: {str(e)}\n\n"
            "Please try again or contact support if this persists."
        )
        await message.reply(error_msg)

@bot.on_message(filters.command("compress") & (filters.private | filters.group))
@authorized_only
async def compress_handler(client: Client, message: Message):
    """Handle file compression command - ASK SETTINGS FIRST, DOWNLOAD LATER IN QUEUE"""
    try:
        chat_id = message.chat.id
        
        # Check if user replied to a file (either document or video)
        if not message.reply_to_message or (not message.reply_to_message.document and not message.reply_to_message.video):
            await message.reply(
                "❌ Please reply to a video file with /compress command\n\n"
                "Example: Reply to a video file and send `/compress`"
            )
            return
            
        file_message = message.reply_to_message
        
        # Handle both document and video message types
        if file_message.document:
            file_name = file_message.document.file_name
            file_size = file_message.document.file_size
            mime_type = file_message.document.mime_type or ""
        elif file_message.video:
            file_name = file_message.video.file_name or f"video_{file_message.video.file_id}.mp4"
            file_size = file_message.video.file_size
            mime_type = "video/mp4"  # Default for video messages
        else:
            await message.reply("❌ Unsupported message type. Please reply to a video file.")
            return

        # Validate file type - support both document videos and direct video messages
        is_video_file = (
            file_name.lower().endswith(('.mkv', '.mp4', '.avi', '.mov', '.flv', '.wmv', '.webm')) or
            mime_type.startswith('video/')
        )
        
        if not is_video_file:
            await message.reply(
                "❌ Please reply to a video file\n\n"
                "Supported formats: MKV, MP4, AVI, MOV, FLV, WMV, WEBM"
            )
            return

        # Check file size
        if file_size > MAX_FILE_SIZE:
            await message.reply(
                f"❌ File size exceeds limit ({humanize.naturalsize(MAX_FILE_SIZE)})\n\n"
                f"Your file: {humanize.naturalsize(file_size)}"
            )
            return

        # Store file info for later download - BUT DON'T DOWNLOAD YET
        file_info = {
            "message_id": file_message.id,
            "file_name": file_name,
            "file_size": file_size,
            "chat_id": file_message.chat.id,
            "is_video_message": bool(file_message.video)  # Track if it's a video message
        }
        
        await update_user_settings(chat_id, {
            "compress_file_info": file_info,
            "status": "compress_pending",  # File is ready but not downloaded
            "task_type": "compress",  # Set task type as compress
            "created_at": time.time(),
            "metadata": {"title": ospath.splitext(file_name)[0]},  # Set filename as default title
            "upload_mode": "document",
            "quality": "all_qualities",  # 🎯 Force All Qualities
            "all_qualities": True,      # 🎯 Enable All Qualities flag
            "selected_qualities": [],  # 🎯 Default qualities
            "codec": "h264",    # Default codec
            "preset": "medium", # Default preset
            "awaiting": None,
            "subtitle_mode": "keep",
            "audio_mode": "keep", 
            "video_tune": "none", 
            "samples_enabled": False,
            "screenshots_enabled": False,
            "custom_crf": None,
            "watermark_enabled": False,
            "cancelled": False
        })

        # Show quality settings menu FIRST - NO DOWNLOAD HAPPENS HERE
        await ask_for_quality(chat_id)
        
    except Exception as e:
        logger.error(f"Compress handler error: {str(e)}")
        await message.reply(f"❌ Failed to process file: {str(e)}")
async def download_compress_file_when_ready(chat_id: int) -> Optional[str]:
    """Download the compress file ONLY when user starts processing - ENHANCED VERSION"""
    try:
        session = await get_user_settings(chat_id)
        if not session or "compress_file_info" not in session:
            return None

        file_info = session["compress_file_info"]
        file_message_id = file_info["message_id"]
        file_name = file_info["file_name"]
        file_size = file_info["file_size"]
        file_chat_id = file_info["chat_id"]
        is_video_message = file_info.get("is_video_message", False)

        # Fetch the Telegram message containing the file
        try:
            file_message = await bot.get_messages(file_chat_id, file_message_id)
        except Exception as e:
            logger.error(f"Failed to get file message: {str(e)}")
            return None

        # Prepare download folder
        download_dir = ospath.join("downloads", f"compress_{chat_id}_{int(time.time())}")
        makedirs(download_dir, exist_ok=True)
        file_path = ospath.join(download_dir, file_name)

        # Initial message
        progress_msg = await bot.send_message(
            chat_id,
            f"📥 **Download Starting...**\n\n"
            f"📄 **File:** `{file_name}`\n"
            f"📦 **Size:** {humanize.naturalsize(file_size)}\n\n"
            f"🔄 Preparing download...",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("❌ Cancel Download", callback_data=f"cancel_download_{chat_id}")
            ]])
        )

        await asyncio.sleep(1)

        last_update = time.time()
        downloaded_bytes = 0
        start_time = time.time()
        speed_samples = []

        async def progress_callback(current, total):
            nonlocal last_update, downloaded_bytes, speed_samples
            now = time.time()
            if now - last_update < 3.0:
                return

            progress = (current / total) * 100
            elapsed = now - start_time

            # Calculate average speed
            current_speed = (current - downloaded_bytes) / (now - last_update)
            speed_samples.append(current_speed)
            if len(speed_samples) > 5:
                speed_samples.pop(0)
            avg_speed = sum(speed_samples) / len(speed_samples) if speed_samples else current_speed

            # ETA
            eta_seconds = (total - current) / avg_speed if avg_speed > 0 else 0
            if eta_seconds < 60:
                eta_str = f"{int(eta_seconds)}s"
            elif eta_seconds < 3600:
                eta_str = f"{int(eta_seconds // 60)}m {int(eta_seconds % 60)}s"
            else:
                hours = int(eta_seconds // 3600)
                minutes = int((eta_seconds % 3600) // 60)
                eta_str = f"{hours}h {minutes}m"

            speed_str = humanize.naturalsize(avg_speed) + "/s"
            progress_bar = get_random_progress_bar(progress, length=12)

            # Update Telegram message
            try:
                await progress_msg.edit_text(
                    f"📥 **Downloading File**\n\n"
                    f"📄 `{file_name[:35]}{'...' if len(file_name) > 35 else ''}`\n\n"
                    f"{progress_bar} **{progress:.1f}%**\n\n"
                    f"📊 **Progress:** {humanize.naturalsize(current)} / {humanize.naturalsize(total)}\n"
                    f"🚀 **Speed:** {speed_str}\n"
                    f"⏳ **ETA:** {eta_str}\n"
                    f"🕒 **Elapsed:** {humanize.precisedelta(elapsed)}",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("❌ Cancel Download", callback_data=f"cancel_download_{chat_id}")
                    ]])
                )
            except (MessageNotModified, FloodWait):
                pass
            except Exception as e:
                logger.warning(f"Progress update error: {str(e)}")

            # ✅ Real-time active task tracking (same as torrent downloads)
            try:
                dl_speed = avg_speed
                eta = eta_seconds
                current_time = now
                async with task_lock:
                    if chat_id in active_tasks:
                        active_tasks[chat_id].update({
                            'real_progress': progress,
                            'real_speed': dl_speed,
                            'real_eta': eta,
                            'last_progress_update': current_time
                        })
            except Exception as e:
                logger.warning(f"Failed to update active_tasks: {str(e)}")

            last_update = now
            downloaded_bytes = current

        # Perform the actual download
        if is_video_message and file_message.video:
            await file_message.download(file_path, progress=progress_callback)
        elif file_message.document:
            await file_message.download(file_path, progress=progress_callback)
        else:
            await progress_msg.edit_text("❌ Unsupported file type")
            return None

        # Verify downloaded file
        if not ospath.exists(file_path) or ospath.getsize(file_path) == 0:
            await progress_msg.edit_text("❌ Download failed - file is empty or missing")
            return None

        actual_size = ospath.getsize(file_path)
        total_time = time.time() - start_time

        await progress_msg.edit_text(
            f"✅ **Download Complete!**\n\n"
            f"📄 **File:** `{file_name}`\n"
            f"📦 **Size:** {humanize.naturalsize(actual_size)}\n"
            f"⏱️ **Time:** {humanize.precisedelta(total_time)}\n"
            f"🚀 **Avg Speed:** {humanize.naturalsize(actual_size / total_time) + '/s'}"
        )

        await asyncio.sleep(1)
        try:
            await bot.delete_messages(chat_id, progress_msg.id)
        except Exception:
            pass

        # Store file info in user session
        file_data = [{
            'path': file_path,
            'size': actual_size,
            'name': file_name,
            'is_video': True
        }]

        await update_user_settings(chat_id, {
            "files": file_data,
            "status": "compress_downloaded",
            "file_path": file_path,
            "download_path": download_dir,
        })

        logger.info(f"✅ Compress file downloaded and stored: {file_path}")
        return file_path

    except Exception as e:
        logger.error(f"Compress download error: {str(e)}")
        try:
            await bot.send_message(chat_id, f"❌ Download failed: {str(e)}")
        except:
            pass
        return None

@bot.on_callback_query(filters.regex(r"^confirm_download_(\d+)$"))
@handle_floodwait
async def confirm_download_enhanced_handler(client: Client, query: CallbackQuery):
    """Start processing - CHECK FOR SELECTED QUALITIES FIRST"""
    try:
        chat_id = int(query.data.split("_")[2])
        
        # FIRST: Check if qualities are selected
        session = await get_user_settings(chat_id)
        if not session:
            await query.answer("Session expired!", show_alert=True)
            return
            
        selected_qualities = session.get("selected_qualities", [])
        
        # 🎯 CHECK: Prevent processing if no qualities selected
        if not selected_qualities:
            await query.answer("❌ Please select at least one quality!", show_alert=True)
            await show_all_qualities_selector(chat_id)  # Send back to quality selection
            return
        await query.message.delete()
        await cleanup_settings_messages(chat_id)
        session = await get_user_settings(chat_id)
        if not session:
            await query.answer("Session expired!", show_alert=True)
            return
        # Get enhanced queue status
        queue_status = await global_queue_manager.get_queue_status_with_estimates()
        # Calculate user-specific wait time
        user_wait_time = queue_status['current_task_remaining']
        user_position = 0
        for idx, task in enumerate(queue_status['queue_list']):
            if task['chat_id'] == chat_id:
                user_position = idx + 1
                break
            user_wait_time += task['estimated_duration']
        # If user not in queue, add current queue wait time
        if user_position == 0:
            user_position = queue_status['queue_size'] + 1
            user_wait_time = queue_status['queue_wait_time']
        # Show queue position message with accurate estimates
        if queue_status["current_task"] is None and queue_status['queue_size'] == 0:
            queue_msg = "🔄 **Starting immediately...**\nYour task will begin in a few seconds!"
            estimated_str = "Less than 1 minute"
        else:
            queue_msg = f"⏳ **Added to Queue**\n📊 **Position:** #{user_position}"
            estimated_str = "Follow queue for updates"  # Removed specific time estimation
        # Get user priority status
        user_priority = "👑 Sudo" if chat_id in SUDO_USERS else "💎 Premium" if await is_premium_user(chat_id) else "👤 Normal"
        # Send immediate queue status
        selected_text = ", ".join(selected_qualities)

        status_msg = await bot.send_message(
            chat_id,
            f"✅ **Task Added to Processing Queue**\n\n"
            f"{queue_msg}\n"
            f"⏰ **Estimated wait:** {estimated_str}\n"
            f"🎯 **Your priority:** {user_priority}\n\n"
            f"📊 **Qualities:** {selected_text}\n\n"
            f"📈 **Queue Status:**\n"
            f"• Active tasks: {queue_status['active_tasks_count']}\n"
            f"• Waiting in queue: {queue_status['queue_size']}\n"
            f"• Follow queue for real-time updates\n\n"
          #  f"• Total queue wait: {humanize.naturaldelta(queue_status['queue_wait_time'])}\n\n"
            f"🔄 I'll notify you when processing starts!",
            reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("📊 View Queue", callback_data="refresh_queue_enhanced"),
                    InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_my_tasks_confirm_{chat_id}")
                ],
                [InlineKeyboardButton("❌ Close", callback_data="close_queue")]
            ])
        )
        # Add small delay to show queue status
        await asyncio.sleep(3)
        # Check if this is All Qualities mode
        is_all_qualities = session.get("quality") == "all_qualities" or session.get("all_qualities", False)
        # Check if this is All Qualities mode
        if is_all_qualities:
            await query.answer("🚀 Added to queue - All Qualities Processing...")
            selected_qualities = session.get("selected_qualities", [])
            # Ensure original_settings is set
            if not session.get("original_settings"):
                original_settings = {
                    "quality": session.get("quality", "720p"),
                    "watermark": session.get("watermark"),
                    "watermark_enabled": session.get("watermark_enabled", False),
                    "thumbnail": session.get("thumbnail"),
                    "upload_mode": session.get("upload_mode", "video"),
                    "metadata": session.get("metadata", {}),
                    "remname": session.get("remname"),
                    "codec": session.get("codec", "h264"),
                    "subtitle_mode": session.get("subtitle_mode", "keep"),
                    "subtitle_file": session.get("subtitle_file"),
                    "audio_mode": session.get("audio_mode", "keep"),
                    "audio_file": session.get("audio_file"),
                    "custom_crf": session.get("custom_crf"),
                    "preset": session.get("preset", "veryfast"),
                    "samples_enabled": session.get("samples_enabled", False),
                    "screenshots_enabled": session.get("screenshots_enabled", False)
                }
                await update_user_settings(chat_id, {"original_settings": original_settings})
            # Add to global queue with proper arguments
            task_id = await task_queue.add_task(chat_id, start_all_qualities_processing, task_id=str(uuid.uuid4())[:8])
        else:
            await query.answer("✅ Added to queue - Starting processing...")
            # Check task type
            task_type = session.get("task_type", "magnet")
            if task_type == "compress":
                if "compress_file_info" not in session:
                    await update_progress(chat_id, "❌ No file to process! Please use /compress again.", force_new=True)
                    return
                # For compress tasks, just call start_processing without additional args
                task_id = await task_queue.add_task(chat_id, start_processing, task_id=str(uuid.uuid4())[:8])
            else:
                if "magnet_link" not in session:
                    await update_progress(chat_id, "❌ Session expired! Please start over.", force_new=True)
                    return
                # For magnet tasks, pass the magnet link as an argument WITHOUT chat_id in kwargs
                magnet_link = session["magnet_link"]
                # 🎯 FIX: Call with proper positional arguments
                task_id = await task_queue.add_task(chat_id, handle_download_and_process, magnet_link, task_id=str(uuid.uuid4())[:8])
        # Update the status message to show task ID
        try:
            await status_msg.edit_text(
                f"✅ **Task Successfully Queued**\n\n"
                f"{queue_msg}\n"
                f"⏰ **Estimated wait:** {estimated_str}\n"
                f"🎯 **Your priority:** {user_priority}\n\n"
                f"📈 **Queue Status:**\n"
                f"• Active tasks: {queue_status['active_tasks_count']}\n"
                f"• Waiting in queue: {queue_status['queue_size']}\n"
                f"• Total queue wait: {humanize.naturaldelta(queue_status['queue_wait_time'])}\n\n"
                f"🔄 You'll be notified when processing starts!",
                reply_markup=InlineKeyboardMarkup([
                    [
                        InlineKeyboardButton("📊 View Queue", callback_data="refresh_queue_enhanced"),
                        InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_my_tasks_confirm_{chat_id}")
                    ],
                    [InlineKeyboardButton("❌ Close", callback_data="close_queue")]
                ])
            )
        except Exception:
            pass
        # Auto-delete queue status after 15 seconds if not closed
        await asyncio.sleep(15)
        try:
            await status_msg.delete()
        except:
            pass
    except Exception as e:
        logger.error(f"Enhanced confirm download error: {str(e)}")
        try:
            await query.answer("Failed to start processing!", show_alert=True)
        except Exception:
            logger.warning("Could not answer callback query - might be expired")
async def start_all_qualities_processing(chat_id: int, task_id: str = None, **kwargs):
    """Main function to handle All Qualities processing for ALL FILES - FIXED VERSION"""
    # Accept **kwargs to handle extra parameters from queue
    try:
        session = await get_user_settings(chat_id)
        if not session:
            await update_progress(chat_id, "❌ Session expired! Please start over.", force_new=True)
            return
        # Get selected qualities
        selected_qualities = session.get("selected_qualities", [])
        logger.info(f"🎯 Starting All Qualities from queue for {chat_id}: {selected_qualities}")
        # 🎯 FIX: Ensure original_settings is always a dict, not None
        original_settings = session.get("original_settings", {})
        if original_settings is None:
            original_settings = {}
            logger.warning(f"original_settings was None for {chat_id}, initialized as empty dict")
        # Store original settings for restoration if not already set
        if not original_settings:
            original_settings = {
                "quality": session.get("quality", "720p"),
                "watermark": session.get("watermark"),
                "watermark_enabled": session.get("watermark_enabled", False),
                "thumbnail": session.get("thumbnail"),
                "upload_mode": session.get("upload_mode", "video"),
                "metadata": session.get("metadata", {}),
                "remname": session.get("remname"),
                "codec": session.get("codec", "h264"),
                "subtitle_mode": session.get("subtitle_mode", "keep"),
                "subtitle_file": session.get("subtitle_file"),
                "audio_mode": session.get("audio_mode", "keep"),
                "audio_file": session.get("audio_file"),
                "custom_crf": session.get("custom_crf"),
                "preset": session.get("preset", "veryfast"),
                "video_tune": session.get("video_tune", "none"),  # 🆕 ADD THIS
                "watermark_position": session.get("watermark_position", "bottom-right"),
                "watermark_scale": session.get("watermark_scale", "10%"),
                "watermark_opacity": session.get("watermark_opacity", "70%"),
                "samples_enabled": session.get("samples_enabled", False),
                "screenshots_enabled": session.get("screenshots_enabled", False)
            }
        await update_user_settings(chat_id, {
            "all_qualities": True,
            "original_settings": original_settings,
            "status": "processing_all_qualities"
        })
        # Handle different task types
        task_type = session.get("task_type", "magnet")
        all_files = []  # 🎯 FIX: Store ALL files, not just one
        download_dir = None
        # 🎯 CANCEL BUTTON for All Qualities
        cancel_button = InlineKeyboardMarkup([[
            InlineKeyboardButton("​🇨​​🇱​​🇴​​🇸​​🇪​", callback_data="close_menu")

        ]])
        if task_type == "compress":
            # 🎯 FOR COMPRESS TASKS: DOWNLOAD FILE NOW
            if "compress_file_info" not in session:
                await update_progress(chat_id, "❌ No file to process! Please use /compress again.", force_new=True)
                return
            # 📥 Show queue message (and make it vanish after 5s)
            progress_msg = await update_progress(
                chat_id,
                "📥 **Downloading File (Your turn in queue)**\n\n",
                #"Please wait while your file is being downloaded for All Qualities processing...",
                force_new=True,
                reply_markup=cancel_button
            )
            await asyncio.sleep(1)
            try:
                if hasattr(progress_msg, "delete"):
                    await progress_msg.delete()
                else:
                    await bot.delete_messages(chat_id, progress_msg.message_id)
            except Exception as e:
                logger.warning(f"Failed to delete compress queue message: {e}")
            # ⬇️ Continue with actual file download
            file_path = await download_compress_file_when_ready(chat_id)
            if not file_path:
                await update_progress(chat_id, "❌ File download failed! Please try /compress again.", force_new=True)
                return
            all_files = [{
                'path': file_path,
                'size': ospath.getsize(file_path),
                'name': ospath.basename(file_path),
                'is_video': True
            }]
            # For magnet tasks, download torrent and get ALL files
        else:
            # 📥 Show queue message (and make it vanish after 5s)
            progress_msg = await update_progress(
                chat_id,
                "📥 **Downloading Torrent (Your turn in queue)**\n\n",
              #  "Starting torrent download for All Qualities processing...",
                force_new=True,
                reply_markup=cancel_button
            )
            await asyncio.sleep(1)
            try:
                if hasattr(progress_msg, "delete"):
                    await progress_msg.delete()
                else:
                    await bot.delete_messages(chat_id, progress_msg.message_id)
            except Exception as e:
                logger.warning(f"Failed to delete torrent queue message: {e}")
            # 🚀 Begin actual torrent download
            download_dir, files = await handle_torrent_download(chat_id, session["magnet_link"])
            all_files = [f for f in files if f.get('is_video')]
            if not all_files:
                raise ValueError("No video files found in torrent for processing")

        # 🎯 FIX: Use process_batch_all_qualities to handle ALL files
        processed_paths = await process_batch(
            chat_id=chat_id,
            files=all_files,
            quality="all_qualities",  # Or use a specific quality that makes sense
            metadata=session.get("metadata", {}),
            watermark_path=session.get("watermark"),
            watermark_enabled=session.get("watermark_enabled", False),
            thumbnail_path=session.get("thumbnail"),
            upload_mode=session.get("upload_mode", "video"),
            codec=session.get("codec", "h264"),
            remname=session.get("remname"),
            subtitle_mode=session.get("subtitle_mode", "keep"),
            subtitle_file=session.get("subtitle_file"),
            audio_mode=session.get("audio_mode", "keep"),
            audio_file=session.get("audio_file"),
            custom_crf=session.get("custom_crf"),
            preset=session.get("preset", "veryfast"),
            samples_enabled=session.get("samples_enabled", False),
            screenshots_enabled=session.get("screenshots_enabled", False),
            video_tune=session.get("video_tune", "none"),  # 🆕 ADD THIS
            watermark_position=session.get("watermark_position", "bottom-right"),
            watermark_scale=session.get("watermark_scale", "10%"),
            watermark_opacity=session.get("watermark_opacity", "70%"),
            task_id=task_id,
            is_all_qualities=True,
            selected_qualities=selected_qualities
        )

        # 🎯 FIX: ADD QUALITY TAG TO ALL QUALITIES FILES
        if processed_paths:
            for processed_file in processed_paths:
                encoded_path = processed_file.get("path")
                file_quality = processed_file.get("quality")
                if encoded_path and ospath.exists(encoded_path) and file_quality:
                    try:
                        # Ensure quality tag is present
                        new_path = await ensure_quality_tag_in_filename(encoded_path, file_quality)  # ✅ FIXED: Added await
                        processed_file["path"] = new_path
                        processed_file["name"] = ospath.basename(new_path)
                        logger.info(f"🔧 Ensured quality tag for {file_quality}: {ospath.basename(new_path)}")
                    except Exception as e:
                        logger.warning(f"Failed to ensure quality tag for {encoded_path}: {str(e)}")
    except TaskCancelled:
        logger.info(f"All qualities processing cancelled by user {chat_id}")
        await update_progress(chat_id, "❌ All qualities processing cancelled by user", force_new=True)
    except Exception as e:
        logger.error(f"All qualities failed: {str(e)}", exc_info=True)
        await update_progress(
            chat_id,
            f"❌ All Qualities Processing Failed\n\n"
            f"📄 **Error:** {str(e)[:100]}{'...' if len(str(e)) > 100 else ''}\n\n"
            f"Please try again with different settings.",
            force_new=True
        )
    finally:
        # 🎯 FIX: Properly restore original settings
        try:
            session = await get_user_settings(chat_id)
            if session and "original_settings" in session:
                original_settings = session.get("original_settings")
                if original_settings and isinstance(original_settings, dict):
                    await update_user_settings(chat_id, original_settings)
                # Clear flags
                await update_user_settings(chat_id, {
                    "all_qualities": False,
                    "original_settings": {},
                    "selected_qualities": []
                })
        except Exception as e:
            logger.error(f"Error restoring settings in All Qualities: {str(e)}")
        # Cleanup
        for path in [download_dir]:
            if path and ospath.exists(path):
                try:
                    shutil.rmtree(path, ignore_errors=True)
                    logger.info(f"🧹 Cleaned up download directory: {path}")
                except Exception as e:
                    logger.warning(f"Failed to cleanup {path}: {str(e)}")
        # Cleanup generated files
        await cleanup_generated_files(chat_id)
@bot.on_callback_query(filters.regex(r"^cancel_all_qualities_(\d+)$"))
async def cancel_all_qualities_handler(client: Client, query: CallbackQuery):
    """Handle cancellation of All Qualities processing"""
    try:
        chat_id = int(query.data.split("_")[3])
        # Set global cancellation flag
        user_event.set()
        # Cancel current task in global queue
        await global_queue_manager.cancel_current_task()
        await query.answer("🛑 Cancelling All Qualities processing...")
        # Update progress message
        await query.message.edit_text(
            "❌ **All Qualities Processing Cancelled**\n\n"
            "The multi-quality encoding has been stopped.\n"
            "You can start a new task whenever you're ready."
        )
        logger.info(f"All Qualities processing cancelled by user {chat_id}")
    except Exception as e:
        logger.error(f"Cancel All Qualities error: {str(e)}")
        await query.answer("Failed to cancel processing!", show_alert=True)

async def start_processing(chat_id: int, task_id: str = None):
    """Start processing with global queue - handles both compress and magnet tasks"""
    session = None
    processing_started = False
    process_start_time = time.time()
    try:
        logger.info(f"🚀 start_processing called for chat_id: {chat_id}, task_id: {task_id}")
        # 🎯 CHECK FOR ALL QUALITIES MODE FIRST
        session = await get_user_settings(chat_id)
        is_all_qualities = session.get("all_qualities", False)
        selected_qualities = session.get("selected_qualities")
        if is_all_qualities and selected_qualities:
            logger.info(f"🔄 All Qualities mode detected for {chat_id}, selected qualities: {selected_qualities}")
        await cleanup_progress_messages(chat_id)
        if not session:
            await update_progress(chat_id, "❌ Session expired", force_new=True)
            return
        # Get files based on task type
        files = []
        task_type = session.get("task_type", "magnet")
        logger.info(f"📋 Task type: {task_type}, Status: {session.get('status')}, All Qualities: {is_all_qualities}")
        # Check for files in multiple possible locations
        if task_type == "compress":
            # 🎯 FOR COMPRESS TASKS: DOWNLOAD FILE NOW (WHEN IT'S THEIR TURN IN QUEUE)
            if "compress_file_info" not in session:
                await update_progress(chat_id, "❌ No file to process! Please use /compress again.", force_new=True)
                return
            # 🎯 SHOW DOWNLOAD PROGRESS - THIS HAPPENS WHEN IT'S THEIR TURN IN QUEUE
            await update_progress(
                chat_id, 
                "📥 **Downloading File (Your turn in queue)**\n\n",
               # "Please wait while your file is being downloaded...",
                force_new=True
            )
            await asyncio.sleep(1)
            # 🎯 DOWNLOAD THE FILE NOW - WHEN IT'S ACTUALLY THEIR TURN
            file_path = await download_compress_file_when_ready(chat_id)
            if not file_path:
                await update_progress(chat_id, "❌ File download failed! Please try /compress again.", force_new=True)
                return
            files = [{
                'path': file_path,
                'size': ospath.getsize(file_path),
                'name': ospath.basename(file_path),
                'is_video': True
            }]
            logger.info(f"✅ Downloaded compress file for processing: {file_path}")
        else:
            # For magnet tasks, use the torrent download files
            if session.get("status") == "downloaded" and session.get("file_path"):
                if ospath.exists(session["file_path"]):
                    files = [{
                        'path': session["file_path"],
                        'size': ospath.getsize(session["file_path"]),
                        'name': ospath.basename(session["file_path"]),
                        'is_video': True
                    }]
                    logger.info(f"✅ Found magnet file: {session['file_path']}")
            elif session.get("status") == "batch_downloaded" and session.get("files"):
                files = [f for f in session["files"] if isinstance(f, dict) and f.get('path') and ospath.exists(f['path'])]
                logger.info(f"✅ Found {len(files)} magnet files via files list")
        if not files:
            logger.error("❌ No valid files to process after all checks")
            await update_progress(chat_id, "❌ No valid files found to process. Please try again.", force_new=True)
            return
        # MARK: Processing is starting now
        processing_started = True
        logger.info(f"🟢 Processing STARTED for {chat_id} with {len(files)} files, All Qualities: {is_all_qualities}")
        # Update status to show processing has started
        await update_user_settings(chat_id, {"status": "processing"})
        codec = session.get("codec", "h264")
        preset = session.get("preset", "veryfast")
        watermark_enabled = session.get("watermark_enabled", False)
        quality = session.get("quality", "720p")
        samples_enabled = session.get("samples_enabled", False)
        screenshots_enabled = session.get("screenshots_enabled", False)
        # 🎯 For All Qualities mode, show different message
        if is_all_qualities and selected_qualities:
            selected_text = ", ".join(selected_qualities)
            await update_progress(
                chat_id,
                f"🚀 Starting All Qualities Processing\n\n"
                f"📂 Files: {len(files)}\n"
                f"🎯 Qualities: {selected_text}\n"
                f"🔤 Codec: {codec.upper()}\n"
                f"⚡ Preset: {preset.upper()}\n"
                f"💧 Watermark: {'Yes' if watermark_enabled else 'No'}",
                force_new=True,
                start_time=process_start_time
            )
        elif quality == "noencode":
            await update_progress(
                chat_id,
                f"🚀 Starting Direct Upload\n\n"
                f"📂 Files: {len(files)}\n"
                f"⚡ Mode: No Encoding\n"
                f"🎬 Samples: {'Yes' if samples_enabled else 'No'}\n"
                f"📸 Screenshots: {'Yes' if screenshots_enabled else 'No'}",
                force_new=True,
                start_time=process_start_time
            )
        else:
            # For encoding tasks, process_batch will handle the progress message
            # So we don't send a duplicate message here
            pass
        # Add a small delay to ensure the message is sent
        await asyncio.sleep(1)
        # Call process_batch with All Qualities parameters if needed
        if is_all_qualities and selected_qualities:
            await process_batch(
                chat_id=chat_id,
                files=files,
                quality=session.get("quality", "720p"),
                metadata=session.get("metadata", {}),
                watermark_path=session.get("watermark"),
                watermark_enabled=session.get("watermark_enabled", False),
                thumbnail_path=session.get("thumbnail"),
                upload_mode=session.get("upload_mode", "video"),
                codec=session.get("codec", "h264"),
                remname=session.get("remname"),
                subtitle_mode=session.get("subtitle_mode", "keep"),
                subtitle_file=session.get("subtitle_file"),
                audio_mode=session.get("audio_mode", "keep"),
                audio_file=session.get("audio_file"),
                custom_crf=session.get("custom_crf"),
                preset=session.get("preset", "veryfast"),
                samples_enabled=session.get("samples_enabled", False),
                screenshots_enabled=session.get("screenshots_enabled", False),
                video_tune=session.get("video_tune", "none"),  # 🆕 ADD THIS
                watermark_position=session.get("watermark_position", "bottom-right"),
                watermark_scale=session.get("watermark_scale", "10%"),
                watermark_opacity=session.get("watermark_opacity", "70%"),
                task_id=task_id,
                is_all_qualities=True,  # 🆕 Pass All Qualities flag
                selected_qualities=selected_qualities  # 🆕 Pass selected qualities
            )
        else:
            # Normal single quality processing
            await process_batch(
                chat_id=chat_id,
                files=files,
                quality=session.get("quality", "720p"),
                metadata=session.get("metadata", {}),
                watermark_path=session.get("watermark"),
                watermark_enabled=session.get("watermark_enabled", False),
                thumbnail_path=session.get("thumbnail"),
                upload_mode=session.get("upload_mode", "video"),
                codec=session.get("codec", "h264"),
                remname=session.get("remname"),
                subtitle_mode=session.get("subtitle_mode", "keep"),
                subtitle_file=session.get("subtitle_file"),
                audio_mode=session.get("audio_mode", "keep"),
                audio_file=session.get("audio_file"),
                custom_crf=session.get("custom_crf"),
                preset=session.get("preset", "veryfast"),
                samples_enabled=session.get("samples_enabled", False),
                screenshots_enabled=session.get("screenshots_enabled", False),
                task_id=task_id,
                video_tune=session.get("video_tune", "none"),  # 🆕 ADD THIS
                watermark_position=session.get("watermark_position", "bottom-right"),
                watermark_scale=session.get("watermark_scale", "10%"),
                watermark_opacity=session.get("watermark_opacity", "70%")
            )
        # PROCESSING COMPLETED SUCCESSFULLY
        logger.info(f"🎉 Processing COMPLETED successfully for {chat_id}")
    except TaskCancelled:
        logger.info(f"🛑 Processing CANCELLED by user {chat_id}")
        await update_progress(chat_id, "❌ Processing cancelled by user", force_new=True)
    except Exception as e:
        logger.error(f"💥 Processing FAILED for {chat_id}: {str(e)}", exc_info=True)
        await update_progress(chat_id, f"❌ Failed to start processing: {str(e)}", force_new=True)
    finally:
        # ONLY cleanup if processing actually started
        if processing_started:
            try:
                if session:
                    task_type = session.get("task_type", "magnet")
                    if task_type == "compress":
                        await cleanup_compress_files(chat_id)
                        logger.info(f"🧹 Cleaned compress files for {chat_id} after processing")
                    else:
                        if "download_path" in session and ospath.exists(session["download_path"]):
                            shutil.rmtree(session["download_path"], ignore_errors=True)
                            logger.info(f"🧹 Cleaned magnet download path for {chat_id}")
                        if "file_path" in session and ospath.exists(session["file_path"]):
                            osremove(session["file_path"])
                            logger.info(f"🧹 Cleaned magnet file for {chat_id}")
                # NEW: Clean up generated files (screenshots, samples, encoded files, etc.)
                await cleanup_generated_files(chat_id)
                logger.info(f"🧹 Cleaned generated files for {chat_id} after processing")
            except Exception as e:
                logger.error(f"🧹 Final cleanup error: {str(e)}")
        else:
            logger.info(f"⏸️  Skipping cleanup - processing never started for {chat_id}")
async def cleanup_compress_files(chat_id: int):
    """Cleanup compress download files for a user - FIXED TIMING"""
    try:
        session = await get_user_settings(chat_id)
        if session:
            logger.info(f"🧹 Starting compress cleanup for {chat_id}")
            # Clean specific compress download path
            if session.get("download_path") and "compress_" in session["download_path"]:
                if ospath.exists(session["download_path"]):
                    shutil.rmtree(session["download_path"], ignore_errors=True)
                    logger.info(f"🧹 Cleaned compress download path: {session['download_path']}")
            # Clean individual file
            if session.get("file_path") and ospath.exists(session["file_path"]):
                try:
                    osremove(session["file_path"])
                    logger.info(f"🧹 Cleaned compress file: {session['file_path']}")
                except Exception as e:
                    logger.warning(f"Failed to delete file {session['file_path']}: {str(e)}")
            # 🎯 CLEAR compress file info from session AFTER cleanup
            await update_user_settings(chat_id, {
                "compress_file_info": None,
                "file_path": None,
                "download_path": None,
                "files": None
            })
        logger.info(f"✅ Compress files cleanup completed for {chat_id}")
    except Exception as e:
        logger.error(f"❌ Cleanup compress files error: {str(e)}")
@bot.on_message(filters.private)
@authorized_only
async def handle_user_input(client: Client, message: Message):
    """Handle user input for settings with proper validation - FIXED VERSION"""
    try:
        chat_id = message.chat.id
        
        # 🎯 FIX: Check if message has text content safely
        if message.text:
            text = message.text.strip()
        else:
            text = None
        
        session = await get_user_settings(chat_id)
        
        # 🎯 FIX: Ignore if message is a command (starts with /)
        if message.text and message.text.startswith('/'):
            return  # Let the command handlers process this
            
        # If no session or not awaiting anything, ignore normal messages
        if not session or "awaiting" not in session or session["awaiting"] is None:
            return  # Ignore normal messages
            
        awaiting = session["awaiting"]
        
        # Handle different input types based on what we're awaiting
        if awaiting == "title" and message.text:
            clean_title = re.sub(r'[^\w\-_\. ]', '', message.text)
            await update_user_settings(chat_id, {
                "metadata.title": clean_title,
                "awaiting": None
            })
            await cleanup_user_messages(chat_id)
            await message.reply(f"✅ METADATA set: {clean_title}")
            return await collect_settings(chat_id)

        elif awaiting == "crf" and message.text:
            await handle_crf_input(chat_id, text, message)
            return    
            
            
        elif awaiting == "torrent_limit" and message.text:
            try:
                if message.text.startswith('/'):
                    await update_user_settings(chat_id, {"awaiting": None})
                    await message.reply("✅ Torrent limit input cancelled.")
                    return await admin_panel_handler(client, message)
                gb_value = float(message.text.strip())
                if 1 <= gb_value <= 500:
                    global TORRENT_SIZE_LIMIT
                    TORRENT_SIZE_LIMIT = int(gb_value * 1024 * 1024 * 1024)
                    await save_size_limits()
                    await update_user_settings(chat_id, {"awaiting": None})
                    await message.reply(f"✅ Torrent size limit set to {gb_value}GB ({humanize.naturalsize(TORRENT_SIZE_LIMIT)})")
                    return await size_limits_menu_handler(client, message)
                else:
                    await message.reply("❌ Please enter a value between 1 and 500 GB.")
            except ValueError:
                await message.reply("❌ Please enter a valid number (1-500).")
                
        elif awaiting == "file_limit" and message.text:
            try:
                if message.text.startswith('/'):
                    await update_user_settings(chat_id, {"awaiting": None})
                    await message.reply("✅ File limit input cancelled.")
                    return await admin_panel_handler(client, message)
                gb_value = float(message.text.strip())
                if 1 <= gb_value <= 4:
                    global SINGLE_FILE_SIZE_LIMIT
                    SINGLE_FILE_SIZE_LIMIT = int(gb_value * 1024 * 1024 * 1024)
                    await save_size_limits()
                    await update_user_settings(chat_id, {"awaiting": None})
                    await message.reply(f"✅ Single file size limit set to {gb_value}GB ({humanize.naturalsize(SINGLE_FILE_SIZE_LIMIT)})")
                    return await size_limits_menu_handler(client, message)
                else:
                    await message.reply("❌ Please enter a value between 1 and 4 GB.")
            except ValueError:
                await message.reply("❌ Please enter a valid number (1-4).")
                
        elif awaiting == "add_premium_user" and message.text:
            try:
                if message.text.startswith('/'):
                    await update_user_settings(chat_id, {"awaiting": None})
                    await message.reply("✅ Premium addition cancelled.")
                    return await premium_management_handler(client, message)
                target_user_id = int(message.text.strip())
                await update_user_settings(chat_id, {
                    "awaiting": "add_premium_duration",
                    "temp_premium_user": target_user_id
                })
                await message.reply(
                    f"👤 <b>User ID:</b> <code>{target_user_id}</code>\n\n"
                    "Now send the duration:\n"
                    "• <code>24h</code> - 24 hours\n"
                    "• <code>7d</code> - 7 days\n"
                    "• <code>30d</code> - 30 days\n"
                    "• <code>90d</code> - 90 days\n"
                    "• <code>365d</code> - 1 year",
                    parse_mode=ParseMode.HTML
                )
            except ValueError:
                await message.reply("❌ Please enter a valid user ID (numbers only).")
                
        elif awaiting == "add_premium_duration" and message.text:
            try:
                if message.text.startswith('/'):
                    await update_user_settings(chat_id, {"awaiting": None})
                    await message.reply("✅ Premium addition cancelled.")
                    return await premium_management_handler(client, message)
                duration_str = message.text.strip().lower()
                target_user_id = (await get_user_settings(chat_id)).get("temp_premium_user")
                if not target_user_id:
                    await update_user_settings(chat_id, {"awaiting": None})
                    await message.reply("❌ Session expired. Please start over.")
                    return await premium_management_handler(client, message)
                # Parse duration
                if duration_str.endswith('h'):
                    hours = int(duration_str[:-1])
                    seconds = hours * 3600
                    duration_display = f"{hours} hours"
                elif duration_str.endswith('d'):
                    days = int(duration_str[:-1])
                    seconds = days * 86400
                    duration_display = f"{days} days"
                else:
                    await message.reply("❌ Invalid duration format. Use 'h' for hours or 'd' for days.")
                    return
                await update_user_settings(chat_id, {
                    "awaiting": "add_premium_plan",
                    "temp_premium_user": target_user_id,
                    "temp_premium_duration": seconds
                })
                await message.reply(
                    f"⏰ <b>Duration:</b> {duration_display}\n\n"
                    "Now send the plan name:\n"
                    "• <code>Basic</code>\n"
                    "• <code>Pro</code>\n"
                    "• <code>Ultimate</code>\n"
                    "• <code>Trial</code>\n"
                    "• Or any custom name",
                    parse_mode=ParseMode.HTML
                )
            except ValueError:
                await message.reply("❌ Please enter a valid duration (e.g., 30d or 24h).")
                
        elif awaiting == "add_premium_plan" and message.text:
            try:
                if message.text.startswith('/'):
                    await update_user_settings(chat_id, {"awaiting": None})
                    await message.reply("✅ Premium addition cancelled.")
                    return await premium_management_handler(client, message)
                plan_name = message.text.strip()
                target_user_id = (await get_user_settings(chat_id)).get("temp_premium_user")
                duration_seconds = (await get_user_settings(chat_id)).get("temp_premium_duration")
                if not target_user_id or not duration_seconds:
                    await update_user_settings(chat_id, {"awaiting": None})
                    await message.reply("❌ Session expired. Please start over.")
                    return await premium_management_handler(client, message)
                # Add premium
                expiry_time = time.time() + duration_seconds
                expiry_date = datetime.fromtimestamp(expiry_time)
                premium_data = {
                    "expiry": expiry_time,
                    "added_by": chat_id,
                    "plan": plan_name,
                    "added_date": datetime.now().isoformat()
                }
                PREMIUM_USERS[target_user_id] = premium_data
                await save_premium_user(target_user_id, premium_data)
                # Clear temp data
                await update_user_settings(chat_id, {
                    "awaiting": None,
                    "temp_premium_user": None,
                    "temp_premium_duration": None
                })
                await message.reply(
                    f"✅ <b>Premium Added Successfully!</b>\n\n"
                    f"👤 <b>User ID:</b> <code>{target_user_id}</code>\n"
                    f"💎 <b>Plan:</b> {plan_name}\n"
                    f"📅 <b>Expires:</b> {expiry_date.strftime('%Y-%m-%d %H:%M:%S')}\n"
                    f"👑 <b>Added by:</b> {message.from_user.mention}",
                    parse_mode=ParseMode.HTML
                )
                return await premium_management_handler(client, message)
            except Exception as e:
                await message.reply(f"❌ Error adding premium: {str(e)}")
                
        elif awaiting == "remove_premium_user" and message.text:
            try:
                if message.text.startswith('/'):
                    await update_user_settings(chat_id, {"awaiting": None})
                    await message.reply("✅ Premium removal cancelled.")
                    return await premium_management_handler(client, message)
                target_user_id = int(message.text.strip())
                if target_user_id not in PREMIUM_USERS:
                    await message.reply(f"❌ User <code>{target_user_id}</code> is not a premium user.", parse_mode=ParseMode.HTML)
                    return
                await revoke_premium(target_user_id)
                await update_user_settings(chat_id, {"awaiting": None})
                await message.reply(
                    f"✅ <b>Premium Removed Successfully!</b>\n\n"
                    f"👤 <b>User ID:</b> <code>{target_user_id}</code>\n"
                    f"👑 <b>Removed by:</b> {message.from_user.mention}",
                    parse_mode=ParseMode.HTML
                )
                return await premium_management_handler(client, message)
            except ValueError:
                await message.reply("❌ Please enter a valid user ID (numbers only).")
            except Exception as e:
                await message.reply(f"❌ Error removing premium: {str(e)}")
                
        # 🎯 FIXED: Handle media inputs (photos/documents)
        elif awaiting == "thumb" and (message.photo or message.document):
            thumb_path = await handle_thumbnail(chat_id, message)
            if thumb_path:
                await update_user_settings(chat_id, {"awaiting": None})
                await cleanup_user_messages(chat_id)
                await message.reply("✅ Thumbnail saved!")
                return await collect_settings(chat_id)
            else:
                await message.reply("❌ Failed to save thumbnail. Please try again.")
                
        elif awaiting == "wm" and (message.photo or (message.document and message.document.mime_type and "image" in message.document.mime_type)):
            wm_path = await handle_watermark(chat_id, message)
            if wm_path:
                await update_user_settings(chat_id, {"awaiting": None})
                await cleanup_user_messages(chat_id)
                await message.reply("✅ Watermark saved!")
                return await collect_settings(chat_id)
            else:
                await message.reply("❌ Failed to save watermark. Please send a valid image (PNG recommended).")
                
        elif awaiting == "subtitle" and message.document:
            subtitle_path = await handle_subtitle(chat_id, message)
            if subtitle_path:
                await update_user_settings(chat_id, {"awaiting": None})
                await cleanup_user_messages(chat_id)
                await message.reply("✅ Subtitle file saved!")
                return await collect_settings(chat_id)
            else:
                await message.reply("❌ Failed to save subtitle file. Please send a valid subtitle file (SRT, ASS, VTT).")
                
        elif awaiting == "audio" and message.document:
            audio_path = await handle_audio(chat_id, message)
            if audio_path:
                await update_user_settings(chat_id, {"awaiting": None})
                await cleanup_user_messages(chat_id)
                await message.reply("✅ Audio file saved!")
                return await collect_settings(chat_id)
            else:
                await message.reply("❌ Failed to save audio file. Please send a valid audio file.")
                
        elif awaiting == "remname" and message.text:
            pattern = message.text.strip()
            if pattern.startswith('/'):
                await update_user_settings(chat_id, {"awaiting": None})
                await message.reply("✅ REMNAME input cancelled.")
                return await collect_settings(chat_id)
            if not validate_remname_pattern(pattern):
                await message.reply(
                    "❌ Invalid REMNAME pattern!\n\n"
                    "📝 <b>Format Examples:</b>\n"
                    "• <code>| :_|\\s:-</code> (spaces→underscores, colons→hyphens)\n"
                    "• <code>|\\[.*?\\]||\\(.*?\\)</code> (remove text in brackets/parentheses)\n"
                    "• <code>| :_</code> (simple space to underscore)\n\n"
                    "💡 <b>Tip:</b> Use <code>/skip</code> to cancel or try a simpler pattern.",
                    parse_mode=ParseMode.HTML
                )
                return
            await update_user_settings(chat_id, {
                "remname": pattern,
                "awaiting": None
            })
            await cleanup_user_messages(chat_id)
            await message.reply(f"✅ REMNAME pattern set:\n<code>{pattern}</code>", parse_mode=ParseMode.HTML)
            return await collect_settings(chat_id)
            
        # If we're here, the input doesn't match what we're awaiting
        else:
            await update_user_settings(chat_id, {"awaiting": None})
            await message.reply("❌ Unexpected input type. Please use the settings menu to configure options.")
            return await collect_settings(chat_id)
            
    except Exception as e:
        logger.error(f"User input handler error: {str(e)}")
        await message.reply("❌ An error occurred while processing your input. Please try again.")
def validate_remname_pattern(pattern: str) -> bool:
    """Validate REMNAME pattern format"""
    if not pattern:
        return False
    # Basic pattern validation
    if not pattern.startswith('|'):
        pattern = f"|{pattern}"
    # Split by pipes and validate each replacement
    parts = pattern.split('|')[1:]  # Skip first empty part
    for part in parts:
        if not part.strip():
            continue
        # Check if it has at least one colon for replacement
        if ':' not in part:
            return False
        # Split into pattern and replacement
        replacement_parts = part.split(':', 1)
        if len(replacement_parts) < 2:
            return False
        pattern_part = replacement_parts[0]
        if not pattern_part.strip():
            return False
    return True
async def clear_awaiting_state(chat_id: int):
    """Safely clear awaiting state for a user"""
    try:
        session = await get_user_settings(chat_id)
        if session and "awaiting" in session:
            await update_user_settings(chat_id, {"awaiting": None})
            logger.info(f"🧹 Cleared awaiting state for user {chat_id}")
            return True
        return False
    except Exception as e:
        logger.error(f"Error clearing awaiting state: {str(e)}")
        return False
# Update the clear command to be more comprehensive
@bot.on_message(filters.command("clear") & (filters.private | filters.group))
@authorized_only
async def clear_handler(client: Client, message: Message):
    """Clear any pending input state - ENHANCED VERSION"""
    try:
        chat_id = message.chat.id
        # Clear awaiting state
        cleared_awaiting = await clear_awaiting_state(chat_id)
        # Also cleanup any progress messages
        await cleanup_progress_messages(chat_id)
        if cleared_awaiting:
            await message.reply("✅ Input state cleared. You can now use normal commands.")
        else:
            await message.reply("ℹ️ No pending input state to clear.")
    except Exception as e:
        logger.error(f"Clear command error: {str(e)}")
        await message.reply("❌ Error clearing input state.")
@bot.on_callback_query(filters.regex(r"^set_(title|thumb|remname|crf)_(\d+)$"))
async def set_item_handler(client: Client, query: CallbackQuery):
    chat_id = int(query.data.split("_")[2])
    item_type = query.data.split("_")[1]
    session = await get_user_settings(chat_id)
    if not session:
        return await query.answer("Session expired!", show_alert=True)
    await update_user_settings(chat_id, {"awaiting": item_type})
    if item_type == "title":
        text = "📝 Please send the new title for your videos:"
    elif item_type == "thumb":
        text = "🖼️ Please send an image to use as thumbnail (JPEG recommended):"
    elif item_type == "remname":
        text = (
            "🔄 Please send the REMNAME pattern for filename modification\n\n"
            "Format: |pattern1:replacement1[:count1]|pattern2:replacement2...\n"
            "Example: | : |_| |\\s:_| (replaces spaces with underscores and colons with pipes)"
        )
    elif item_type == "crf":
        text = (
            "🎛️ Please send the CRF value (0-51)\n\n"
            "Lower values = better quality, larger files\n"
            "Higher values = lower quality, smaller files\n"
            "Recommended: 18-28 for H.264, 22-32 for H.265]\n"
            "Enter a number between 0-51, or use Skip for auto CRF:"
        )
    buttons = [
        [InlineKeyboardButton("❌ Skip", callback_data=f"skip_{item_type}_{chat_id}")],
        [InlineKeyboardButton("🔙 Back to Settings", callback_data=f"settings_menu_{chat_id}")]
    ]
    await query.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    )
    await query.answer()
@bot.on_callback_query(filters.regex(r"^toggle_watermark_(\d+)$"))
async def toggle_watermark_handler(client: Client, query: CallbackQuery):
    """Toggle watermark enabled/disabled status"""
    try:
        chat_id = int(query.data.split("_")[2])
        session = await get_user_settings(chat_id)
        if not session:
            return await query.answer("Session expired!", show_alert=True)
        # Check if watermark file exists
        has_watermark = session.get("watermark") and ospath.exists(session["watermark"])
        if not has_watermark:
            await query.answer("❌ No watermark file found! Please upload a watermark first.", show_alert=True)
            return
        # Toggle watermark status
        current_status = session.get("watermark_enabled", False)
        new_status = not current_status
        await update_user_settings(chat_id, {"watermark_enabled": new_status})
        if new_status:
            await query.answer("✅ Watermark ENABLED - Will be added to encoded videos")
        else:
            await query.answer("🔴 Watermark DISABLED - Will not be added to videos")
        # Refresh the watermark settings menu
        await set_watermark_handler(client, query)
    except Exception as e:
        logger.error(f"Toggle watermark error: {str(e)}")
        await query.answer("Failed to toggle watermark!", show_alert=True)
@bot.on_callback_query(filters.regex(r"^set_wm_(\d+)$"))
async def set_watermark_handler(client: Client, query: CallbackQuery):
    """Enhanced watermark management with position, scale, and opacity controls"""
    try:
        chat_id = int(query.data.split("_")[2])
        session = await get_user_settings(chat_id)
        if not session:
            return await query.answer("Session expired!", show_alert=True)
        # Get current watermark settings
        current_position = session.get("watermark_position", "bottom-right")
        current_scale = session.get("watermark_scale", "10%")
        current_opacity = session.get("watermark_opacity", "70%")
        wm_enabled = session.get("watermark_enabled", False)
        has_watermark = session.get("watermark") and ospath.exists(session["watermark"])
        # Create comprehensive watermark management menu
        caption = "💧 **Advanced Watermark Management**\n\n"
        if has_watermark:
            caption += (
                f"✅ **Watermark File:** `{ospath.basename(session['watermark'])}`\n"
                f"📍 **Position:** {WATERMARK_POSITIONS.get(current_position, current_position)}\n"
                f"📐 **Scale:** {WATERMARK_SCALES.get(current_scale, current_scale)}\n"
                f"🎨 **Opacity:** {WATERMARK_OPACITY.get(current_opacity, current_opacity)}\n"
                f"🔧 **Status:** {'🟢 ENABLED' if wm_enabled else '🔴 DISABLED'}\n\n"
                "💡 *Configure your watermark appearance below:*"
            )
        else:
            caption += (
                "📁 **No Watermark File Set**\n\n"
                "Upload a PNG image to use as watermark:\n"
                "• Transparent background recommended\n" 
                "• PNG format works best\n"
                "• Will be auto-resized and positioned\n\n"
                "Click 'Upload Watermark' to get started!"
            )
        # Create button grid
        buttons = []
        if has_watermark:
            # First row: Status toggle
            buttons.append([
                InlineKeyboardButton(
                    f"{'🔴 Disable' if wm_enabled else '🟢 Enable'} Watermark", 
                    callback_data=f"toggle_watermark_{chat_id}"
                )
            ])
            # Second row: Position settings
            buttons.append([
                InlineKeyboardButton("📍 Position", callback_data=f"wm_position_menu_{chat_id}"),
                InlineKeyboardButton("📐 Scale", callback_data=f"wm_scale_menu_{chat_id}")
            ])
            # Third row: Opacity and advanced
            buttons.append([
                InlineKeyboardButton("🎨 Opacity", callback_data=f"wm_opacity_menu_{chat_id}"),
                InlineKeyboardButton("🔄 Change", callback_data=f"change_wm_{chat_id}")
            ])
            # Fourth row: Delete and preview
            buttons.append([
                InlineKeyboardButton("👁️ Preview", callback_data=f"preview_wm_{chat_id}"),
                InlineKeyboardButton("🗑️ Delete", callback_data=f"delete_wm_{chat_id}")
            ])
        else:
            # No watermark - just upload option
            buttons.append([
                InlineKeyboardButton("📤 Upload Watermark", callback_data=f"upload_wm_{chat_id}")
            ])
        # Always include back button
        buttons.append([
            InlineKeyboardButton("🔙 Back to Settings", callback_data=f"settings_menu_{chat_id}")
        ])
        await query.message.edit_text(
            caption,
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        await query.answer()
    except Exception as e:
        logger.error(f"Set watermark error: {str(e)}")
        await query.answer("Failed to load watermark settings!", show_alert=True)
@bot.on_callback_query(filters.regex(r"^wm_position_menu_(\d+)$"))
async def watermark_position_menu_handler(client: Client, query: CallbackQuery):
    """Watermark position selection menu"""
    try:
        chat_id = int(query.data.split("_")[3])
        buttons = [
            [
                InlineKeyboardButton("↖️ Top Left", callback_data=f"wm_pos_tl_{chat_id}"),
                InlineKeyboardButton("↗️ Top Right", callback_data=f"wm_pos_tr_{chat_id}")
            ],
            [
                InlineKeyboardButton("↙️ Bottom Left", callback_data=f"wm_pos_bl_{chat_id}"),
                InlineKeyboardButton("↘️ Bottom Right", callback_data=f"wm_pos_br_{chat_id}")
            ],
            [
                InlineKeyboardButton("⏺️ Center", callback_data=f"wm_pos_center_{chat_id}"),
                InlineKeyboardButton("🎲 Random", callback_data=f"wm_pos_random_{chat_id}")
            ],
            [
                InlineKeyboardButton("🔙 Back", callback_data=f"set_wm_{chat_id}")
            ]
        ]
        await query.message.edit_text(
            "📍 **Select Watermark Position**\n\n"
            "Choose where to place the watermark:\n\n"
            "• **Corners**: Traditional placement\n"
            "• **Center**: Over the main content\n" 
            "• **Random**: Changes position every 10 seconds (anti-piracy)\n\n"
            "💡 *Recommended: Bottom Right for minimal intrusion*",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        await query.answer()
    except Exception as e:
        logger.error(f"Watermark position menu error: {str(e)}")
        await query.answer("Failed to load position menu!", show_alert=True)
@bot.on_callback_query(filters.regex(r"^wm_pos_(tl|tr|bl|br|center|random)_(\d+)$"))
async def watermark_position_handler(client: Client, query: CallbackQuery):
    """Handle watermark position selection with proper full names"""
    try:
        position_abbr = query.data.split("_")[2]
        chat_id = int(query.data.split("_")[3])
        # 🎯 MAP ABBREVIATIONS TO FULL NAMES FOR STORAGE
        position_map = {
            'tl': 'top-left',
            'tr': 'top-right', 
            'bl': 'bottom-left',
            'br': 'bottom-right',
            'center': 'center',
            'random': 'random'
        }
        selected_position = position_map.get(position_abbr, 'bottom-right')
        await update_user_settings(chat_id, {"watermark_position": selected_position})
        await query.answer(f"📍 Position set to {WATERMARK_POSITIONS[selected_position]}")
        await set_watermark_handler(client, query)
    except Exception as e:
        logger.error(f"Watermark position error: {str(e)}")
        await query.answer("Failed to set position!", show_alert=True)
@bot.on_callback_query(filters.regex(r"^wm_scale_menu_(\d+)$"))
async def watermark_scale_menu_handler(client: Client, query: CallbackQuery):
    """Watermark scale selection menu"""
    try:
        chat_id = int(query.data.split("_")[3])
        buttons = [
            [
                InlineKeyboardButton("🔘 5% - Very Small", callback_data=f"wm_scale_5%_{chat_id}"),
                InlineKeyboardButton("🔘 10% - Small", callback_data=f"wm_scale_10%_{chat_id}")
            ],
            [
                InlineKeyboardButton("🔘 15% - Medium", callback_data=f"wm_scale_15%_{chat_id}"),
                InlineKeyboardButton("🔘 20% - Large", callback_data=f"wm_scale_20%_{chat_id}")
            ],
            [
                InlineKeyboardButton("🔘 25% - Very Large", callback_data=f"wm_scale_25%_{chat_id}")
            ],
            [
                InlineKeyboardButton("🔙 Back", callback_data=f"set_wm_{chat_id}")
            ]
        ]
        await query.message.edit_text(
            "📐 **Select Watermark Size**\n\n"
            "Choose how large the watermark should be:\n\n"
            "• **5%**: Very subtle, barely noticeable\n"
            "• **10%**: Small but readable (recommended)\n"
            "• **15%**: Clearly visible\n"
            "• **20%**: Prominent placement\n"
            "• **25%**: Very large and obvious\n\n"
            "💡 *Recommended: 10-15% for balance*",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        await query.answer()
    except Exception as e:
        logger.error(f"Watermark scale menu error: {str(e)}")
        await query.answer("Failed to load scale menu!", show_alert=True)
@bot.on_callback_query(filters.regex(r"^wm_scale_(\d+%)_(\d+)$"))
async def watermark_scale_handler(client: Client, query: CallbackQuery):
    """Handle watermark scale selection"""
    try:
        scale = query.data.split("_")[2]  # This should be like "10%"
        chat_id = int(query.data.split("_")[3])
        await update_user_settings(chat_id, {"watermark_scale": scale})
        await query.answer(f"📐 Scale set to {WATERMARK_SCALES[scale]}")
        await set_watermark_handler(client, query)
    except Exception as e:
        logger.error(f"Watermark scale error: {str(e)}")
        await query.answer("Failed to set scale!", show_alert=True)
@bot.on_callback_query(filters.regex(r"^wm_opacity_menu_(\d+)$"))
async def watermark_opacity_menu_handler(client: Client, query: CallbackQuery):
    """Watermark opacity selection menu"""
    try:
        chat_id = int(query.data.split("_")[3])
        buttons = [
            [
                InlineKeyboardButton("💧 30% - Light", callback_data=f"wm_opacity_30%_{chat_id}"),
                InlineKeyboardButton("💧 50% - Medium", callback_data=f"wm_opacity_50%_{chat_id}")
            ],
            [
                InlineKeyboardButton("💧 70% - Strong", callback_data=f"wm_opacity_70%_{chat_id}"),
                InlineKeyboardButton("💧 90% - Heavy", callback_data=f"wm_opacity_90%_{chat_id}")
            ],
            [
                InlineKeyboardButton("💧 100% - Full", callback_data=f"wm_opacity_100%_{chat_id}")
            ],
            [
                InlineKeyboardButton("🔙 Back", callback_data=f"set_wm_{chat_id}")
            ]
        ]
        await query.message.edit_text(
            "🎨 **Select Watermark Opacity**\n\n"
            "Choose how transparent the watermark should be:\n\n"
            "• **30%**: Very light, semi-transparent\n"
            "• **50%**: Medium visibility\n" 
            "• **70%**: Clearly visible (recommended)\n"
            "• **90%**: Heavy, almost solid\n"
            "• **100%**: Completely solid\n\n"
            "💡 *Recommended: 70% for good visibility without being intrusive*",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        await query.answer()
    except Exception as e:
        logger.error(f"Watermark opacity menu error: {str(e)}")
        await query.answer("Failed to load opacity menu!", show_alert=True)
@bot.on_callback_query(filters.regex(r"^wm_opacity_(\d+%)_(\d+)$"))
async def watermark_opacity_handler(client: Client, query: CallbackQuery):
    """Handle watermark opacity selection"""
    try:
        opacity = query.data.split("_")[2]  # This should be like "70%"
        chat_id = int(query.data.split("_")[3])
        await update_user_settings(chat_id, {"watermark_opacity": opacity})
        await query.answer(f"🎨 Opacity set to {WATERMARK_OPACITY[opacity]}")
        await set_watermark_handler(client, query)
    except Exception as e:
        logger.error(f"Watermark opacity error: {str(e)}")
        await query.answer("Failed to set opacity!", show_alert=True)
@bot.on_callback_query(filters.regex(r"^preview_wm_(\d+)$"))
async def preview_watermark_handler(client: Client, query: CallbackQuery):
    """Generate and send watermark preview"""
    try:
        chat_id = int(query.data.split("_")[2])
        session = await get_user_settings(chat_id)
        if not session or not session.get("watermark") or not ospath.exists(session["watermark"]):
            await query.answer("❌ No watermark file found!", show_alert=True)
            return
        await query.answer("🔄 Generating preview...")
        # Create a simple preview image showing watermark position
        preview_path = await generate_watermark_preview(chat_id, session)
        if preview_path and ospath.exists(preview_path):
            caption = (
                "👁️ **Watermark Preview**\n\n"
                f"📍 **Position:** {WATERMARK_POSITIONS.get(session.get('watermark_position', 'bottom-right'))}\n"
                f"📐 **Scale:** {WATERMARK_SCALES.get(session.get('watermark_scale', '10%'))}\n"
                f"🎨 **Opacity:** {WATERMARK_OPACITY.get(session.get('watermark_opacity', '70%'))}\n\n"
                "💡 *This shows approximate placement on a 16:9 video*"
            )
            await query.message.reply_photo(
                photo=preview_path,
                caption=caption
            )
            # Cleanup preview file
            try:
                osremove(preview_path)
            except:
                pass
        else:
            await query.answer("❌ Failed to generate preview!", show_alert=True)
    except Exception as e:
        logger.error(f"Preview watermark error: {str(e)}")
        await query.answer("Failed to generate preview!", show_alert=True)
async def generate_watermark_preview(chat_id: int, session: dict) -> Optional[str]:
    """Generate a visual preview of watermark placement"""
    try:
        from PIL import Image, ImageDraw, ImageFont
        # Create a 16:9 preview canvas
        width, height = 800, 450
        preview = Image.new('RGB', (width, height), (40, 40, 60))
        draw = ImageDraw.Draw(preview)
        # Add video area
        video_area = Image.new('RGB', (width-100, height-100), (80, 80, 100))
        preview.paste(video_area, (50, 25))
        # Load and process watermark
        wm_path = session["watermark"]
        wm_scale = int(session.get("watermark_scale", "10%").replace('%', ''))
        wm_opacity = int(session.get("watermark_opacity", "70%").replace('%', ''))
        position = session.get("watermark_position", "bottom-right")
        try:
            wm_image = Image.open(wm_path).convert("RGBA")
            # Resize watermark
            wm_width = int(width * wm_scale / 100)
            wm_ratio = wm_image.width / wm_image.height
            wm_height = int(wm_width / wm_ratio)
            wm_image = wm_image.resize((wm_width, wm_height), Image.Resampling.LANCZOS)
            # Apply opacity
            if wm_opacity < 100:
                alpha = wm_image.split()[3]
                alpha = alpha.point(lambda p: p * wm_opacity / 100)
                wm_image.putalpha(alpha)
            # Calculate position
            positions = {
                "top-left": (60, 35),
                "top-right": (width - wm_width - 60, 35),
                "bottom-left": (60, height - wm_height - 35),
                "bottom-right": (width - wm_width - 60, height - wm_height - 35),
                "center": ((width - wm_width) // 2, (height - wm_height) // 2)
            }
            pos = positions.get(position, positions["bottom-right"])
            preview.paste(wm_image, pos, wm_image)
        except Exception as e:
            logger.warning(f"Could not add watermark to preview: {e}")
        # Add position indicator
        draw.rectangle([45, 20, width-55, height-30], outline=(255, 255, 255), width=2)
        # Save preview
        preview_path = f"previews/wm_preview_{chat_id}.jpg"
        os.makedirs("previews", exist_ok=True)
        preview.save(preview_path, "JPEG", quality=90)
        return preview_path
    except Exception as e:
        logger.error(f"Generate watermark preview error: {e}")
        return None
@bot.on_callback_query(filters.regex(r"^upload_wm_(\d+)$"))
async def upload_watermark_handler(client: Client, query: CallbackQuery):
    """Handle watermark upload initiation"""
    try:
        chat_id = int(query.data.split("_")[2])
        await update_user_settings(chat_id, {"awaiting": "wm"})
        await query.message.edit_text(
            "📤 **Upload Watermark Image**\n\n"
            "Please send a PNG image to use as watermark:\n\n"
            "📝 **Requirements:**\n"
            "• PNG format with transparency\n"
            "• Square or rectangular shape\n" 
            "• Clear, recognizable design\n"
            "• File size under 5MB\n\n"
            "🎯 **Best Practices:**\n"
            "• Use your logo or text\n"
            "• Transparent background\n"
            "• Simple, clean design\n"
            "• High contrast for visibility\n\n"
            "You can configure position, size, and opacity after upload.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("❌ Cancel", callback_data=f"set_wm_{chat_id}")]
            ])
        )
        await query.answer()
    except Exception as e:
        logger.error(f"Upload watermark error: {str(e)}")
        await query.answer("Failed to start upload!", show_alert=True)
@bot.on_callback_query(filters.regex(r"^change_wm_(\d+)$"))
async def change_watermark_handler(client: Client, query: CallbackQuery):
    """Change existing watermark"""
    try:
        chat_id = int(query.data.split("_")[2])
        await update_user_settings(chat_id, {"awaiting": "wm"})
        await query.message.edit_text(
            "💧 **Change Watermark**\n\n"
            "Please send a new PNG image to replace the current watermark:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("❌ Cancel", callback_data=f"set_wm_{chat_id}")]
            ])
        )
        await query.answer()
    except Exception as e:
        logger.error(f"Change watermark error: {str(e)}")
        await query.answer("Failed to change watermark!", show_alert=True)
@bot.on_callback_query(filters.regex(r"^delete_wm_(\d+)$"))
async def delete_watermark_handler(client: Client, query: CallbackQuery):
    """Delete watermark file"""
    try:
        chat_id = int(query.data.split("_")[2])
        session = await get_user_settings(chat_id)
        if not session:
            return await query.answer("Session expired!", show_alert=True)
        # Delete watermark file
        watermark_path = session.get("watermark")
        if watermark_path and ospath.exists(watermark_path):
            try:
                osremove(watermark_path)
                logger.info(f"Deleted watermark file: {watermark_path}")
            except Exception as e:
                logger.warning(f"Failed to delete watermark file: {e}")
        # Clear settings
        await update_user_settings(chat_id, {
            "watermark": None,
            "watermark_enabled": False
        })
        await query.answer("✅ Watermark deleted")
        await collect_settings(chat_id)
    except Exception as e:
        logger.error(f"Delete watermark error: {str(e)}")
        await query.answer("Failed to delete watermark!", show_alert=True)
@bot.on_callback_query(filters.regex(r"^set_subtitle_mode_(\d+)$"))
async def set_subtitle_mode_handler(client: Client, query: CallbackQuery):
    """Enhanced subtitle mode selection with extract option - FIXED CALLBACK DATA"""
    try:
        chat_id = int(query.data.split("_")[3])
        session = await get_user_settings(chat_id)
        if not session:
            return await query.answer("Session expired!", show_alert=True)
        
        # Enhanced subtitle options with SIMPLIFIED callback data
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("📝 Keep Original", callback_data=f"sub_keep_{chat_id}")],
            [InlineKeyboardButton("🗑️ Remove All", callback_data=f"sub_remove_{chat_id}")],
            [InlineKeyboardButton("🔥 Hard Sub (Existing)", callback_data=f"sub_hardexist_{chat_id}")],  # Changed to single word
            [InlineKeyboardButton("🔥 Hard Sub (Upload new)", callback_data=f"sub_hardnew_{chat_id}")],   # Changed to single word
            [InlineKeyboardButton("📤 Extract Subtitles", callback_data=f"sub_extract_{chat_id}")],
            [InlineKeyboardButton("🔙 Back to Settings", callback_data=f"settings_menu_{chat_id}")]
        ])
        
        await query.message.edit_text(
            "📝 **Subtitle Settings**\n\n"
            "**Options:**\n"
            "• 📝 **Keep** - Preserve original subtitles\n"
            "• 🗑️ **Remove** - Remove all subtitles\n"
            "• 🔥 **Hard Sub (Existing)** - Burn existing subtitles into video\n"
            "• 🔥 **Hard Sub (New)** - Burn uploaded subtitles into video\n"
            "• 📤 **Extract** - Extract subtitles as separate files\n\n"
            "💡 **Works in both encode and noencode modes**",
            reply_markup=keyboard
        )
        await query.answer()
    except Exception as e:
        logger.error(f"Subtitle mode handler error: {str(e)}")
        await query.answer("Failed to load subtitle settings!", show_alert=True)

@bot.on_callback_query(filters.regex(r"^set_audio_mode_(\d+)$"))
async def set_audio_mode_handler(client: Client, query: CallbackQuery):
    """Enhanced audio mode selection"""
    try:
        chat_id = int(query.data.split("_")[3])
        session = await get_user_settings(chat_id)
        if not session:
            return await query.answer("Session expired!", show_alert=True)
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔊 Keep Original", callback_data=f"audio_keep_{chat_id}")],
            [InlineKeyboardButton("🔇 Remove All", callback_data=f"audio_remove_{chat_id}")],
            [InlineKeyboardButton("➕ Add New Track", callback_data=f"audio_add_{chat_id}")],
            [InlineKeyboardButton("🎵 Extract Audio Only", callback_data=f"audio_extract_{chat_id}")],
            [InlineKeyboardButton("🔙 Back to Settings", callback_data=f"settings_menu_{chat_id}")]
        ])
        
        await query.message.edit_text(
            "🔊 **Audio Settings**\n\n"
            "**Options:**\n"
            "• 🔊 **Keep** - Preserve original audio tracks\n"
            "• 🔇 **Remove** - Remove all audio tracks\n"
            "• ➕ **Add New** - Add additional audio track\n"
            "• 🎵 **Extract** - Extract audio only (no video)\n\n"
            "💡 **Works in both encode and noencode modes**",
            reply_markup=keyboard
        )
        await query.answer()
    except Exception as e:
        logger.error(f"Audio mode handler error: {str(e)}")
        await query.answer("Failed to set audio mode!", show_alert=True)
# Update the regex pattern to match the simplified callback data
@bot.on_callback_query(filters.regex(r"^sub_(keep|remove|hardexist|hardnew|extract)_(\d+)$"))
async def subtitle_selection_handler(client: Client, query: CallbackQuery):
    """Handle enhanced subtitle mode selection - SIMPLIFIED VERSION"""
    try:
        # Parse the callback data - now always 3 parts
        data_parts = query.data.split("_")
        
        if len(data_parts) != 3:
            logger.error(f"Invalid subtitle callback data: {query.data}")
            await query.answer("Invalid request!", show_alert=True)
            return
            
        mode = data_parts[1]  # keep, remove, hardexist, hardnew, extract
        chat_id = int(data_parts[2])
        
        session = await get_user_settings(chat_id)
        if not session:
            return await query.answer("Session expired!", show_alert=True)
        
        # Map simplified callback modes to internal values
        mode_map = {
            "keep": "keep",
            "remove": "remove", 
            "hardexist": "hard_existing",  # Map back to internal value
            "hardnew": "hard_new",         # Map back to internal value
            "extract": "extract"
        }
        
        internal_mode = mode_map.get(mode, "keep")
        await update_user_settings(chat_id, {"subtitle_mode": internal_mode})
        
        if mode == "hardnew":
            await update_user_settings(chat_id, {"awaiting": "subtitle"})
            await query.message.edit_text(
                "✅ **Hard Subtitle Mode (New)**\n\n"
                "Please send a subtitle file (SRT, ASS, or SSA format) to burn into the video.\n\n"
                "📝 **Supported formats:**\n"
                "• SRT (.srt) - SubRip Subtitle\n"
                "• ASS (.ass) - Advanced SubStation Alpha\n"
                "• SSA (.ssa) - SubStation Alpha\n\n"
                "💡 The subtitles will be permanently burned into the video.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("❌ Cancel", callback_data=f"set_subtitle_mode_{chat_id}")]
                ])
            )
        elif mode == "extract":
            await query.answer("✅ Subtitle extraction enabled")
            await query.message.edit_text(
                "✅ **Subtitle Extraction Enabled**\n\n"
                "All subtitle tracks will be extracted as separate files and sent to you.\n\n"
                "📝 **Extraction details:**\n"
                "• All subtitle tracks will be extracted\n"
                "• Each track as separate file\n"
                "• Original format preserved\n"
                "• Works in both encode and noencode modes",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Back", callback_data=f"set_subtitle_mode_{chat_id}")]
                ])
            )
        else:
            mode_display = {
                "keep": "Keep Original",
                "remove": "Remove All", 
                "hardexist": "Hard Sub (Existing)",
                "hardnew": "Hard Sub (New)"
            }.get(mode, mode)
            
            await query.answer(f"✅ Subtitle mode: {mode_display}")
            await collect_settings(chat_id)
            
    except Exception as e:
        logger.error(f"Subtitle selection error: {str(e)}")
        await query.answer("Failed to set subtitle mode!", show_alert=True)
@bot.on_callback_query(filters.regex(r"^audio_(keep|remove|add|extract)_(\d+)$"))
async def audio_selection_handler(client: Client, query: CallbackQuery):
    """Handle audio mode selection"""
    try:
        mode = query.data.split("_")[1]
        chat_id = int(query.data.split("_")[2])
        session = await get_user_settings(chat_id)
        if not session:
            return await query.answer("Session expired!", show_alert=True)
        
        await update_user_settings(chat_id, {"audio_mode": mode})
        
        if mode == "add":
            await update_user_settings(chat_id, {"awaiting": "audio"})
            await query.message.edit_text(
                "✅ **Add Audio Track**\n\n"
                "Please send an audio file to add to the video.\n\n"
                "🎵 **Supported formats:**\n"
                "• MP3 (.mp3) - MPEG Audio\n"
                "• AAC (.aac) - Advanced Audio Coding\n"
                "• WAV (.wav) - Waveform Audio\n"
                "• FLAC (.flac) - Free Lossless Audio Codec\n\n"
                "💡 The audio will be added as an additional track.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("❌ Cancel", callback_data=f"set_audio_mode_{chat_id}")]
                ])
            )
        elif mode == "extract":
            await query.answer("✅ Audio extraction enabled")
            await query.message.edit_text(
                "✅ **Audio Extraction Enabled**\n\n"
                "Audio will be extracted without video encoding.\n\n"
                "🎵 **Extraction details:**\n"
                "• Video track will be removed\n"
                "• Audio extracted in original quality\n"
                "• Output as audio file only\n"
                "• Works in both encode and noencode modes",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Back", callback_data=f"set_audio_mode_{chat_id}")]
                ])
            )
        else:
            mode_display = {
                "keep": "Keep Original",
                "remove": "Remove All"
            }.get(mode, mode)
            
            await query.answer(f"✅ Audio mode: {mode_display}")
            await collect_settings(chat_id)
            
    except Exception as e:
        logger.error(f"Audio selection error: {str(e)}", exc_info=True)
        await query.answer("Failed to set audio mode!", show_alert=True)
@bot.on_callback_query(filters.regex(r"^toggle_samples_(\d+)$"))
async def toggle_samples_handler(client: Client, query: CallbackQuery):
    """Toggle samples enabled/disabled"""
    try:
        chat_id = int(query.data.split("_")[2])
        session = await get_user_settings(chat_id)
        if not session:
            return await query.answer("Session expired!", show_alert=True)
        current = session.get("samples_enabled", False)
        new_value = not current
        await update_user_settings(chat_id, {"samples_enabled": new_value})
        await query.answer(f"Samples {'enabled' if new_value else 'disabled'}")
        await collect_settings(chat_id)
    except Exception as e:
        logger.error(f"Toggle samples error: {str(e)}")
        await query.answer("Failed to toggle samples!", show_alert=True)
@bot.on_callback_query(filters.regex(r"^toggle_screenshots_(\d+)$"))
async def toggle_screenshots_handler(client: Client, query: CallbackQuery):
    """Toggle screenshots enabled/disabled"""
    try:
        chat_id = int(query.data.split("_")[2])
        session = await get_user_settings(chat_id)
        if not session:
            return await query.answer("Session expired!", show_alert=True)
        current = session.get("screenshots_enabled", False)
        new_value = not current
        await update_user_settings(chat_id, {"screenshots_enabled": new_value})
        await query.answer(f"Screenshots {'enabled' if new_value else 'disabled'}")
        await collect_settings(chat_id)
    except Exception as e:
        logger.error(f"Toggle screenshots error: {str(e)}")
        await query.answer("Failed to toggle screenshots!", show_alert=True)
@bot.on_callback_query(filters.regex(r"^skip_(thumb|wm|title|remname|crf|subtitle|audio)_(\d+)$"))
async def skip_handler(client: Client, query: CallbackQuery):
    """Skip input handler - FIXED VERSION"""
    try:
        chat_id = int(query.data.split("_")[2])
        item_type = query.data.split("_")[1]
        # Clear the awaiting state first
        await clear_awaiting_state(chat_id)
        # Clear the specific setting with proper None values
        if item_type == "crf":
            await update_user_settings(chat_id, {"custom_crf": None})
            message = "CRF setting skipped - using auto CRF"
        elif item_type == "subtitle":
            await update_user_settings(chat_id, {"subtitle_mode": "keep", "subtitle_file": None})
            message = "Subtitle setting skipped - keeping original subtitles"
        elif item_type == "audio":
            await update_user_settings(chat_id, {"audio_mode": "keep", "audio_file": None})
            message = "Audio setting skipped - keeping original audio"
        elif item_type == "remname":
            await update_user_settings(chat_id, {"remname": None})
            message = "REMNAME setting skipped - using original filenames"
        elif item_type == "title":
            await update_user_settings(chat_id, {"metadata.title": ""})
            message = "Title setting skipped - no custom title"
        elif item_type == "thumb":
            await update_user_settings(chat_id, {"thumbnail": None})
            message = "Thumbnail setting skipped - no custom thumbnail"
        elif item_type == "wm":
            await update_user_settings(chat_id, {"watermark": None, "watermark_enabled": False})
            message = "Watermark setting skipped - watermark disabled"
        else:
            message = f"{item_type.upper()} setting skipped"
        await query.answer(f"✅ {message}")
        await collect_settings(chat_id)
    except Exception as e:
        logger.error(f"Skip handler error: {str(e)}")
        await query.answer("Error skipping input!", show_alert=True)
@bot.on_callback_query(filters.regex(r"^cancel_(download|encode|batch|upload)_(\d+)$"))
async def cancel_task_handler(client: Client, query: CallbackQuery):
    """Handle task cancellation - FIXED VERSION"""
    try:
        cancel_type = query.data.split("_")[1]
        chat_id = int(query.data.split("_")[2])
        # Verify the user is cancelling their own task
        if query.from_user.id != chat_id:
            await query.answer("❌ You can only cancel your own tasks!", show_alert=True)
            return
        # Cancel user tasks using the new function
        success = await cancel_user_tasks(chat_id)
        if success:
            await query.answer("🛑 Cancellation requested...")
            # Edit the current message instead of sending new one
            await query.message.edit_text(
                f"❌ **{cancel_type.capitalize()} Cancelled**\n\n"
                "Your task has been stopped successfully.\n"
                "You can start a new task whenever you're ready."
            )
            logger.info(f"✅ Task cancelled for user {chat_id} during {cancel_type}")
        else:
            await query.answer("❌ No active tasks to cancel!", show_alert=True)
    except Exception as e:
        logger.error(f"Cancel handler error: {str(e)}")
        await query.answer("Failed to cancel task!", show_alert=True)
async def cleanup_temp_files():
    """Periodic cleanup of temporary files every 12-24 hours when no tasks are running"""
    while True:
        await asyncio.sleep(43200)  # 12 hours
        try:
            queue_status = global_queue_manager.get_queue_status()
            # Only cleanup if no tasks are running and queue is empty
            if queue_status['current_task'] is None and queue_status['queue_size'] == 0:
                logger.info("Performing scheduled cleanup...")
                deleted_files, freed_space = await cleanup_storage()
                logger.info(f"Scheduled cleanup completed: {deleted_files} files deleted, {humanize.naturalsize(freed_space)} freed")
            else:
                logger.info("Skipping scheduled cleanup - tasks are running")
        except Exception as e:
            logger.error(f"Scheduled cleanup error: {str(e)}")
async def session_cleanup_manager():
    """Periodically clean up expired sessions"""
    while True:
        try:
            await session_manager.cleanup_sessions()
            await asyncio.sleep(1800)
        except Exception as e:
            logger.error(f"Session cleanup error: {str(e)}")
            await asyncio.sleep(300)
async def check_session_timeout():
    """Automatically clear stale awaiting states"""
    while True:
        await asyncio.sleep(300)  # Check every 5 minutes
        try:
            current_time = time.time()
            for chat_id, session_data in session_manager.sessions.items():
                if (session_data.get('awaiting') and 
                    current_time - session_data.get('last_activity', 0) > 600):  # 10 minutes timeout
                    await update_user_settings(chat_id, {"awaiting": None})
                    logger.info(f"🕒 Cleared stale awaiting state for user {chat_id}")
        except Exception as e:
            logger.error(f"Session timeout check error: {str(e)}")
async def check_premium_expiry():
    """Automatically check and remove expired premium users"""
    while True:
        await asyncio.sleep(3600)  # Check every hour
        try:
            current_time = time.time()
            expired_count = 0
            for user_id in list(PREMIUM_USERS.keys()):
                if PREMIUM_USERS[user_id].get('expiry', 0) < current_time:
                    await revoke_premium(user_id)
                    expired_count += 1
            if expired_count > 0:
                logger.info(f"🕒 Automatically removed {expired_count} expired premium users")
        except Exception as e:
            logger.error(f"Premium expiry check error: {str(e)}")
# Start this in your main function:
# Start this in your main function
def get_bot_uptime():
    """Calculate bot uptime"""
    if not hasattr(get_bot_uptime, 'start_time'):
        get_bot_uptime.start_time = time.time()
    uptime_seconds = int(time.time() - get_bot_uptime.start_time)
    if uptime_seconds < 60:
        return f"{uptime_seconds} seconds"
    elif uptime_seconds < 3600:
        return f"{uptime_seconds // 60} minutes"
    elif uptime_seconds < 86400:
        hours = uptime_seconds // 3600
        minutes = (uptime_seconds % 3600) // 60
        return f"{hours}h {minutes}m"
    else:
        days = uptime_seconds // 86400
        hours = (uptime_seconds % 86400) // 3600
        return f"{days}d {hours}h"
async def check_ban_expiry():
    """Automatically check and remove expired bans"""
    while True:
        await asyncio.sleep(3600)  # Check every hour
        try:
            current_time = time.time()
            expired_count = 0
            
            for user_id in list(BANNED_USERS.keys()):
                ban_data = BANNED_USERS[user_id]
                expires_at = ban_data.get('expires_at')
                
                if expires_at:
                    expiry_time = datetime.fromisoformat(expires_at).timestamp()
                    if current_time > expiry_time:
                        await unban_user(user_id)
                        expired_count += 1
            
            if expired_count > 0:
                logger.info(f"🕒 Automatically removed {expired_count} expired bans")
                
        except Exception as e:
            logger.error(f"Ban expiry check error: {str(e)}")          
async def send_restart_notification():
    try:
        all_users = set()  
        for chat_id in session_manager.sessions.keys():
            all_users.add(chat_id)
        for chat_id in active_tasks.keys():
            all_users.add(chat_id)
        
        if user_settings is not None:
            try:
                users_from_db = await user_settings.distinct("chat_id")
                for user_id in users_from_db:
                    all_users.add(user_id)
            except Exception:
                pass        
        restart_msg = "𝗛𝗲𝘆,𝗕𝗼𝘁 𝗜𝘀 𝗥𝗲𝘀𝘁𝗮𝗿𝘁𝗲𝗱!!!"
        restart_sticker_id = "CAACAgIAAxkBAAJtk2kPTsKZhoExJ0zYVAs241COBRiBAAJTMgACXZYZSMfsKURiyhquHgQ"  # ← replace this with your actual sticker_id

        for user_id in all_users:
            try:
                # Send restart message
                await bot.send_message(user_id, restart_msg)
                
                # Send sticker
                await bot.send_sticker(user_id, restart_sticker_id)
                
                await asyncio.sleep(0.1)
            except Exception:
                pass                
    except Exception as e:
        logger.error(f"Restart notification error: {str(e)}")  
async def main():
    """Main bot entry point with enhanced startup"""
    # DECLARE GLOBAL VARIABLES AT THE VERY BEGINNING
    global SUDO_USERS, BOT_AUTHORIZED, BANNED_USERS 
    logger.info("🚀 Starting enhanced video encoder bot...")
    try:
        # Initialize uptime tracker
        get_bot_uptime.start_time = time.time()
        await test_mongodb_connection()  # ADD THIS LINE
        # Test MongoDB connection
        if db is not None:
            try:
                await db.command('ping')
                logger.info("✅ Connected to MongoDB successfully")
                # Load settings from MongoDB
                await load_sudo_users()
                await load_bot_settings()
                await load_size_limits()  # Load size limits
                await load_premium_users()  # Add this line
                await load_showcase_settings()  # ADD THIS LINE
                await load_banned_users()  # 🆕 ADD THIS LINE


    # Load storage analytics
                analytics_data = await load_storage_analytics()
                logger.info(f"📊 Loaded storage analytics: {analytics_data['total_processed']} files processed")
            except ConnectionFailure:
                logger.error("❌ Failed to connect to MongoDB")
                # Initialize with defaults
                SUDO_USERS = [BOT_OWNER_ID]
                BOT_AUTHORIZED = False
                logger.info("🔄 Using default settings (MongoDB connection failed)")
        else:
            logger.warning("⚠️ MongoDB is not initialized; running in offline mode")
            # Initialize with defaults
            SUDO_USERS = [BOT_OWNER_ID]
            BOT_AUTHORIZED = False
        # Start the bot
        logger.info("🔄 Starting bot...")
        await bot.start()
        # Get bot info to confirm it's working
        bot_info = await bot.get_me()
        logger.info(f"✅ Bot started successfully: @{bot_info.username} (ID: {bot_info.id})")
        # Log loaded settings
        logger.info(f"👥 Sudo users loaded: {len(SUDO_USERS)}")
        logger.info(f"🚫 Banned users loaded: {len(BANNED_USERS)}")  # 🆕 ADD THIS LINE

        logger.info(f"🔐 Authorization mode: {'RESTRICTED' if BOT_AUTHORIZED else 'PUBLIC'}")
        # Send comprehensive startup notification to owner
        try:
            startup_msg = (
                f"🤖 **Bot Started Successfully!**\n\n"
                f"🕒 **Startup Time:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"👤 **Bot:** @{bot_info.username}\n"
                f"🆔 **Bot ID:** {bot_info.id}\n"
                f"👥 **Sudo Users:** {len(SUDO_USERS)}\n"
                f"📏 **Size Limits:** {humanize.naturalsize(TORRENT_SIZE_LIMIT)} torrent, {humanize.naturalsize(SINGLE_FILE_SIZE_LIMIT)} file\n"
                f"🔐 **Authorization:** {'🔒 RESTRICTED' if BOT_AUTHORIZED else '🔓 PUBLIC'}\n"
                f"💾 **Storage:** {'MongoDB' if db is not None else 'Memory'}\n"
                f"💻 **System:** {psutil.cpu_percent()}% CPU, {psutil.virtual_memory().percent}% RAM\n\n"
                f"✅ **All systems operational**\n"
                f"🔧 **Commands ready:** /start, /status, /queue, /magnet, /compress, /cancel, /cleanup"
            )
            await bot.send_message(BOT_OWNER_ID, startup_msg)
        except Exception as e:
            logger.warning(f"Could not send startup message to owner: {e}")
        # Start background tasks
        asyncio.create_task(cleanup_temp_files())
        asyncio.create_task(session_cleanup_manager())
        asyncio.create_task(check_session_timeout())  
        asyncio.create_task(check_premium_expiry())     
        asyncio.create_task(cleanup_old_cancellation_events())  # Add this if missing
        asyncio.create_task(check_ban_expiry())  # 🆕 ADD THIS LINE
        # In main() function after await bot.start():
        asyncio.create_task(send_restart_notification())
        logger.info("✅ All systems operational - Bot is ready and commands are active!")
        # Keep the bot running
        await idle()
    except Exception as e:
        logger.critical(f"💥 Bot crashed: {str(e)}", exc_info=True)
        # Try to notify owner about crash
        try:
            await bot.send_message(BOT_OWNER_ID, f"❌ Bot crashed: {str(e)}")
        except:
            pass
    finally:
        logger.info("🛑 Shutting down bot...")
        try:
            # Save settings before shutdown only if MongoDB is available
            if db is not None:
                await save_sudo_users()
                await save_bot_settings()
                await save_size_limits()  # Save size limits
                logger.info("💾 Settings saved to MongoDB")
            if bot.is_connected:
                await bot.stop()
            logger.info("✅ Bot stopped gracefully")
        except Exception as e:
            logger.error(f"Error during bot shutdown: {e}")
        logger.info("🎯 Bot shutdown complete")
if __name__ == "__main__":
    logger.info("Starting enhanced video encoder bot...")
    try:
        import signal
        loop = asyncio.get_event_loop()
        for sig in [signal.SIGINT, signal.SIGTERM]:
            loop.add_signal_handler(sig, lambda: loop.stop())
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down...")
    except Exception as e:
        logger.critical(f"Bot failed to start: {str(e)}")
    finally:
        loop.close()
        logger.info("Bot shutdown complete")
