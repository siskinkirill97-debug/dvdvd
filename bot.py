import asyncio
import sqlite3
import json
import logging
import time
from datetime import datetime
from io import BytesIO
import google.generativeai as genai

from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove, LabeledPrice, PreCheckoutQuery

