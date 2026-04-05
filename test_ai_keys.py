import sys
import os
import logging
logging.basicConfig(level=logging.ERROR, stream=sys.stdout)
from engines.ai_engine import _call_gemini, _call_openrouter, _call_cohere
from config import GEMINI_API_KEYS, OPENROUTER_API_KEY, COHERE_API_KEY

def test_keys():
    print("Testing AI Keys...\n")
    print(f"[config] GEMINI_API_KEYS: {bool(GEMINI_API_KEYS)} ({len(GEMINI_API_KEYS)} keys)")
    print(f"[config] OPENROUTER_API_KEY: {bool(OPENROUTER_API_KEY)}")
    print(f"[config] COHERE_API_KEY: {bool(COHERE_API_KEY)}")
    print("-" * 50)

    # 1. Test Gemini
    try:
        print("[1] Testing Gemini...")
        res_g = _call_gemini("مرحباً، أجب بكلمة واحدة فقط: ممتاز")
        print(f"Gemini Response: {res_g}")
    except Exception as e:
        print(f"Gemini Error: {e}")

    # 2. Test OpenRouter
    try:
        print("[2] Testing OpenRouter...")
        res_o = _call_openrouter("مرحباً، أجب بكلمة واحدة فقط: رائع")
        print(f"OpenRouter Response: {res_o}")
    except Exception as e:
        print(f"OpenRouter Error: {e}")

    # 3. Test Cohere
    try:
        print("[3] Testing Cohere...")
        res_c = _call_cohere("مرحباً، أجب بكلمة واحدة فقط: مذهل")
        print(f"Cohere Response: {res_c}")
    except Exception as e:
        print(f"Cohere Error: {e}")

if __name__ == "__main__":
    test_keys()
