import requests
import json
from config import GEMINI_API_KEYS, OPENROUTER_API_KEY, COHERE_API_KEY

def test_gemini():
    print("[1] Testing Gemini...")
    key = GEMINI_API_KEYS[0] if GEMINI_API_KEYS else None
    if not key:
        return "No key"
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={key}"
    payload = {"contents": [{"parts": [{"text": "مرحباً، أجب بكلمة واحدة فقط: ممتاز"}]}]}
    r = requests.post(url, json=payload, timeout=10)
    print(f"  Status: {r.status_code}")
    if r.status_code == 200:
        return r.json()["candidates"][0]["content"]["parts"][0]["text"].strip()
    return r.text

def test_openrouter():
    print("\n[2] Testing OpenRouter...")
    if not OPENROUTER_API_KEY:
        return "No key"
    url = "https://openrouter.ai/api/v1/chat/completions"
    headers = {"Authorization": f"Bearer {OPENROUTER_API_KEY}", "HTTP-Referer": "https://mahwous.com"}
    payload = {"model": "google/gemini-2.0-flash-lite-preview-02-05:free", "messages": [{"role": "user", "content": "مرحباً، أجب بكلمة واحدة فقط: رائع"}]}
    r = requests.post(url, json=payload, headers=headers, timeout=10)
    print(f"  Status: {r.status_code}")
    if r.status_code == 200:
        return r.json()["choices"][0]["message"]["content"].strip()
    return r.text

def test_cohere():
    print("\n[3] Testing Cohere...")
    if not COHERE_API_KEY:
        return "No key"
    url = "https://api.cohere.com/v2/chat"
    headers = {"Authorization": f"Bearer {COHERE_API_KEY}", "Content-Type": "application/json"}
    payload = {"model": "command-r-plus", "messages": [{"role": "user", "content": "مرحباً، أجب بكلمة واحدة فقط: مذهل"}]}
    r = requests.post(url, json=payload, headers=headers, timeout=10)
    print(f"  Status: {r.status_code}")
    if r.status_code == 200:
        return r.json().get("message", {}).get("content", [{}])[0].get("text", "").strip()
    return r.text

print(f"Gemini Result: {test_gemini()}")
print(f"OpenRouter Result: {test_openrouter()}")
print(f"Cohere Result: {test_cohere()}")
