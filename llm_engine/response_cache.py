"""
=============================================================
  response_cache.py
  Caches LLM responses using a simple JSON file.
  Prevents regenerating descriptions we already have.
  (No Redis needed — uses local file cache for simplicity)
=============================================================
"""

import json
import os
import hashlib
from datetime import datetime


# Cache file location
CACHE_FILE = os.path.join(os.path.dirname(__file__), "llm_cache.json")


def _load_cache() -> dict:
    """Loads cache from JSON file. Returns empty dict if file doesn't exist."""
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def _save_cache(cache: dict):
    """Saves cache dict to JSON file."""
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2, ensure_ascii=False)


def _make_key(prompt: str) -> str:
    """
    Creates a unique key for a prompt using MD5 hash.
    Same prompt always produces same key.

    Example:
        "Describe the orders table..." → "a3f4b2c1..."
    """
    return hashlib.md5(prompt.encode()).hexdigest()


def get_cached_response(prompt: str) -> str | None:
    """
    Checks if we already have a cached response for this prompt.

    Returns:
        Cached response string if found
        None if not in cache
    """
    cache = _load_cache()
    key   = _make_key(prompt)

    if key in cache:
        return cache[key]["response"]
    return None


def save_to_cache(prompt: str, response: str):
    """
    Saves a new LLM response to cache.

    Args:
        prompt  : The prompt we sent to the LLM
        response: The response the LLM gave back
    """
    cache     = _load_cache()
    key       = _make_key(prompt)
    cache[key] = {
        "response"  : response,
        "cached_at" : datetime.now().isoformat(),
        "prompt_preview": prompt[:100] + "..."
    }
    _save_cache(cache)


def get_cache_stats() -> dict:
    """Returns stats about the current cache."""
    cache = _load_cache()
    return {
        "total_cached" : len(cache),
        "cache_file"   : CACHE_FILE,
        "exists"       : os.path.exists(CACHE_FILE)
    }


def clear_cache():
    """Clears all cached responses. Use when you want fresh descriptions."""
    if os.path.exists(CACHE_FILE):
        os.remove(CACHE_FILE)
        print("  🗑️  Cache cleared!")
    else:
        print("  ℹ️  No cache file found.")


if __name__ == "__main__":
    # Quick test
    print("Testing response_cache...\n")

    test_prompt   = "Describe the orders table with columns order_id, user_id"
    test_response = "Stores all customer purchase transactions."

    # Save to cache
    save_to_cache(test_prompt, test_response)
    print(f"  ✅ Saved to cache")

    # Retrieve from cache
    cached = get_cached_response(test_prompt)
    print(f"  ✅ Retrieved from cache: {cached}")

    # Stats
    stats = get_cache_stats()
    print(f"  📊 Cache stats: {stats}")

    # Clear cache
    clear_cache()
    print("\n✅ response_cache working correctly!")