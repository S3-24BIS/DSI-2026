#!/usr/bin/env python3
"""Test DSI optimizations"""
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from dotenv import load_dotenv
load_dotenv()

print("=" * 50)
print("Testing DSI-2026 Optimizations")
print("=" * 50)

print("\n[TEST 1] dsi_config import")
try:
    from dsi_config import ESTADO_MAIOR, COMPANHIAS, CALENDARIOS, get_all_users, GOOGLE_SCOPES
    print("[OK] Imported dsi_config")

    s3_email = ESTADO_MAIOR.get("s3")
    assert s3_email == "s3.24bis03@gmail.com", f"Expected s3.24bis03@gmail.com, got {s3_email}"
    print("[OK] Config variables loaded correctly")

    all_users = get_all_users()
    assert len(all_users) > 0, "No users loaded"
    print(f"[OK] get_all_users() returned {len(all_users)} users")

    assert len(GOOGLE_SCOPES) > 0, "No scopes loaded"
    print(f"[OK] GOOGLE_SCOPES loaded: {len(GOOGLE_SCOPES)} scopes")

except Exception as e:
    print(f"[FAIL] Error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print("\n[TEST 2] dsi_utils import")
try:
    from dsi_utils import DSILogger, cache_with_ttl, validate_email, RateLimiter, api_limiter
    print("[OK] Imported dsi_utils")

    logger = DSILogger()
    print("[OK] DSILogger initialized")

    assert validate_email("test@example.com") == True, "Valid email failed"
    assert validate_email("invalid-email") == False, "Invalid email passed"
    print("[OK] Email validation working")

    limiter = RateLimiter(max_requests=5, window_seconds=60)
    allowed = limiter.allow()
    assert allowed == True, "First request should be allowed"
    print("[OK] RateLimiter initialized and working")

except Exception as e:
    print(f"[FAIL] Error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print("\n[TEST 3] Environment variables")
try:
    dsi_s3 = os.getenv("DSI_S3_EMAIL")
    cache_ttl = os.getenv("CACHE_CALENDAR_TTL")

    assert dsi_s3 == "s3.24bis03@gmail.com", "DSI_S3_EMAIL not loaded"
    assert cache_ttl == "5", "CACHE_CALENDAR_TTL not loaded"

    print("[OK] All environment variables loaded correctly")
except AssertionError as e:
    print(f"[FAIL] {e}")
    sys.exit(1)

print("\n" + "=" * 50)
print("ALL TESTS PASSED")
print("=" * 50)
