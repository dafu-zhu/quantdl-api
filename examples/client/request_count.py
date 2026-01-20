"""
request_count - Track S3 API request counts

When to use:
    Use request_count() to monitor API usage in your session.
    Useful for debugging, cost tracking, and optimization.

Methods:
    request_count(period="session") - Get request count
        period="session": Total requests in this session (resets each run)
        period="today": Requests made today (persists across runs)
    request_stats() - Get detailed statistics

Note:
    - Session counts reset on each new client instance
    - Daily counts persist across sessions (stored in ~/.quantdl/request_counts.json)
    - Cache hits do not increment the counter

Example output:
    Session and daily request counts
"""
from dotenv import load_dotenv

load_dotenv()

from quantdl import QuantDLClient

# Initialize client
client = QuantDLClient()

print("request_count() - Track S3 API request counts")
print("=" * 50)

# Check counts at start (today_count may have values from previous runs)
print(f"\nInitial session count: {client.request_count()}")
print(f"Initial today count: {client.request_count('today')} (persisted from previous runs)")

# Clear cache to ensure fresh S3 requests
client.clear_cache()
print("\nCache cleared to demonstrate S3 request counting")

# Make API calls - each symbol triggers an S3 request
symbols = ["IBM", "TXN", "NOW"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-01-31")

print(f"\nAfter fetching {len(symbols)} symbols:")
print(f"  Session count: {client.request_count()}")
print(f"  Today count: {client.request_count('today')}")

# Fetch same data again - should be cached (no new S3 requests)
prices_cached = client.ticks(symbols, field="close", start="2024-01-01", end="2024-01-31")
print("\nAfter fetching same data (cached):")
print(f"  Session count: {client.request_count()} (unchanged - served from cache)")

# Get detailed stats
print("\nDetailed request stats:")
stats = client.request_stats()
print(f"  session_count: {stats['session_count']} (this session only)")
print(f"  today_count: {stats['today_count']} (cumulative today)")
print(f"  daily_counts: {stats['daily_counts']} (history by date)")

# Cleanup
client.close()
