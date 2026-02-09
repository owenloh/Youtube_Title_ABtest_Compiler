import requests
import re
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

# Get @veritasium page
r = requests.get('https://www.youtube.com/@veritasium', headers=HEADERS, timeout=15)

# Find ALL channel IDs in the page
channel_ids = re.findall(r'"(?:channelId|browseId)":"(UC[a-zA-Z0-9_-]{22})"', r.text)
print('All channel IDs found:', set(channel_ids))

# Look for externalId which is more reliable
external_match = re.search(r'"externalId":"(UC[a-zA-Z0-9_-]{22})"', r.text)
if external_match:
    print('externalId:', external_match.group(1))

# Look for canonical URL
canonical_match = re.search(r'"canonicalBaseUrl":"(/[^"]+)"', r.text)
if canonical_match:
    print('canonicalBaseUrl:', canonical_match.group(1))
