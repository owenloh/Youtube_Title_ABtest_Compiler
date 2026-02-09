import requests
import re
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

# Get channel name from @veritasium
r = requests.get('https://www.youtube.com/@veritasium', headers=HEADERS, timeout=15)

# Look for channel name
name_match = re.search(r'"channelName":"([^"]+)"', r.text)
if name_match:
    print('Channel name:', name_match.group(1))

# Look for title
title_match = re.search(r'"title":"([^"]+)"', r.text)
if title_match:
    print('Title:', title_match.group(1))

# Check og:title
og_match = re.search(r'<meta property="og:title" content="([^"]+)"', r.text)
if og_match:
    print('og:title:', og_match.group(1))
