import requests
import re
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

# The original channel ID from user's env
original_id = 'UCHnyfMqiRRG1u-2MsSQLbXA'

# Get RSS from original ID
rss_url = f'https://www.youtube.com/feeds/videos.xml?channel_id={original_id}'
r = requests.get(rss_url, headers=HEADERS, timeout=15)
print('Original ID RSS status:', r.status_code)
if r.ok:
    # Get channel title from RSS
    import xml.etree.ElementTree as ET
    root = ET.fromstring(r.text)
    title = root.find('.//{http://www.w3.org/2005/Atom}title')
    if title is not None:
        print('Channel title from RSS:', title.text)

# Now check what @veritasium resolves to
r2 = requests.get('https://www.youtube.com/@veritasium', headers=HEADERS, timeout=15)
match = re.search(r'"channelId":"(UC[a-zA-Z0-9_-]{22})"', r2.text)
if match:
    resolved_id = match.group(1)
    print('Resolved @veritasium to:', resolved_id)
    print('Same as original?', resolved_id == original_id)
