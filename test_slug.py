import requests
import re
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

# Test @veritasium
r = requests.get('https://www.youtube.com/@veritasium', headers=HEADERS, timeout=15)
match = re.search(r'"channelId":"(UC[a-zA-Z0-9_-]{22})"', r.text)
if match:
    print('@veritasium ->', match.group(1))
else:
    print('@veritasium -> NOT FOUND')

# Test veritasium (no @)
r2 = requests.get('https://www.youtube.com/veritasium', headers=HEADERS, timeout=15)
match2 = re.search(r'"channelId":"(UC[a-zA-Z0-9_-]{22})"', r2.text)
if match2:
    print('veritasium ->', match2.group(1))
else:
    print('veritasium -> NOT FOUND')

# Check if they're the same
if match and match2:
    print('Same?', match.group(1) == match2.group(1))
