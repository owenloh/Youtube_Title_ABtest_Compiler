import requests
import re

HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

channels = [
    'Veritasium',
    'Kurzgesagt',
    'MrBeast',
    '3Blue1Brown',
    'SmarterEveryDay',
    'Numberphile',
    'CGPGrey',
    'TwoMinutePapers',
    'TED-Ed',
    'Practical Engineering',
    'Computerphile',
    'ElectroBOOM',
    'Mark Rober',
    'Real Engineering',
    'ColdFusion',
    'Linus Tech Tips',
    'Johnny Harris',
    'Cleo Abram'
]

# Test handles
test_handles = [
    '@veritasium',
    '@kurzgesagt',
    '@MrBeast',
    '@3blue1brown',
    '@smartereveryday',
    '@numberphile',
    '@CGPGrey',
    '@TwoMinutePapers',
    '@TEDEd',
    '@PracticalEngineeringChannel',
    '@Computerphile',
    '@ElectroBOOM',
    '@MarkRober',
    '@RealEngineering',
    '@ColdFusion',
    '@LinusTechTips',
    '@johnnyharris',
    '@CleoAbram'
]

for handle, name in zip(test_handles, channels):
    try:
        url = f'https://www.youtube.com/{handle}'
        r = requests.get(url, headers=HEADERS, timeout=10)
        
        # Get og:title to verify channel name
        og_match = re.search(r'<meta property="og:title" content="([^"]+)"', r.text)
        og_title = og_match.group(1) if og_match else 'NOT FOUND'
        
        # Get externalId (most reliable channel ID)
        ext_match = re.search(r'"externalId":"(UC[a-zA-Z0-9_-]{22})"', r.text)
        channel_id = ext_match.group(1) if ext_match else 'NOT FOUND'
        
        status = 'OK' if name.lower() in og_title.lower() or og_title.lower() in name.lower() else 'MISMATCH'
        print(f'{handle}:{name} -> {og_title} [{channel_id}] {status}')
    except Exception as e:
        print(f'{handle}:{name} -> ERROR: {e}')
