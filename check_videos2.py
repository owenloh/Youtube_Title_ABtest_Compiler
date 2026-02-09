import requests
import json

r = requests.get('https://youtubetitleabtestcompiler-production.up.railway.app/api/videos')
data = r.json()

commented = [v for v in data['videos'] if v['comment_id']]
print(f'Videos with comments: {len(commented)}')
for v in commented:
    print(f"  {v['channel_name']}: {v['video_id']} - published {v['published_at']} - short: {v['is_short']}")

print(f"\nAll videos after 2026-02-07 (cutoff):")
after_cutoff = [v for v in data['videos'] if v['published_at'] and v['published_at'] >= '2026-02-07' and not v['is_short']]
print(f"Count: {len(after_cutoff)}")
for v in after_cutoff:
    has_comment = 'YES' if v['comment_id'] else 'NO'
    print(f"  {v['channel_name']}: {v['video_id']} - {v['published_at']} - comment: {has_comment}")
