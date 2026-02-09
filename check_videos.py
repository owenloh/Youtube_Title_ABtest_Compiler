import json
data = json.load(open('videos_check.json'))
commented = [v for v in data['videos'] if v['comment_id']]
print(f'Videos with comments: {len(commented)}')
for v in commented:
    print(f"{v['channel_name']}: {v['video_id']} - published {v['published_at']}")

print(f"\nVideos after 2026-02-07:")
after_cutoff = [v for v in data['videos'] if v['published_at'] and v['published_at'] >= '2026-02-07']
for v in after_cutoff[:20]:
    has_comment = 'YES' if v['comment_id'] else 'NO'
    print(f"{v['channel_name']}: {v['video_id']} - {v['published_at']} - comment: {has_comment} - short: {v['is_short']}")
