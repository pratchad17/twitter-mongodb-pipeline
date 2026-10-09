# clean_step1.py
import re, json

input_file = "data/10000_tweets_1.json"
output_file = "data/10000_clean.json"

with open(input_file, "r", encoding="utf-8") as f:
    raw = f.read()

# Remove comments (/* ... */)
raw = re.sub(r'/\*.*?\*/', '', raw, flags=re.DOTALL)

# Replace ObjectId("...") with just the string
raw = re.sub(r'ObjectId\("([0-9a-f]+)"\)', r'"\1"', raw)

# Wrap into JSON array
raw = "[" + raw.strip().rstrip(",") + "]"

# Save to file (don’t parse yet — might still contain issues)
with open(output_file, "w", encoding="utf-8") as f:
    f.write(raw)

print(f" Step 1 complete → {output_file}")
