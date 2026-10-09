# clean_step2.py
import re, json

input_file = "data/10000_clean.json"          # from Step 1
output_file = "data/10000_clean_final.json"   # final clean output

with open(input_file, "r", encoding="utf-8") as f:
    txt = f.read()

# 1️⃣ Remove Mongo-specific wrappers and comments
txt = re.sub(r'ObjectId\("([0-9a-f]+)"\)', r'"\1"', txt)
txt = re.sub(r'NumberLong\((\d+)\)', r'\1', txt)
txt = re.sub(r"/\*.*?\*/", "", txt, flags=re.DOTALL)

# 2️⃣ Remove trailing commas before } or ]
txt = re.sub(r',\s*([\]}])', r'\1', txt)

# 3️⃣ Split into JSON object candidates
blocks = re.findall(r"\{.*?\}(?=\s*\{|\s*$)", txt, flags=re.DOTALL)
print(f"🔍 Found {len(blocks)} objects in file")

# 4️⃣ Parse and filter valid JSON objects
tweets = []
for block in blocks:
    try:
        tweets.append(json.loads(block))
    except Exception:
        pass  # skip malformed

print(f" Successfully parsed {len(tweets)} tweets")

# 5️⃣ Save as a clean JSON array
with open(output_file, "w", encoding="utf-8") as f:
    json.dump(tweets, f, ensure_ascii=False, indent=2)

print(f" Step 2 complete → {output_file}")
