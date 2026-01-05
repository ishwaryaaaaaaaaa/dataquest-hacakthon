import json
import time
import pathway as pw

# 1. Read fake news JSON
with open("../data/fake_news.json", "r") as f:
    news_data = json.load(f)

# 2. Create an input stream for Pathway
input_stream = pw.io.python.read(
    lambda: (
        {
            "id": item["id"],
            "title": item["title"],
            "content": item["content"],
            "timestamp": item["timestamp"],
        }
        for item in news_data
    )
)

# 3. Convert stream to Pathway table
news_table = pw.Table.from_rows(input_stream)

# 4. Print table updates (just to see something)
pw.debug.compute_and_print(news_table)

# 5. Run Pathway
pw.run()

