import json
import time

# Load fake news
with open("../data/fake_news.json", "r") as f:
    news = json.load(f)

print("Starting live news ingestion...\n")

for item in news:
    print("NEW NEWS ARRIVED")
    print("ID:", item["id"])
    print("TITLE:", item["title"])
    print("TIME:", item["timestamp"])
    print("-" * 50)

    # simulate live arrival
    time.sleep(2)

print("\nIngestion completed.")

