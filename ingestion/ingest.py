import json
import time
import pathway as pw

# ---------- STEP 1: Define schema ----------
class News(pw.Schema):
    id: int
    title: str
    content: str
    label: str


# ---------- STEP 2: Create Pathway table ----------
table = pw.Table.empty(schema=News)


# ---------- STEP 3: Ingest data (simulated streaming) ----------
def stream_news(json_path):
    with open(json_path, "r") as f:
        data = json.load(f)

    for item in data:
        print("🆕 NEW NEWS ARRIVED:", item["title"])

        table.insert(
            id=item["id"],
            title=item["title"],
            content=item["content"],
            label=item["label"]
        )

        time.sleep(2)  # simulate real-time arrival


# ---------- STEP 4: Observe Pathway table ----------
@pw.compute
def show_news(t=table):
    pw.debug.compute_and_print(t)


# ---------- STEP 5: Run ----------
if __name__ == "__main__":
    stream_news("data/fake_news.json")
    pw.run()
