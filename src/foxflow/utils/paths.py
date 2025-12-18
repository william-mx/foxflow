import re

def _safe_topic_name(topic: str) -> str:
    name = topic.strip().lstrip("/")
    name = name.replace("/", "__")
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", name)