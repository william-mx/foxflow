# foxflow/utils/utils.py

def build_event_query(
    *,
    keys: list[str] | None = None,
    values: list | None = None,
    pairs: dict | None = None,
) -> str:
    tokens: list[str] = []

    if keys:
        for k in keys:
            tokens.append(f"{k}:*")

    if values:
        for v in values:
            tokens.append(str(v))

    if pairs:
        for k, v in pairs.items():
            tokens.append(f"{k}:{v}")

    return " ".join(tokens)
