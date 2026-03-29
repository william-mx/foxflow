# foxflow/reader.py
from __future__ import annotations
from foxglove.client import Client
import pandas as pd
import requests
import os
from tqdm import tqdm
import shutil
from functools import wraps


from foxflow.parsers import load_plugins
from foxflow.registry import get_parser, list_schemas
from foxflow.utils import get_timestamp_ns, _safe_topic_name, build_event_query
from foxflow.parsers.generic import parse_generic



def ensure_recording(fn):
    @wraps(fn)
    def wrapper(self, *args, **kwargs):
        if not all(hasattr(self, a) for a in ("recording_id", "device_id", "start", "end")):
            raise RuntimeError(
                "No recording selected. Call select_recording_by_id(...) or "
                "select_recording_by_name(...) first."
            )
        return fn(self, *args, **kwargs)
    return wrapper


def iter_with_timestamp(message_iter):
    for item in message_iter:
        yield get_timestamp_ns(item), item

class BagfileReader:
    def __init__(self, api_key: str):
        self.client = Client(token=api_key)
        load_plugins()  # import all parsers once

        self.recordings = {}
        self.mapping = {}
        self._info_df = None

        self.get_recordings()

    def get_recordings(self) -> None:
        try:
            self.recordings = {
                r["path"].removesuffix(".mcap").removesuffix(".bag"): r["id"]
                for r in self.client.get_recordings()
            }
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 401:
                raise ValueError("Invalid API key") from e
            raise
        
        self.recordings_by_id = {v: k for k, v in self.recordings.items()}

    def print_recordings(self):
        print("Available Recordings:\n")
        for name, rec_id in self.recordings.items():
            print(f"{name.ljust(40)} → {rec_id}")

    def list_available_message_types(self) -> None:
        print("Supported message types:\n")
        for s in list_schemas():
            print(s)

    @property
    def devices(self):
        """Return a list of device names from Foxglove client."""
        devices = self.client.get_devices()
        return [d["name"] for d in devices if "name" in d]

    def select_recording_by_name(self, name):
        if name not in self.recordings:
            raise ValueError(f"Recording name '{name}' not found in available recordings.")
        
        recording_id = self.recordings[name]
        return self.select_recording_by_id(recording_id)



    def select_recording_by_id(self, recording_id: str) -> None:
        rec = [r for r in self.client.get_recordings() if r["id"] == recording_id]
        if not rec:
            raise ValueError(f"Recording with id {recording_id} not found.")
        rec = rec[0]

        if not rec.get("device"):
            raise ValueError("Recording must be assigned to a device before import.")

        self.recording_name = self.recordings_by_id[recording_id]
        self.recording_id = recording_id
        self.device_id = rec["device"]["id"]
        self.start = rec["start"]
        self.end = rec["end"]

        topics = self.client.get_topics(device_id=self.device_id, start=self.start, end=self.end)

        df = pd.DataFrame(topics)

        # topic -> schema_name
        self.mapping = dict(zip(df["topic"], df["schema_name"]))

        return df


    def _get_parser(self, schema):
        try:
            return get_parser(schema)
        except KeyError:
            return parse_generic

    @ensure_recording
    def read_topic(self, topic: str, **kwargs):
        """Return parsed data for a topic using the registered parser for its schema."""
        if topic not in self.mapping:
            raise ValueError(f"Topic '{topic}' not found in recording.")

        # Normalize schema (ROS1 vs ROS2)
        schema = self.mapping[topic].replace("/msg/", "/")
        parser = self._get_parser(schema)

        raw_iter = self.client.iter_messages(
            device_id=self.device_id,
            start=self.start,
            end=self.end,
            topics=[topic],
        )

        messages_iter = iter_with_timestamp(raw_iter)

        result = parser(messages_iter, **kwargs)

        values = tuple(result.values())

        return values[0] if len(values) == 1 else values


    @ensure_recording
    def read_events(
        self,
        topic: str,
        *,
        event_keys: list[str] | None = None,
        event_values: list | None = None,
        event_pairs: dict | None = None,
        **kwargs,
    ):
        if topic not in self.mapping:
            raise ValueError(f"Topic '{topic}' not found in recording.")

        schema = self.mapping[topic].replace("/msg/", "/")
        parser = self._get_parser(schema)

        query = build_event_query(
            keys=event_keys,
            values=event_values,
            pairs=event_pairs,
        )

        events = self.client.get_events(
            device_id=self.device_id,
            start=self.start,
            end=self.end,
            query=query if query else None,
        )

        results = {}

        for evt in events:
            raw_iter = self.client.iter_messages(
                device_id=self.device_id,
                start=evt["start"],
                end=evt["end"],
                topics=[topic],
            )

            messages_iter = iter_with_timestamp(raw_iter)

            parsed = parser(messages_iter, **kwargs)

            results[evt["id"]] = {
                "event": evt,
                **parsed,
            }

        return results


    @ensure_recording
    def get_events(self, *,
        event_keys: list[str] | None = None,
        event_values: list | None = None,
        event_pairs: dict | None = None,
    ):
        query = build_event_query(keys=event_keys, values=event_values, pairs=event_pairs)

        return self.client.get_events(device_id=self.device_id, start=self.start,
            end=self.end, query=query if query else None)


    @ensure_recording
    def export_bagfile(
        self,
        out_dir: str,
        *,
        df_format: str = "parquet",
        export_assets: bool = True,
    ):
        """
        Exports all topics:
        - saves each parser's df to out_dir
        - optionally exports extra assets via the parser (e.g. images)
        - optionally returns extra assets in a dict keyed by topic
        """
        out_dir = os.path.join(out_dir, self.recording_id)
        if os.path.isdir(out_dir):
            shutil.rmtree(out_dir)

        os.makedirs(out_dir, exist_ok=True)


        for topic, schema in tqdm(self.mapping.items()):
            tqdm.write(f"Processing {topic}")
            schema = schema.replace("/msg/", "/")
            topic_name = _safe_topic_name(topic)


            kwargs = {}
            if schema in ("sensor_msgs/Image", "sensor_msgs/CompressedImage"):
                if export_assets:
                    topic_dir = os.path.join(out_dir, topic_name)
                    kwargs.update({"export": True, "export_dir": topic_dir})

            result = self.read_topic(topic, **kwargs)  # expects dict like {"df": df, ...}
            df = result if not isinstance(result, tuple) else result[0]

            if df_format == "csv":
                df_path = os.path.join(out_dir, f"{topic_name}.csv")
                df.to_csv(df_path, index=False)
            else:
                df_path = os.path.join(out_dir, f"{topic_name}.parquet")
                df.to_parquet(df_path, index=False)


        return out_dir

    def __repr__(self) -> str:
        if not hasattr(self, "recording_id") or self.recording_id is None:
            return (
                f"BagfileReader("
                f"recordings={len(self.recordings)} available, "
                f"no recording selected)"
            )

        parts = [
            f"recording='{self.recording_name}'",
            f"id='{self.recording_id}'",
            f"device_id='{self.device_id}'",
        ]

        if getattr(self, "start", None) is not None and getattr(self, "end", None) is not None:
            parts.append(f"time=[{self.start} → {self.end}]")

        if self.mapping:
            parts.append(f"topics={len(self.mapping)}")

        return f"BagfileReader({', '.join(parts)})"


    def get_event_overview(self) -> pd.DataFrame:
        events = self.client.get_events(
            device_id=self.device_id,
            start=self.start,
            end=self.end,
        )

        rows = []
        for evt in events:
            meta = evt.get("metadata") or {}
            if not isinstance(meta, dict):
                meta = {"metadata": meta}

            start = evt.get("start")
            end = evt.get("end")

            duration_s = None
            if start is not None and end is not None:
                # Handles datetime objects (aware or naive)
                start_ns = pd.Timestamp(start).value
                end_ns = pd.Timestamp(end).value
                duration_s = (end_ns - start_ns) * 1e-9

            row = {"event_id": evt.get("id"), "duration_s": duration_s}
            row.update(meta)
            rows.append(row)

        df = pd.DataFrame(rows)

        if df.empty:
            return pd.DataFrame(columns=["event_id", "duration_s"])

        core = ["event_id", "duration_s"]
        meta_cols = sorted(c for c in df.columns if c not in core)
        return df[core + meta_cols]
