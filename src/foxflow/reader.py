# foxflow/reader.py
from __future__ import annotations
from foxglove.client import Client
import pandas as pd
import requests
import os
from tqdm import tqdm
import shutil

from foxflow.parsers import load_plugins
from foxflow.registry import get_parser, list_schemas
from foxflow.utils import get_timestamp_ns, _safe_topic_name
from foxflow.parsers.generic import parse_generic




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


    def read_events(self, topic: str, **kwargs):
        if topic not in self.mapping:
            raise ValueError(f"Topic '{topic}' not found in recording.")

        schema = self.mapping[topic].replace("/msg/", "/")
        parser = self._get_parser(schema)

        events = self.client.get_events(
            device_id=self.device_id,
            start=self.start,
            end=self.end,
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

            # parsed is ALWAYS a dict: {"df": df, ...}
            parsed = parser(messages_iter, **kwargs)

            results[evt["id"]] = {
                "event": evt,
                **parsed,   # ← THIS is the important part
            }

        return results


    def get_events(self):
        return self.client.get_events(device_id=self.device_id, start=self.start, end=self.end)


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



