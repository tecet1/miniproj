"""트리거 설정(pipeline_config.json)과 상시 스트림 노트북·Job 정의가 서로 맞는지 본다. Databricks 실행은 확인하지 못한다."""
import re
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
PIPELINE = ROOT / "02_pipeline"
sys.path[:0] = [str(PIPELINE), str(PIPELINE / "quality")]

import stream_config as C  # noqa: E402


def text(rel):
    return (PIPELINE / rel).read_text(encoding="utf-8")


class TestTriggerConfig(unittest.TestCase):
    def test_file_gives_a_valid_trigger_for_every_layer(self):
        for layer in C.LAYERS:
            trigger = C.trigger_for(layer)
            self.assertEqual(1, len(trigger), layer)
        self.assertEqual({"processingTime": "10 seconds"}, C.trigger_for("bronze"))
        self.assertEqual(60, C.trigger_seconds("gold"))

    def test_rejects_unknown_layer_and_malformed_triggers(self):
        good = {"triggers": {"bronze": {"availableNow": True}, "silver": {"processingTime": "1 second"}, "gold": {"processingTime": "30 seconds"}}}
        self.assertEqual({"availableNow": True}, C.trigger_for("bronze", good))
        self.assertIsNone(C.trigger_seconds("bronze", good))
        self.assertEqual(1, C.trigger_seconds("silver", good))
        with self.assertRaises(ValueError):
            C.trigger_for("platinum", good)
        for bad in ({"processingTime": "soon"}, {"processingTime": "0 seconds"}, {"availableNow": False}, {"once": True},
                    {"processingTime": "5 seconds", "availableNow": True}, {}):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                C.trigger_for("gold", {"triggers": {"gold": bad}})


class TestNotebooksAndJob(unittest.TestCase):
    NOTEBOOKS = {"bronze": "bronze/stream_bronze.py", "silver": "silver/stream_silver.py", "gold": "gold/stream_gold.py"}

    def test_each_stream_notebook_reads_its_own_trigger_from_the_config(self):
        for layer, rel in self.NOTEBOOKS.items():
            with self.subTest(layer=layer):
                body = text(rel)
                self.assertRegex(body, rf'trigger_(for|seconds)\("{layer}"\)')
                self.assertNotIn("processingTime", body)      # 간격은 설정 파일에만 둔다
                self.assertNotIn("availableNow=True", body)

    def test_every_job_task_points_at_an_existing_notebook(self):
        job = text("jobs/pipeline_job.yml")
        paths = re.findall(r"notebook_path: \.\./(\S+)", job)
        self.assertEqual(4, len(paths))
        for rel in paths:
            self.assertTrue((PIPELINE / f"{rel}.py").exists(), rel)

    def test_reconcile_schedule_matches_between_config_and_job(self):
        cron = re.search(r'quartz_cron_expression: "([^"]+)"', text("jobs/pipeline_job.yml")).group(1)
        self.assertEqual(C.load_config()["reconcile_schedule"], cron)

    def test_job_has_no_secret_or_hardcoded_cluster(self):
        job = text("jobs/pipeline_job.yml")
        self.assertNotRegex(job, r"(?i)password|secret|sas|endpoint=")
        self.assertIn("${var.cluster_id}", job)


if __name__ == "__main__":
    unittest.main()
