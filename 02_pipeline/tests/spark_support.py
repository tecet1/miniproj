"""로컬 Spark 세션 도우미. pyspark와 Java가 있을 때만 세션을 만들고, 없으면 None을 돌려줘서 테스트가 건너뛴다.

준비 (한 번): 프로젝트 루트의 .venv에 pyspark를 설치하고 JDK 17 또는 21을 ~/.jdks 아래에 둔다.
  python -m venv .venv ; .\\.venv\\Scripts\\python.exe -m pip install pyspark install-jdk
  .\\.venv\\Scripts\\python.exe -c "import jdk; jdk.install('21', path=r'C:/Users/<사용자>/.jdks')"
실행: .\\.venv\\Scripts\\python.exe -m unittest discover -s .\\02_pipeline\\tests -t .\\02_pipeline\\tests
Databricks와 완전히 같지는 않다 (로컬 Spark 4.x, 델타·유니티 카탈로그 없음). 표현식과 SQL 문법·의미 확인용이다.
"""
import os
import sys
import tempfile
from pathlib import Path

_SESSION = {}


def _find_java_home():
    if os.environ.get("JAVA_HOME") and Path(os.environ["JAVA_HOME"], "bin").exists():
        return os.environ["JAVA_HOME"]
    candidates = sorted((Path.home() / ".jdks").glob("jdk-*"), reverse=True)
    return str(candidates[0]) if candidates else None


def local_spark():
    """로컬 SparkSession. pyspark나 Java가 없으면 None."""
    if "spark" in _SESSION:
        return _SESSION["spark"]
    spark = None
    try:
        import pyspark  # noqa: F401
        java_home = _find_java_home()
        if java_home:
            os.environ["JAVA_HOME"] = java_home
            os.environ["PATH"] = str(Path(java_home, "bin")) + os.pathsep + os.environ["PATH"]
            os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
            from pyspark.sql import SparkSession
            warehouse = tempfile.mkdtemp(prefix="spark-warehouse-")
            spark = (SparkSession.builder.master("local[2]").appName("local-tests")
                     .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2")
                     .config("spark.sql.warehouse.dir", warehouse).config("spark.sql.session.timeZone", "UTC")
                     .config("spark.driver.host", "127.0.0.1").getOrCreate())
            spark.sparkContext.setLogLevel("ERROR")
    except Exception:   # noqa: BLE001  pyspark가 없거나 Java가 동작하지 않으면 테스트를 건너뛴다
        spark = None
    _SESSION["spark"] = spark
    return spark
