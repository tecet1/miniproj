"""MERGE 문장을 만들고 실행한다. 기준은 event_spec.md 6.4절이다.

  merge_sql   키 기준 MERGE 문장. 대상에 있으면 갱신, 없으면 추가한다.
              delete_missing=True면 새 결과에 없는 대상 행을 삭제한다 (Fact: 재계산 결과와 같게 맞춘다).
  merge_rows  DataFrame을 임시 뷰로 만들어 merge_sql을 실행한다.
`UPDATE SET *`·`INSERT *`는 소스와 대상의 컬럼 이름이 같아야 하고, `NOT MATCHED BY SOURCE`는 Databricks Runtime 12.2 이상이다.
소스에 같은 키가 둘일 때: 대상에 이미 있는 키는 MERGE가 오류를 내지만(여러 소스 행이 한 대상 행에 일치), 대상에 없는 키는
오류 없이 소스의 중복 행이 모두 삽입될 수 있다. 그래서 gold_build가 MERGE 전에 소스 PK 중복을 따로 검사한다 (event_spec.md 6.4절).
pyspark는 import하지 않는다 (spark·DataFrame 객체를 받아 쓴다).
"""


def merge_sql(target: str, view: str, keys: list[str], delete_missing: bool = False) -> str:
    """target 테이블에 view를 keys 기준으로 MERGE하는 문장."""
    condition = " AND ".join(f"t.{key} = s.{key}" for key in keys)
    sql = (f"MERGE INTO {target} AS t USING {view} AS s ON {condition} "
           f"WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")
    if delete_missing:
        sql += " WHEN NOT MATCHED BY SOURCE THEN DELETE"
    return sql


def merge_rows(spark, df, target: str, keys: list[str], delete_missing: bool = False) -> None:
    """df를 임시 뷰("gold_src_<테이블>")로 등록하고 target에 MERGE한다."""
    view = "gold_src_" + target.split(".")[-1]
    df.createOrReplaceTempView(view)
    spark.sql(merge_sql(target, view, keys, delete_missing))
