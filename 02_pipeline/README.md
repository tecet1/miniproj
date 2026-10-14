# 02_pipeline — 수신, Bronze, Silver, Gold

모든 채널(A/B/C)이 같은 경로를 쓴다: HTTP → Azure Functions → Event Hubs → Bronze → Silver → Gold.

| 폴더 | 내용 | 상태 |
|---|---|---|
| `ingestion/` | HTTP 수신(Function)과 Event Hubs 전송, 송신·도착 확인 스크립트 | 완료. 원격에서 72건 도착 확인 |
| `bronze/` | Event Hubs → `commerce_poc.bronze.events` 적재와 대조 함수, 실행 노트북 | 완료. Databricks에서 72행 확인 |
| `silver/` | Bronze → 공통 이벤트 → Silver 테이블·Quarantine. 채널별 변환은 `channels/`, 공통 처리는 별도 파일 | 완료. 로컬 테스트와 Databricks 실행으로 `expected.json` 건수 일치 확인 |
| `gold/` | Silver → Fact 2개·Dimension 4개. Fact SQL(`gold_sql.py`), Dimension(`dimensions.py`), MERGE(`gold_merge.py`), 실행(`gold_build.py`), 대조(`gold_check.py`), 노트북(`run_gold.py`) | 구현·로컬 테스트 완료. 일자 차원 제외(2026-10-14) 후 Databricks 재실행에서 건수 일치와 정합성 체크 OK(사용자 보고) |
| `tests/` | `bronze`, `contracts`, `gold`, `ingestion`, `serving`, `silver` 하위 폴더. 고정 입력과 독립 기대값으로 비교한다. | 로컬 실행 |

Metric View와 대시보드(`03_serving`)는 구현·로컬 테스트 완료, Databricks 실행과 대시보드 출력은 사용자 보고로 확인했다(기간·채널 필터 추가분은 미검증). 단계별 목표와 완료 조건은 `plans/`를 따른다.

## 실행 (프로젝트 루트, PowerShell)

```powershell
python -m unittest discover -s .\02_pipeline\tests -v
```

저장 위치: Bronze·Silver·Gold 노트북이 스키마를 만들 때 `MANAGED LOCATION`으로 ADLS 경로(`abfss://commerce-poc@swleeteststorage01.dfs.core.windows.net/storage/{bronze,silver,gold}`)를 지정한다. 테이블과 체크포인트 볼륨은 그 아래에 저장된다. 스키마가 이미 있으면 위치는 바뀌지 않으므로 지우고 다시 만들어야 한다. Databricks 반영은 **미검증**.

Spark 로직은 로컬에서 검증하지 않는다. Databricks 노트북은 함수 호출만 하며 실행 결과를 확인하기 전에는 '미검증'으로 적는다.
