# 02_pipeline — 수신, Bronze, Silver

모든 채널(A/B/C)이 같은 경로를 쓴다: HTTP → Azure Functions → Event Hubs → Bronze → Silver.

| 폴더 | 내용 | 상태 |
|---|---|---|
| `ingestion/` | HTTP 수신(Function)과 Event Hubs 전송, 송신·도착 확인 스크립트 | 완료. 원격에서 72건 도착 확인 |
| `bronze/` | Event Hubs → `commerce_poc.bronze.events` 적재와 대조 함수, 실행 노트북 | 완료. Databricks에서 72행 확인 |
| `silver/` | Bronze → 공통 이벤트 → Silver 테이블·Quarantine. 채널별 변환은 `channels/`, 공통 처리는 별도 파일 | 진행 중. 채널 변환·행 단위 검증은 로컬 완료, Spark 적재는 뼈대와 테스트만 있음(구현·Databricks 실행 전) |
| `tests/` | `bronze`, `contracts`, `ingestion`, `silver` 하위 폴더. 고정 입력과 독립 기대값으로 비교한다. | 로컬 실행 |

Gold는 아직 시작하지 않았다. 단계별 목표와 완료 조건은 `plans/`를 따른다.

## 실행 (프로젝트 루트, PowerShell)

```powershell
python -m unittest discover -s .\02_pipeline\tests -v
```

Spark 로직은 로컬에서 검증하지 않는다. Databricks 노트북은 함수 호출만 하며 실행 결과를 확인하기 전에는 '미검증'으로 적는다.
