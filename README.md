# 멀티채널 커머스 Lakehouse PoC

가상 고객사 A사의 세 판매 채널(A 자사몰, B 종합 오픈마켓, C 스포츠 전문몰)에서 나온 이벤트를 하나의 공통 모델로 통합하고, 결제일 기준 판매 지표를 만드는 PoC다.
이 문서는 AI(Claude)의 보조를 받아 작성한 코드와 문서를 포함한다. 역할 구분은 `CONTRIBUTIONS.md`에 기록한다.

## 데이터 흐름

```text
채널 원본(A/B/C) → HTTP → Azure Functions → Event Hubs → Bronze → Silver → Gold → Metric View → 대시보드
```

| 층 | 내용 |
|---|---|
| Bronze | 단일 테이블에 원본 JSON(`payload`)과 수신 메타데이터를 보존한다. 채널은 요청 URL 경로로 받아 `channel` 컬럼에 둔다. |
| Silver | 채널별 원본을 공통 이벤트 모델로 변환해 이벤트 유형별 5개 테이블(`orders`, `order_items`, `payments`, `refunds`, `order_cancellations`)에 쌓는다. 검증 실패는 `quarantine`으로 격리한다. |
| Gold | 결제 성공 주문상품(`fact_sales_item`)·환불(`fact_refund`) Fact와 채널·상품·결제수단·환불 프로필 Dimension (Kimball 방식, 일자 차원은 두지 않음). 구조 레퍼런스는 `01_contracts/data_fixtures/A사_ERD.pdf`이며 ②의 `dim_date`는 제외했다. |
| 지표 | 결제일 귀속(원거래 기준) 판매·환불 지표. 정의는 `01_contracts/contracts/event_spec.md` 6절. |

이벤트는 주문 생성, 주문 취소, 결제 성공, 결제 실패, 환불 성공 5종이다. 인증은 Managed Identity, 코드 배포는 GitHub Actions를 쓴다.

## 폴더

| 폴더 | 내용 |
|---|---|
| `01_contracts/` | 이벤트 명세, 시나리오·상품 기준 데이터, 고정 입력(fixtures)과 독립 기대값, 이벤트 생성기 |
| `02_pipeline/` | `ingestion`(수신·전송), `bronze`, `silver`, `gold`, `tests` |
| `03_serving/` | Metric View(`metric_views/`)와 대시보드(`dashboards/`), 벤치마크 참고 자료(`reference/`) |
| `04_experiments/` | 핵심 기능 완료 후의 부하·확장성·AI 비교 실험 |
| `plans/` | 단계별 계획과 완료 조건 |
| `.github/workflows/` | Function 배포 |

## 진행 상황

| 단계 | 상태 |
|---|---|
| 계획 01. 생성기, 수신·전송 | 완료. 채널당 24건, 합계 72건을 Event Hubs에서 확인했다 (2026-10-12). 잘못된 채널의 400 응답과 5xx 재시도는 로컬 단위 테스트로만 확인했다. |
| 계획 02. Bronze 적재 | 완료. Databricks 서버리스에서 72행과 `(channel, payload)` 일치를 확인하고 재실행해도 행 수가 유지됨을 확인했다 (2026-10-12). |
| 계획 03. Silver 변환 | 완료. 로컬 테스트와 Databricks 실행(서버리스)으로 `expected.json` 건수 일치를 확인했다 (2026-10-13). |
| 계획 04. Gold | 완료. 2026-10-14 일자 차원(`dim_date`) 제외를 반영했다. 로컬 테스트와 Databricks 재실행(사용자 보고): 테이블 건수 일치, 정합성 체크 OK. 출력 원문은 받지 못했다. |
| 계획 05. Metric View, 대시보드 | 완료 (일자 속성 기준 제외 반영, 2026-10-14). Databricks 재생성 후 채널별 `MEASURE()` 조회와 정합성 체크 OK(사용자 보고, 2026-10-14). 제외 이전 확인(사용자 보고, 2026-10-13): `check_metric_views` OK, 대시보드가 화면에 정상 출력. 기간·채널 필터 추가분은 **미검증**. 대시보드 파일은 Databricks에서 내려받은 것과 `uiSettings`만 다르다. |

범위 밖: 스키마 버전 처리, 상품 SCD Type 2, Backfill·Data Factory, Key Vault, 실행 모니터링, Linear 연동, Genie(대시보드 이후), 정량 성능 목표, 채널 간 고객 통합.

## 실행 (프로젝트 루트, PowerShell)

```powershell
# 전체 로컬 테스트 (표준 라이브러리만 사용)
python -m unittest discover -s .\02_pipeline\tests -v

# 층별 테스트 예
python -m unittest discover -s .\02_pipeline\tests\silver -v

# 이벤트 생성 (명세가 바뀔 때만 fixtures를 다시 만든다)
python .\01_contracts\src\generate.py
```

로컬 테스트 통과는 Databricks 실행 성공을 뜻하지 않는다. Databricks 노트북은 Git folder 동기화 후 Databricks에서 실행한다.

## 문서의 우선순위

최신 사용자 지시 > `AGENTS.md` > `plans/` > 각 폴더 README. 명세(`01_contracts/contracts/`)가 바뀌면 명세 → 입력 픽스처·독립 기대값 → 구현 → 테스트 순서로 진행한다. 독립 기대값은 구현을 맞추기 위해 수정하지 않는다.

`03_serving/reference/`의 스마트스토어 캡처에는 계정·사업자 정보가 있을 수 있다. 외부 공개 전에 마스킹한다.
