# 멀티채널 커머스 Lakehouse PoC

- 서로 다른 채널 원본을 공통 Silver 이벤트로 통합하고 이후 Gold·대시보드·Genie로 연결한다.
- **변경: 책임별 폴더로 통합하고, 원본 버전 처리와 상품 SCD Type 2 로컬 로직을 추가했다.**
- **변경: GitHub Actions Function 배포는 성공했고, 사용자 공유 결과에서 A의 Bronze 6건 적재를 확인했다. Spark Silver 적재·Gold Delta 저장은 아직 미검증이다.**

## 폴더 구성

- **변경: 채널 A/B/C는 같은 파이프라인을 사용하며, Linear M1~M5는 구현 순서를 관리한다.**
- **변경: 기존 스마트스토어 이미지·HTML·설계 메모는 03_serving으로 이동했다.**

| 이전 폴더 | 변경 후 |
|---|---|
| ~~02_channel_a, 03_multichannel~~ | **02_pipeline** |
| ~~04_consumption~~ | **03_serving** |
| ~~05_experiments~~ | **04_experiments** |
| ~~contracts/channel_a.md~~ | **contracts/channels.md** |

```text
miniproj/
├── 01_contracts/
│   ├── contracts/              # channels, common_event, product_dimension
│   ├── src/                    # 고정 원본 생성
│   └── data/                   # 입력·기대 행·SKU 매핑·상품 변경 이력
├── 02_pipeline/
│   ├── ingestion/              # A 송신 경로 확인, B/C 후속 구현
│   ├── bronze/                 # A 6건 원문 적재 확인·실행 근거
│   ├── silver/                 # A v1/v2 → 공통 이벤트
│   ├── gold/                   # 상품 SCD2·시점 조회·Delta 저장 함수
│   ├── tests/                  # 로컬 검증
│   └── planning/               # 기존 M2/M3 안내 보존
├── 03_serving/                  # 대시보드·Genie·참고 자료
└── 04_experiments/              # 후속 비교 실험
```

## 실행과 현재 완료 범위

- **추가: 원본 v1/v2는 업무 내용과 event_id가 같은 두 표현이며, 새 거래 12개가 아니다.**
- **추가: 상품 SCD는 작은 전체 변경 이력을 재계산한다. 증분 스트리밍 SCD라고 주장하지 않는다.**

프로젝트 루트에서 실행한다.

```powershell
python .\01_contracts\src\generate_channel_a.py
python .\01_contracts\src\generate_channel_a.py --schema-version 2.0
python -m unittest discover -s .\02_pipeline\tests -v
```

계약 버전 처리는 source schema_version에 따라 payload.total 또는 payload.order_total을 읽고 같은 공통 행을 만든다. 지원하지 않는 버전과 필수 필드 누락은 실패한다. 공통 행 6개의 독립 기대값과 비교한다.

상품 SCD는 product_name·category·brand를 추적한다. 변경 전 행을 보존하고 유효기간과 안정적인 product_key를 만든다. 10월 10일 SKU-001 거래는 '의류', 변경 이후 거래는 '아웃도어'로 조회된다. 전체 상품 변경 이력을 제공해야 한다.

로컬 정상 변환·자료형·미매핑·SCD 재생·유효기간 경계·충돌을 테스트한다. ~~단일 Silver 테이블의 Spark schema·Quarantine·배치 간 멱등 저장과 원격 Delta 실행은 미구현 또는 미검증~~ **변경: Spark schema·Quarantine·멱등 MERGE 코드와 실행/검증 노트북을 추가했다. 원격 Delta 실행은 미검증이며 로컬 검사로 완료 처리하지 않는다.**

**변경: 사용자 공유 결과로 A의 HTTP → Event Hubs → Bronze 6건을 확인했다.** 공유 원문을 로컬 변환한 결과도 독립 기대 공통 이벤트와 일치한다. ~~다음 작업은 명시적 Spark 공통 스키마·오류 격리·이벤트 ID 기준 멱등 Delta 저장이다.~~ **변경: 해당 코드를 준비했다. 다음 작업은 별도 Databricks 테스트와 실제 Bronze→Silver 실행 결과 확인이다.** 실행 근거는 02_pipeline/bronze/evidence에 보관한다.

Linear의 기존 SCD 제외 문구·보일러플레이트 협업 문구보다 최신 사용자 승인과 AGENTS.md를 우선한다. 이번 반영 대상은 로컬 miniproj다.
