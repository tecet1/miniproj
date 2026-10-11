# 채널 A Silver 변환·저장

- **변경:** 로컬 변환에 Spark 타입·Bronze 연결·Quarantine·멱등 Delta 저장 코드를 추가했다.
- **추가:** 코드 준비와 원격 실행 검증을 구분한다. Databricks의 실제 Silver 적재 성공은 아직 확인하지 않았다.

## 파일과 책임

- 순수 변환은 channel_a.py, DataFrame 분기/저장은 silver_pipeline.py다.
- 실행과 상태 확인은 노트북, 반복 시험은 별도 검증 노트북으로 분리했다.

| 파일 | 역할 |
|---|---|
| channel_a.py | 원본 v1/v2 → JSON 호환 공통 이벤트, 구조화된 입력 오류 |
| silver_pipeline.py | 명시적 Spark schema, 분산 변환, 중복/충돌 분기, Delta MERGE |
| 02_bronze_to_silver.ipynb | 실제 Bronze → Silver/Quarantine 처리·상태·고정 6건 검산 |
| 03_validate_silver.ipynb | 별도 테이블의 오류·재전송·부분 저장 실패·체크포인트 재시작 검증 |

계약은 01_contracts/contracts/data_quality.md와 common_event.md다. 데이터와 예상 결과는 01_contracts/data/fixtures/channel_a에 있다.

## 함수·모듈·문법 선택

- 업무 규칙을 두 벌로 구현하지 않고 기존 순수 함수를 Spark 작업자에서 재사용한다.
- 클래스는 안정적인 오류 속성을 가진 DataQualityError 한 개뿐이며 범용 설정 엔진은 없다.

DataQualityError는 ValueError를 상속하여 기존 호출과 호환되며 rule_id/code를 속성으로 전달한다. 예외 문자열을 잘라 오류 종류를 추측하지 않는다. inspect_raw_event는 이 예상 입력 오류만 잡고 버그·쓰기 실패는 숨기지 않는다.

json.loads의 parse_constant는 NaN/Infinity를 차단한다. json.dumps의 sort_keys=True와 고정 separators로 dict 키 순서 차이를 없앤다. test_run_id/published_at을 제외하는 dict comprehension은 원본 객체를 변경하지 않고 오류 fingerprint를 만든다. SHA-256은 전송 메타데이터 없이 동일 업무/오류를 식별한다.

functools.partial은 매핑과 저장 대상을 미리 묶는다. foreachBatch가 전달하는 (DataFrame, batch_id)와 순수 함수의 매핑 인자를 연결하기 위해 사용한다. F.udf(..., INSPECTION_SCHEMA)는 driver collect 없이 작업자에서 원문 하나씩 처리한다. Python 실행/직렬화 비용이 있으므로 성능 최적화 완료라고 표현하지 않는다.

from_json(..., COMMON_SCHEMA)는 UTC Timestamp와 Decimal(18,0)을 명시한다. 단일 Delta 테이블의 data/items는 다섯 이벤트 필드의 합집합 struct다. 해당 유형에서 쓰지 않는 필드는 null이다. 물리 nullable과 이벤트별 필수값 검증은 별개다. ArrayType도 Delta의 저장 nullability와 호환되도록 선언하고 배열 안 null은 변환 규칙으로 거절한다.

join으로 기존 키를 비교하고, 새 키의 countDistinct(business_hash) > 1을 충돌로 분리한다. left_anti join은 충돌 키를 정상 후보에서 제외한다. Window.partitionBy와 row_number는 같은 값의 여러 전달 중 최초 출처 한 행을 고른다. MERGE 전에 source 키를 유일하게 만들어 복수 일치 오류를 방지한다.

persist/count는 정상·격리 판정을 첫 쓰기 전에 고정한다. finally의 unpersist는 저장 실패 시에도 캐시를 해제한다. whenNotMatchedInsertAll만 사용해 저장된 값·최초 시각을 덮어쓰지 않는다. batch_id를 저장 키로 쓰지 않아 다른 실행/배치의 동일 이벤트도 중복 처리한다.

## 실행 준비

- 일반 컴퓨트 DBR 16.1 이상과 최신 Databricks Git 폴더를 사용한다.
- 노트북 하나만 Import하면 인접 모듈·매핑이 없으므로 저장소 파일도 함께 필요하다.

1. 이 변경을 GitHub에 commit/push하고 Databricks Git 폴더에서 pull한다.
2. project_root에 저장소 루트 /Workspace/.../databricks_project를 입력한다. 자동 탐색에 성공하면 비워도 된다.
3. commerce_poc의 스키마/테이블 생성·읽기/쓰기와 /Volumes/commerce_poc/bronze/checkpoints 쓰기 권한을 확인한다.
4. 03_validate_silver.ipynb를 별도 테스트 스키마에서 먼저 실행한다.
5. 검증 후 02_bronze_to_silver.ipynb로 기존 commerce_poc.bronze.events를 처리한다.

Databricks에는 PySpark/Delta가 제공되므로 이 노트북에서 로컬 테스트용 버전을 설치하지 않는다. 모듈 변경 후에는 기존 스트림을 중지하고 Python 실행 상태를 새로 시작해 최신 코드로 실행한다.

## 트리거·저장·확인

- availableNow는 적재된 Bronze를 처리하고 종료한다. 실시간 확인은 live를 선택한다.
- Bronze와 Silver의 체크포인트는 별개이며 같은 대상에는 writer 하나만 둔다.

정상 대상 commerce_poc.silver.events, 오류 대상 commerce_poc.silver.quarantine, 체크포인트 /Volumes/commerce_poc/bronze/checkpoints/bronze_to_silver_a_v1이다. 코드가 맞지 않는 기존 테이블은 자동 변형하지 않고 TARGET_SCHEMA_MISMATCH로 멈춘다. 기존 테이블을 삭제하거나 overwrite하여 해결하지 않는다.

고정 6개 ID는 정상 6개, 해당 ID 오류 0개를 기대한다. 앞선 단건과 전체 송신의 첫 ID가 같으므로 Bronze 전달 행 수와 Silver 업무 이벤트 수는 다르다. 실행 ID는 중복 키가 아니다.

두 Delta 테이블의 커밋은 원자적이지 않다. Quarantine 쓰기 후 Silver가 실패하면 같은 입력 재시도로 정상 상태에 도달한다. 다중 writer·수정 이벤트 UPDATE·범용 오류 재평가·대용량 성능 검증은 현재 보장에 포함하지 않는다.

## 검증과 근거

- 기본 로컬 테스트는 외부 패키지 없이 실행하며 Spark 테스트는 명시적으로 선택한다.
- Delta 쓰기·foreachBatch·원격 체크포인트 검증 결과는 Databricks 실행 후 기록한다.

python -m unittest discover -s 02_pipeline/tests -v

RUN_SPARK_TESTS=1을 설정하고 PySpark와 지원 Java가 있는 환경에서 Spark 테스트를 추가 실행할 수 있다. 테스트의 작은 collect는 기대값 검사 용도이며 처리 코드에는 driver collect가 없다.

참고: [foreachBatch와 실패/재시도](https://learn.microsoft.com/en-us/azure/databricks/structured-streaming/foreach), [Delta MERGE](https://learn.microsoft.com/en-us/azure/databricks/delta/merge), [작업자 모듈 배포](https://docs.databricks.com/aws/en/files/workspace-modules).

## 이번 로컬 실행 결과

- **추가:** 기본 Python 검사 24개와 Spark DataFrame 검사 6개 통과. 오류 16개도 독립 기대 코드와 일치했다.
- **추가:** evidence/local_validation_2026-10-12.json에 환경·검증 범위·남은 원격 검증을 기록했다.

Windows/Python 3.12의 worker 종료 후 기존 Python 3.11.2 + PySpark 3.5.3 + Java 8로 로컬 Spark 검사를 수행했다. 이는 Databricks 컴퓨트의 Python 버전을 바꾸라는 의미가 아니다. Delta 저장과 원격 노트북 실행은 아직 미검증이다.

## 테이블 생성 옵션 오류 수정

- **변경:** `mode("errorifexists").saveAsTable(...)`에서 **`writeTo(...).using("delta").create()`**로 초기 생성 API를 바꿨다.
- **추가:** 실제 환경의 `[UNSUPPORTED_OPERATION] errorifexists is not supported` 보고를 반영했다. 수정 후 원격 실행 결과는 아직 확인하지 않았다.

`writeTo(table)`은 대상 테이블, `using("delta")`는 저장 형식, `create()`는 신규 생성이다. 기존 대상은 `tableExists` 분기에서 생성 호출을 건너뛰고 명시적 스키마와 비교한다. 검증 노트북의 새 실행별 Bronze도 같은 생성 API를 사용한다. MERGE와 availableNow는 이 변경의 대상이 아니다.

GitHub에 변경을 반영하고 Databricks Git 폴더에서 Pull한다. 실행 중인 Silver 쿼리가 있다면 먼저 중지한다. `importlib.reload(pipeline)`로 모듈을 다시 불러온 뒤 테이블 설정 셀부터 재실행한다.

참고: [Databricks DataFrameWriterV2](https://learn.microsoft.com/en-us/azure/databricks/pyspark/reference/classes/dataframewriterv2).

## 프로덕션 전환 시 재설계 방향

- **추가:** 사용자 피드백: 현재 가독성과 노트북별 경로/옵션 수동 설정의 부담이 크다. 지금은 PoC 완료를 우선한다.
- **추가:** 프로덕션 전환 시 환경 설정의 단일화, 실행/배포 자동화, 변환·검증·저장 책임과 명시적인 입출력 구조를 재설계한다.

현재 코드의 파일 배치를 프로덕션 구조로 고정하지 않는다. 데이터 계약·독립 기대 결과·검증된 순수 변환 규칙은 새 구현의 정확성을 검증하는 기준으로 유지한다. 프로덕션용 전면 재설계는 PoC 완료 후 실제 운영 요구에 맞춰 진행한다.
