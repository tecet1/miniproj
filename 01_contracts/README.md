# M1: 최소 계약과 고정 입력

- 채널 A의 6개 이벤트, 2개 SKU 매핑, 독립 기대 결과를 보관한다.
- 실제 Shopify 필드를 재현하지 않고, 통합에 필요한 최소 프로젝트 원본 계약을 사용한다.
- **추가: A v1/v2 입력과 완전한 공통 기대 행을 제공한다. 상품 변경 이력도 함께 관리한다.**

## 계약 문서

- **변경: channel_a.md를 channels.md로 통합했다.**
- **추가: 공통 이벤트와 상품 차원의 계약을 분리했다.**

contracts/channels.md는 송신 경로·원본 버전·변환을, common_event.md는 Silver 업무 구조를, product_dimension.md는 Gold 상품 이력을 정의한다. 문서는 실행 코드가 자동으로 읽는 설정이 아니다. 구현과 테스트가 계약을 따르도록 유지한다.

## 실행

- 외부 패키지가 필요 없는 Python 코드다.
- 같은 코드를 재실행하면 같은 이벤트 ID·시각·본문이 생성된다.

프로젝트 루트에서 실행한다.

```powershell
python .\01_contracts\src\generate_channel_a.py
python .\01_contracts\src\generate_channel_a.py --schema-version 2.0
```

`data/fixtures/channel_a/events.jsonl`의 한 줄이 한 메시지다. Event Hubs에는 각 줄을 별도로 송신한다. 전체 파일을 한 메시지로 보내지 않는다.

**추가:** 첫 명령은 기존 events.jsonl, 두 번째는 별도 events_v2.jsonl을 만든다. 원본 형식은 달라도 같은 여섯 업무 이벤트를 나타내므로 새 거래 12개로 계산하지 않는다.

## 코드의 구성

- 데이터 작성과 파일 저장을 분리하여 이후 송신 코드에서도 이벤트를 재사용한다.
- 작은 고정 fixture에는 딕셔너리와 함수만으로 충분하므로 dataclass·UUID·범용 설정 기능을 추가하지 않았다.

| 함수 | 역할 |
|---|---|
| `make_item` | 원본 상품 라인 생성 |
| `make_event` | 공통 원본 헤더와 고정 ID·시각 생성 |
| `build_events` | 주문 생성→취소→새 주문→결제 실패→성공→부분 환불 조립 |
| `write_events` | 이벤트들을 JSONL로 저장 |

`datetime`·`timedelta`는 시간대가 있는 기준 시각에서 1분 간격을 계산한다. UTC를 사용하며 실제 생성·송신 시각으로 해석하지 않는다.

`make_event`의 `*` 뒤 인자는 이름으로만 전달한다. 주문 번호·payload·이벤트 이름의 위치 혼동을 줄인다. `payload: dict`는 타입 힌트이며 자동 검증은 아니다.

`f"a-poc-v1-{sequence:02d}"`는 두 자리 번호를 채운 고정 시험 ID다. 현재 여섯 이벤트의 재생용일 뿐, 대용량 생성기의 ID 정책은 아니다. 새 주문을 만드는 시험과 같은 메시지를 재전송하는 시험을 구분해야 한다.

`for event in events`는 이벤트를 순서대로 직렬화한다. `json.dumps(..., ensure_ascii=False, allow_nan=False)`는 한국어를 유지하고 표준 JSON에 없는 NaN 값을 차단한다. 파일 객체를 자동으로 닫기 위해 `with`를 사용했다.

`Path`의 `/`는 경로 조합이다. 코드 위치를 기준으로 출력 폴더를 계산하므로 실행한 터미널 위치에 영향을 받지 않는다. `mkdir(parents=True, exist_ok=True)`는 필요한 부모 폴더를 만들고 기존 폴더를 허용한다.

`if __name__ == "__main__"`은 직접 실행할 때만 저장을 수행하게 한다. 다른 모듈에서는 `build_events()`를 import하여 파일을 거치지 않고 송신에 사용할 수 있다.

## 검증 기준

- `expected.json`의 건수·금액·환불 참조는 생성 함수에서 계산하지 않은 독립 기대값이다.
- Azure 송신 및 Bronze/Silver 처리는 후속 단계에서 검증한다.
- **추가: expected_common_events.json은 v1/v2가 도달해야 하는 6개 공통 행 전체다. product_changes.json은 상품 속성의 전체 변경 이력이다.**

실행 검사: `python -m unittest discover -s 02_pipeline/tests -v`. 로컬 Python 변환과 SCD를 확인하며 원격 Spark·Delta 저장 성공을 의미하지 않는다.

정상 입력 6개 → 고유 Silver 이벤트 6개, 격리 0개가 기대값이다. 순결제금액은 80,000 - 25,000 = 55,000원이다. 미결제 취소액을 다시 차감하지 않는다.

같은 메시지를 정확히 한 번 더 전달하면 정상적인 입력 전달 조건에서 Bronze 12행, Silver 고유 이벤트 6행을 기대한다. SDK의 추가 전달이 발생하면 Bronze는 더 많을 수 있다.

~~현재 단계에는 필수값 누락·미매핑·파싱 오류 입력을 추가하지 않았다.~~ **변경: quality_cases.json의 오류 16개, replay_events.jsonl의 v1/v2 재전송, conflict_events.jsonl의 키 충돌과 독립 quality_expected.json을 추가했다. data_quality.md에서 규칙·Quarantine 계약을 정의한다.**
