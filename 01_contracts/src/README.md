# 이벤트 생성기

고정 시나리오(`contracts/scenarios.json`)와 상품(`contracts/skus.json`)에서 채널 A·B·C의 원본 이벤트를 만든다.
표준 라이브러리만 사용한다.

## 데이터 흐름

```
scenarios.json ─► scenario_loader ─► scenario_validator ─► canonical ─► channel_a/b/c ─► (faults) ─► generate ─► events.jsonl
skus.json      ─►                      │ pricing                       │ timeutil
                                       └ 금액·배송비·환불 계산           └ 채널별 시각 형식
expected.py: 덤프 기대값(04_experiments) 계산 전용. 고정 픽스처 기대값(expected.json)은 제거했다
```

| 모듈 | 하는 일 | 알아야 할 것 |
|---|---|---|
| `timeutil` | datetime → 채널별 시각 문자열·숫자 | UTC와 KST 날짜가 다를 수 있음(S23) |
| `pricing` | 금액·배송비·환불 금액 | 규칙은 `scenarios.md` |
| `ids` | ID 생성 3종: `ReadableIds`(픽스처 규칙), `NamedIds`(이름 기반 UUID v5, 시드 고정), `RandomIds`(UUID v4) | 규칙은 `event_spec.md` 1.1절. 생략하면 `ReadableIds` |
| `scenario_loader` | JSON 읽기 | 파일 읽기만 담당 |
| `scenario_validator` | 시나리오가 SKU·이벤트 정의대로인지 검사 | 읽기와 검사를 분리 |
| `canonical` | 시나리오 → 채널 중립 이벤트 | 채널을 모르는 로직. ID·시각·금액이 여기서 정해짐 |
| `channel_a/b/c` | 중립 이벤트 → 채널 원본 dict | 필드명·코드·시각 형식만 다름. 매핑표는 이미 채워져 있음 |
| `faults` | 원본에 오류 주입 | 채널별 위치는 `FAULT_PATHS` |
| `generate` | 줄 만들기와 파일 쓰기 | `{"channel","payload"}` 한 줄 |
| `expected` | 덤프 기대값 계산 | 덤프 전용으로 남겼다. 고정 픽스처의 기대값 파일과 비교 검증은 제거했다 |

## 모듈과 테스트

앞 모듈일수록 다른 모듈에 의존하지 않는다. 모든 모듈이 구현되어 있고 테스트는 `02_pipeline/tests/contracts`에 있다.

| 순서 | 모듈 | 테스트 |
|---|---|---|
| 1 | `timeutil.py` | `test_timeutil` |
| 2 | `pricing.py` | `test_pricing` |
| 2 | `ids.py` | `test_ids` |
| 3 | `scenario_loader.py` | `test_scenario_loader` |
| 4 | `scenario_validator.py` | `test_scenario_validator` |
| 5 | `canonical.py` | `test_canonical` |
| 6 | `channel_a.py`, `channel_b.py`, `channel_c.py` | `test_channel_a`, `test_channel_b`, `test_channel_c` |
| 7 | `faults.py` | `test_faults` |
| 8 | `generate.py` | `test_generate` |
| 9 | `expected.py` (덤프 전용) | `test_expected` |

## 실행 (프로젝트 루트, PowerShell)

```powershell
# 모듈 하나만 테스트
python -m unittest discover -s .\02_pipeline\tests\contracts -p "test_timeutil.py" -v

# 계약 테스트 전체
python -m unittest discover -s .\02_pipeline\tests\contracts -v

# 이벤트 생성
python .\01_contracts\src\generate.py
```

## 환불 수량 필드

수량 일부 환불을 위해 환불 이벤트에 수량 필드를 둔다 (A `refund_quantity`, B `refundQty`, C `rfd_qty`). 이벤트 명세 `event_spec.md`에 있다.
