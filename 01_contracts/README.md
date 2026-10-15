# 01_contracts — 명세, 고정 입력, 생성기

이 폴더가 프로젝트의 단일 기준이다. 파이프라인 구현은 여기의 명세를 따른다.

## 구성

| 위치 | 내용 |
|---|---|
| `contracts/event_spec.md` | 이벤트 명세: 공통 이벤트 모델, Silver 테이블 키·컬럼(1.3절), 채널별 원본 형식과 매핑, 처리 규칙, Gold·지표(6절) |
| `contracts/scenarios.md`, `scenarios.json` | 소량 정답 대조 세트. 9개 시나리오를 세 채널에 같은 내용으로 적용한다. 값은 `scenarios.json`이 기준이다. |
| `contracts/sku_spec.md`, `skus.json` | 상품 기준 데이터. 모든 채널이 같은 SKU(`product_id`)를 쓴다. |
| `data_fixtures/A사_ERD.pdf` | 테이블 구조 레퍼런스(① Bronze·Silver, ② Gold, ③ 적재 흐름·Metric View). Gold는 이 ERD를 따르고, 명세와 어긋나면 사용자에게 확인한다. |
| `data_fixtures/{A,B,C}/events.jsonl` | 채널별 고정 입력. 한 줄이 `{"channel","payload"}`이며 채널당 24건이다. |
| `src/` | 이벤트 생성기. 구성은 `src/README.md` 참고 |

## 변경 규칙

- `contracts/`는 사용자 승인 후에만 바꾼다.
- `data_fixtures/`는 구현 작업에서 수정하지 않는다. 명세가 바뀔 때만 명세 → 입력 순서로 다시 만든다.

## 실행 (프로젝트 루트, PowerShell)

```powershell
python -m unittest discover -s .\02_pipeline\tests\contracts -v
python .\01_contracts\src\generate.py
```

생성기는 같은 입력으로 몇 번을 실행해도 같은 `events.jsonl`을 만든다 (시각을 시나리오에서 고정하기 때문).
