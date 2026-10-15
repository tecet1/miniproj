# 03_serving — Metric View, 대시보드

검증된 Gold 지표를 Metric View로 정의하고 대시보드로 보여준다. 구현·로컬 테스트 완료, Databricks 실행과 대시보드 출력은 사용자가 확인했다(보고 기준) (계획 `plans/05_Metric_View_대시보드.md`).

| 폴더 | 내용 |
|---|---|
| `metric_views/` | `sales_metrics`·`refund_metrics` 정의(`metric_defs.py`), 생성(`create_metric_views.py`), 자체 정합성 확인(`metric_check.py`), 실행 노트북(`run_metric_views.py`) |
| `dashboards/` | 화면 구성 가이드(`화면_구성_가이드.md`): 3페이지 대시보드의 데이터셋 SQL과 위젯 설명서. 대시보드는 Databricks 화면에서 직접 만들고 유지한다(생성기·JSON은 2026-10-15에 삭제, 이력은 git `beba3ef`) |
| `genie/` | 대시보드 이후의 후속 범위. 아직 만들지 않았다. |
| `reference/` | 벤치마크 참고 자료(스마트스토어 캡처 등). 계정·사업자 정보가 있을 수 있어 외부 공개 전에 마스킹한다. |

지표 정의는 `01_contracts/contracts/event_spec.md` 6.5절을 따른다.

## 실행 순서 (Databricks)

1. `02_pipeline/gold/run_gold.py`로 Gold를 만든다 (`check_gold` 통과 후).
2. `metric_views/run_metric_views.py`를 실행한다: `commerce_poc.serving`에 뷰 2개를 만들고 `check_metric_views`가 빈 목록(`OK`)인지 본다.
3. `dashboards/화면_구성_가이드.md`의 데이터셋 SQL과 위젯 설명서로 Databricks 화면에서 대시보드를 만든다.

주의: 카드 5-G가 두 Metric View에 `payment_week`를 추가했으므로 **2번을 반드시 다시 실행**한다.

정의(Metric View)를 바꾸면 `metric_views/run_metric_views.py`를 다시 실행하고, 화면의 데이터셋·위젯은 가이드의 "자주 하는 유지보수" 표를 따라 직접 고친다.
