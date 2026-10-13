# 03_serving — Metric View, 대시보드

검증된 Gold 지표를 Metric View로 정의하고 대시보드로 보여준다. 구현·로컬 테스트 완료, Databricks 실행과 대시보드 출력은 사용자가 확인했다(보고 기준) (계획 `plans/05_Metric_View_대시보드.md`).

| 폴더 | 내용 |
|---|---|
| `metric_views/` | `sales_metrics`·`refund_metrics` 정의(`metric_defs.py`), 생성(`create_metric_views.py`), 기대값 대조(`metric_check.py`), 실행 노트북(`run_metric_views.py`) |
| `dashboards/` | 대시보드 정의(`dashboard_spec.py`), JSON 생성기(`build_dashboard.py`), 생성 결과(`commerce_poc.lvdash.json`) |
| `genie/` | 대시보드 이후의 후속 범위. 아직 만들지 않았다. |
| `reference/` | 벤치마크 참고 자료(스마트스토어 캡처 등). 계정·사업자 정보가 있을 수 있어 외부 공개 전에 마스킹한다. |

지표 정의는 `01_contracts/contracts/event_spec.md` 6.5절을 따른다.

## 실행 순서 (Databricks)

1. `02_pipeline/gold/run_gold.py`로 Gold를 만든다 (`check_gold` 통과 후).
2. `metric_views/run_metric_views.py`를 실행한다: `commerce_poc.serving`에 뷰 2개를 만들고 `check_metric_views`가 빈 목록(`OK`)인지 본다.
3. `dashboards/commerce_poc.lvdash.json`을 Git folder로 동기화해 대시보드로 연다. 기간(결제일)·채널 필터는 모든 페이지 맨 위(전역 필터)에 있다. 위젯이 열리지 않으면 `dashboard_spec.py`의 데이터셋 쿼리로 화면에서 위젯을 다시 만든다.

정의를 바꾸면 대시보드 파일을 다시 만든다 (프로젝트 루트, PowerShell).

```powershell
python .\03_serving\dashboards\build_dashboard.py
python -m unittest discover -s .\02_pipeline\tests\serving -v
```
