# 03_serving — Metric View, 대시보드

검증된 Gold 지표를 Metric View로 정의하고 대시보드로 보여준다. 아직 시작하지 않았다.

| 폴더 | 내용 |
|---|---|
| `metric_views/` | 지표 정의 (판매·환불 지표, 분석 기준: 일자·채널·상품·결제수단·환불 유형·사유) |
| `dashboards/` | 대시보드 |
| `genie/` | 대시보드 이후의 후속 범위 |
| `reference/` | 벤치마크 참고 자료(스마트스토어 캡처 등). 계정·사업자 정보가 있을 수 있어 외부 공개 전에 마스킹한다. |

지표 정의는 `01_contracts/contracts/event_spec.md` 4절을 따른다. 현재 존재하는 폴더는 `reference/`뿐이며 나머지는 해당 단계를 시작할 때 만든다.
