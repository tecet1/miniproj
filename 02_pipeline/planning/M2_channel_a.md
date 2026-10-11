# M2: 채널 A 수집·Bronze·Silver·Gold

- ~~다음 작업은 M1의 events.jsonl을 Event Hubs에 한 줄씩 송신하는 것이다.~~ **변경: v1 6개 원문의 Bronze 적재를 확인했다. 다음은 Silver 검증/실제 적재다.**
- 현재 목표는 Bronze 원문 저장과 공통 Silver 변환까지다. Gold는 그 뒤 진행한다.

대응 이슈: TEC-16 수집·Bronze, TEC-17 Silver·검증·멱등 저장, TEC-18 Gold.

~~src/에는 송신 코드, notebooks/에는 Databricks 노트북~~ **변경: ingestion/에 송신 코드, bronze/·silver/에 단계별 노트북, 해당 단계의 evidence/에 실행 근거를 둔다.** Functions를 경유하지 않은 직접 송신은 Functions 검증 완료로 표시하지 않는다.
