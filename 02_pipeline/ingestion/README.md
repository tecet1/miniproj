# 수집·송신

- A/B/C의 원본 계약은 01_contracts/contracts/channels.md에서 관리한다.
- 다음 구현은 A JSONL의 한 줄씩 Event Hubs에 송신하는 코드다.

직접 송신과 Functions 경유 테스트를 구분한다. 현재 송신 코드와 인증 설정은 아직 추가하지 않았다.
