# Silver 변환 로직

- channel_a.py는 A v1/v2를 같은 공통 v1 이벤트로 변환하는 순수 함수다.
- 현재 결과는 로컬 JSON 형태다. Spark 타입 지정·Bronze 연결·Quarantine·멱등 Delta 저장은 다음 작업이다.

## 사용과 코드 선택

- normalize_event(event, sku_mapping)는 입력을 변경하지 않고 공통 행 하나를 반환한다.
- 실패하면 ValueError가 발생한다. 호출부가 실패 입력을 기록해야 하며 예외를 삼키면 안 된다.

ORDER_TOTAL_FIELDS는 선언된 버전마다 읽을 필드를 고르는 작은 사전이다. 필드 존재만 보고 추측하면 잘못된 버전의 누락을 놓칠 수 있으므로 지원 버전을 명시한다. 원본 버전만 바뀌고 업무 필드가 같으면 공통 결과는 동일하다.

text·integer는 필수 문자열과 정수 타입을 확인한다. type(value) is int로 bool을 금액·수량으로 허용하지 않는다. utc_time은 시간대를 요구하고 UTC로 바꾼다. normalize_items는 원본 SKU를 매핑하고 각 이벤트의 수량·금액 필드로 변환한다.

이벤트 종류가 다섯 개뿐이라 일반 if 분기로 payload를 작성한다. 클래스 계층이나 규칙 설정 엔진을 추가하지 않는다. 입력 JSON의 구조와 출력 필드가 눈에 보이는 것이 현재 유지보수에 유리하다.

조회·파일·네트워크 접근이 없는 함수이므로 고정 입력으로 테스트할 수 있다. 추후 Spark 연동 시 대량 데이터를 driver로 collect해서 이 함수를 돌리는 방식은 사용하지 않는다. 물리 타입과 분산 실행 방법은 Bronze→Silver 저장 단계에서 결정한다.
