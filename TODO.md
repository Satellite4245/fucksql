# FUCKsqlite 기능 구현 로드맵 & 작업 현황

## ✅ 현재 구현 완료된 기능

- [x] **연결 및 리소스 관리**
  - `connect()`: WAL 모드, `PRAGMA busy_timeout = 5000`, 외래키 활성화 PRAGMA 설정
  - `close()`: 미커밋 변경사항 자동 커밋 및 리소스 안전 해제
  - `__aenter__` / `__aexit__`: 비동기 컨텍스트 매니저(`async with`) 지원
  - `commit()`, `rollback()`: 수동 트랜잭션 제어 지원

- [x] **트랜잭션 관리**
  - `transaction()` 비동기 컨텍스트 매니저 (에러 시 자동 ROLLBACK, 정상 완료 시 COMMIT)

- [x] **테이블 스키마 및 메타데이터 관리 (DDL / Utility)**
  - `Column`, `ForeignKey` 데이터 클래스 및 제약조건 (PK, AI, NOT NULL, UNIQUE, DEFAULT, CHECK, FK)
  - `create_table()`: 안전한 테이블 생성
  - `drop_table()`: 테이블 삭제
  - `create_index()`: 단일/복합/유니크 인덱스 생성
  - `drop_index()`: 인덱스 삭제
  - `table_exists()`: `sqlite_master` 기반 특정 테이블 존재 여부 확인
  - `list_tables()`: DB 내 모든 사용자 테이블 목록 정렬 반환 (`sqlite_master` 기반, 내부 테이블 제외)

- [x] **데이터 생성 (Create)**
  - `insert()`: 단일 행 삽입 (lastrowid 반환)
  - `inserts()`: 다중 행 대량 삽입 (`executemany` 기반, 키 순서/누락 방어, rowcount 반환)

- [x] **데이터 조회 (Read)**
  - `select()`: Raw `where` 문자열, 단일/다중 `params` 정규화, 컬럼 선택, 정렬(ORDER BY), 페이징(LIMIT/OFFSET) 지원
  - `select_one()`: 단일 행 조회
  - `count()`: 전체/컬럼별/DISTINCT 행 개수 조회 및 WHERE 조건 연동
  - `exists()`: 데이터 존재 여부(`bool`) 반환 (`SELECT 1 ... LIMIT 1`)

- [x] **데이터 수정 (Update)**
  - `update()`: Raw `where` 조건별 수정, `params` 바인딩, `allow_all` 전체 수정 방어 옵션, rowcount 반환

- [x] **데이터 삭제 (Delete)**
  - `delete()`: Raw `where` 조건별 삭제, `params` 바인딩, `allow_all` 전체 삭제 방어 옵션, rowcount 반환

- [x] **범용 Raw SQL 실행 지원**
  - `execute()`: 임의의 SQL 1회 실행 및 Cursor 반환 (자동 커밋 지원)
  - `fetch()`: 임의의 SELECT SQL 실행 및 `list[dict[str, Any]]` 반환
  - `fetch_one()`: 임의의 SELECT SQL 실행 및 `Optional[dict[str, Any]]` 단건 반환

---

## 🚀 앞으로 구현할 남은 작업 목록

*(현재 계획된 작업이 모두 완료되었습니다)*
