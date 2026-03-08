# 포트폴리오 기능 문서

> 작성일: 2026-03-08
> 앱 내 포트폴리오 기능 전반에 대한 기술 문서입니다.

---

## 목차

1. [개요](#1-개요)
2. [데이터베이스 스키마](#2-데이터베이스-스키마)
3. [데이터베이스 함수](#3-데이터베이스-함수)
4. [REST API 엔드포인트](#4-rest-api-엔드포인트)
5. [AI 포트폴리오 분석](#5-ai-포트폴리오-분석)
6. [프론트엔드 UI](#6-프론트엔드-ui)
7. [응답 데이터 구조](#7-응답-데이터-구조)
8. [파일 경로 요약](#8-파일-경로-요약)

---

## 1. 개요

포트폴리오 기능은 사용자가 보유 주식을 관리하고, 수익률을 분석하며, AI 기반 투자 조언을 받을 수 있는 통합 시스템입니다.

### 주요 기능

| 기능 | 설명 |
|------|------|
| 보유 종목 관리 | 종목 추가/수정/삭제, 매입가·수량 관리 |
| 예수금 관리 | 현금 잔고 추적 및 매매 시 자동 반영 |
| 거래 실행 | BUY/SELL 주문 실행 및 자동 거래 기록 |
| 수익률 추이 | 기간별 포트폴리오 수익률 차트 (KOSPI 벤치마크 비교) |
| 건강 상태 | 7개 핵심 지표 기반 다차원 포트폴리오 진단 |
| 거래 기록 | 전체 매매 이력 조회 |
| 리밸런싱 | 목표 비중 설정 및 조정 가이드 |
| AI 분석 | Claude API 기반 포트폴리오 종합 분석 보고서 |

---

## 2. 데이터베이스 스키마

> 파일: `db.py`

### 2.1 `portfolio` — 보유 종목

```sql
CREATE TABLE portfolio (
    종목코드    TEXT PRIMARY KEY,
    종목명      TEXT,
    수량        INTEGER,
    평균매입가  DOUBLE,
    매입일      TEXT,
    메모        TEXT,
    created_at  TEXT,
    updated_at  TEXT
);
```

### 2.2 `portfolio_cash` — 예수금

```sql
CREATE TABLE portfolio_cash (
    id          INTEGER PRIMARY KEY DEFAULT 1,  -- 항상 1 (단일 레코드)
    amount      DOUBLE,
    updated_at  TEXT
);
```

### 2.3 `portfolio_transactions` — 거래 기록

```sql
CREATE TABLE portfolio_transactions (
    id          INTEGER PRIMARY KEY,
    종목코드    TEXT,
    종목명      TEXT,
    거래유형    TEXT,      -- BUY / SELL / ADJUST
    수량        INTEGER,
    단가        DOUBLE,
    거래일      TEXT,
    메모        TEXT,
    before_qty  INTEGER,  -- 거래 전 수량
    before_avg  DOUBLE,   -- 거래 전 평균매입가
    after_qty   INTEGER,  -- 거래 후 수량
    after_avg   DOUBLE,   -- 거래 후 평균매입가
    created_at  TEXT
);

CREATE INDEX idx_pftx_code ON portfolio_transactions (종목코드, created_at DESC);
```

### 2.4 `portfolio_targets` — 리밸런싱 목표 비중

```sql
CREATE TABLE portfolio_targets (
    종목코드    TEXT PRIMARY KEY,
    목표비중    DOUBLE,  -- 목표 비중 (%)
    updated_at  TEXT
);
```

### 2.5 `portfolio_analysis` — AI 분석 보고서 캐시

```sql
CREATE SEQUENCE seq_portfolio_analysis;
CREATE TABLE portfolio_analysis (
    id              INTEGER PRIMARY KEY DEFAULT nextval('seq_portfolio_analysis'),
    report_html     TEXT,    -- 렌더링된 HTML 보고서
    scores_json     TEXT,    -- JSON 분석 결과
    portfolio_hash  TEXT,    -- 포트폴리오 상태 MD5 해시 (캐시 무효화용)
    model_used      TEXT,    -- 사용된 AI 모델명
    generated_date  TEXT,
    saved_at        TEXT
);
-- 최대 5개 보고서 보관
```

### 2.6 `price_supplement` — 보조 시세 (ETF·우선주·리츠)

```sql
CREATE TABLE price_supplement (
    종목코드   TEXT PRIMARY KEY,
    종목명     TEXT,
    현재가     DOUBLE,
    전일비     DOUBLE,
    등락률     DOUBLE,
    updated_at TEXT
);
```

---

## 3. 데이터베이스 함수

> 파일: `db.py`

### 3.1 보유 종목 관리

| 함수 | 설명 |
|------|------|
| `load_portfolio()` | 전체 보유 종목 조회 |
| `upsert_portfolio_item(code, qty, price, buy_date, memo, name, adjust_cash)` | 종목 추가/수정 (옵션: 예수금 자동 차감) |
| `delete_portfolio_item(code, name, adjust_cash)` | 종목 삭제 (옵션: 예수금 자동 반환) |

#### `upsert_portfolio_item` 동작

- **신규 추가 시**: 평균매입가 = 입력값, 거래 유형 `ADJUST` 로그 기록
- **수량 변경 시**: 가중평균으로 평균매입가 재계산
- **`adjust_cash=True`**: `평균매입가 × 수량` 만큼 예수금 자동 차감

### 3.2 예수금 관리

| 함수 | 설명 |
|------|------|
| `load_cash()` | 현재 예수금 조회 (미설정 시 0 반환) |
| `save_cash(amount)` | 예수금 저장/갱신 |

### 3.3 거래 실행

| 함수 | 설명 |
|------|------|
| `execute_trade(code, name, tx_type, trade_qty, trade_price, tx_date, memo)` | BUY/SELL 주문 실행 |
| `log_transaction(...)` | 단일 거래 로그 기록 |
| `load_transactions(code=None, limit=100)` | 거래 이력 조회 (종목 필터 지원) |

#### `execute_trade` 동작

- **BUY**: 포트폴리오 업데이트 → 예수금 차감 → 거래 로그 기록
- **SELL**: 수량 감소 (0이면 포트폴리오 삭제) → 예수금 증가 → 거래 로그 기록
- 거래 전후 수량·평균가 모두 `portfolio_transactions` 에 저장

### 3.4 리밸런싱

| 함수 | 설명 |
|------|------|
| `save_targets(targets: list[dict])` | 목표 비중 저장 |
| `load_targets()` | 목표 비중 조회 |

### 3.5 AI 분석 보고서

| 함수 | 설명 |
|------|------|
| `save_portfolio_analysis(html, scores_json, portfolio_hash, model, date)` | 분석 보고서 저장 (최대 5개 유지) |
| `load_portfolio_analysis()` | 최신 분석 보고서 조회 |
| `load_portfolio_analysis_history()` | 전체 보고서 목록 조회 |
| `load_portfolio_analysis_by_id(report_id)` | 특정 보고서 조회 |

---

## 4. REST API 엔드포인트

> 파일: `webapp/app.py`

### 4.1 보유 종목 CRUD

| 메서드 | 경로 | 설명 |
|--------|------|------|
| `GET` | `/api/portfolio` | 보유 종목 목록 (현재가·수익률·비중 포함) |
| `POST` | `/api/portfolio` | 종목 추가 |
| `PUT` | `/api/portfolio/<code>` | 종목 수정 |
| `DELETE` | `/api/portfolio/<code>` | 종목 삭제 |

**GET `/api/portfolio` 응답 항목**:
- 현재가 (dashboard_result → price_supplement 순 조회)
- 평가금액, 수익금액, 수익률
- 포트폴리오 내 비중 (%)
- 섹터, PER, 종합점수 (퀀트 지표)

### 4.2 예수금 관리

| 메서드 | 경로 | 설명 |
|--------|------|------|
| `GET` | `/api/portfolio/cash` | 현재 예수금 조회 |
| `POST` | `/api/portfolio/cash` | 예수금 수동 설정 |

### 4.3 거래 실행

| 메서드 | 경로 | 설명 |
|--------|------|------|
| `POST` | `/api/portfolio/trade` | 매수/매도 주문 실행 |

**요청 바디**:
```json
{
  "종목코드": "005930",
  "종목명": "삼성전자",
  "거래유형": "BUY",      // BUY | SELL
  "수량": 10,
  "단가": 60000,
  "거래일": "2026-03-08",
  "메모": "분할매수"
}
```

### 4.4 수익률 추이

| 메서드 | 경로 | 설명 |
|--------|------|------|
| `GET` | `/api/portfolio/performance` | 기간별 수익률 시계열 |

**쿼리 파라미터**: `range=1M|3M|6M|1Y`

**응답**: 날짜별 포트폴리오 수익률 + KOSPI 벤치마크 + 개별 종목 수익률

### 4.5 건강 상태

| 메서드 | 경로 | 설명 |
|--------|------|------|
| `GET` | `/api/portfolio/health` | 포트폴리오 건강 진단 |

**7개 진단 지표**:

| 지표 | Green 기준 | Yellow 기준 |
|------|-----------|------------|
| 수익률 | ≥ 0% | -10% ~ 0% |
| RS등급 | ≥ 70 | 50 ~ 70 |
| 종합점수 | ≥ 60 | 40 ~ 60 |
| 52주고점비율 | ≥ 70% | 50% ~ 70% |
| F-Score | ≥ 6 | 4 ~ 6 |
| AI거장평균 | ≥ 7.0 | 5.0 ~ 7.0 |
| 해자등급 | Wide | Narrow |

### 4.6 거래 기록

| 메서드 | 경로 | 설명 |
|--------|------|------|
| `GET` | `/api/portfolio/transactions` | 거래 이력 조회 |

**쿼리 파라미터**: `code=종목코드` (선택), `limit=200` (기본값)

### 4.7 리밸런싱

| 메서드 | 경로 | 설명 |
|--------|------|------|
| `GET` | `/api/portfolio/rebalance` | 리밸런싱 가이드 (조정 수량/금액) |
| `POST` | `/api/portfolio/rebalance/targets` | 목표 비중 저장 |

**GET 응답 항목**:
- 현재 비중 vs 목표 비중 차이
- 종목별 매수/매도 조정 수량 및 금액

### 4.8 AI 분석 보고서

| 메서드 | 경로 | 설명 |
|--------|------|------|
| `GET` | `/api/portfolio/analysis` | 최신 분석 보고서 캐시 조회 |
| `POST` | `/api/portfolio/analysis` | 새 분석 보고서 생성 |
| `GET` | `/api/portfolio/analysis/history` | 보고서 목록 조회 |
| `GET` | `/api/portfolio/analysis/<report_id>` | 특정 보고서 조회 |

**캐시 무효화**: `portfolio_hash` (MD5) — 포트폴리오 구성·예수금 변경 시 `stale: true` 반환

---

## 5. AI 포트폴리오 분석

> 파일: `analysis/claude_analyzer.py`

### 5.1 분석 프레임워크 (7단계)

```
Step 1. 포트폴리오 건강 점수 (0~100)
        - 분산도, 밸류에이션, 성장성, 안정성 종합

Step 2. 종목별 액션 권고
        - BUY_MORE / HOLD / TRIM / SELL
        - 목표 주수, 목표 주가 (하단/상단), 예상 금액

Step 3. 섹터 집중도 분석
        - 과대/과소 비중 섹터 식별

Step 4. 포트폴리오 위험 & 모멘텀
        - 상관관계 기반 리스크 요인
        - 긍정적 촉매 요인

Step 5. 누락 테마 발굴
        - 포트폴리오 공백 영역 식별

Step 6. 관심 종목 추천 (ADD / WATCH / SKIP)
        - 기존 포트폴리오와의 시너지 분석

Step 7. 리밸런싱 실행 계획
        - 우선순위 및 긴급도 제시
```

### 5.2 주요 함수

| 함수 | 설명 |
|------|------|
| `generate_portfolio_report(...)` | Claude API 호출, 종합 보고서 생성 |
| `render_portfolio_html(...)` | 분석 결과 → HTML 보고서 렌더링 |
| `format_portfolio_stock(...)` | 개별 종목 퀀트 지표 + AI 보고서 요약 포맷 |
| `compute_correlation_matrix(codes, names)` | 250거래일 주가 상관계수 행렬 계산 |

### 5.3 입력 데이터

- 포트폴리오 보유 현황 (수량·평균매입가·현재가·수익률)
- 개별 종목 퀀트 지표 (종합점수, PER, ROE, F-Score, RS등급 등)
- 개별 종목 AI 분석 요약 (6인 거장 프레임워크)
- 관심 종목 목록
- 종목 간 주가 상관계수 행렬
- 예수금 잔고

### 5.4 캐시 정책

- `portfolio_hash` = MD5(종목코드 목록 + 관심 종목 목록 + 예수금)
- 포트폴리오 변경 시 기존 보고서 `stale: true` 표시
- 새 보고서 생성 시 이전 보고서는 유지 (최대 5개)

---

## 6. 프론트엔드 UI

> 파일: `webapp/templates/dashboard.html`, `webapp/static/js/dashboard.js`

### 6.1 탭 구성

```
포트폴리오 탭
├── 보유종목      — 종목별 현황 테이블
├── 수익률 추이   — 기간 선택 가능한 수익률 차트
├── 건강 상태     — 지표별 색상 코드 진단 카드
├── 거래 기록     — 전체 매매 이력 목록
└── 리밸런싱      — 목표 비중 설정 및 조정 가이드
```

### 6.2 상단 요약 바

- 총 평가금액 / 총 수익금액 / 총 수익률
- 예수금 입력 필드 (직접 입력 후 저장)
- 종목 추가 버튼
- AI 분석 실행 버튼
- 섹터별 비중 표시

### 6.3 종목 추가/수정 모달

| 필드 | 설명 |
|------|------|
| 종목코드 | 6자리 (자동 zero-padding) |
| 수량 | 보유 수량 |
| 평균매입가 | 매입 단가 |
| 매입일 | 날짜 선택 |
| 메모 | 자유 메모 |

### 6.4 AI 분석 보고서 모달

- 로딩 스피너 (생성 중 프로그레스)
- 오래된 보고서 경고 배너 (`stale: true` 시)
- HTML 보고서 인라인 렌더링
- 보고서 이력 조회 (과거 보고서 열람)

### 6.5 주요 JavaScript 함수

| 함수 | 설명 |
|------|------|
| `loadPortfolioData()` | 포트폴리오 데이터 로드 |
| `loadCashBalance()` | 예수금 조회 |
| `saveCashBalance()` | 예수금 저장 |
| `savePortfolioItem()` | 종목 추가/수정 저장 |
| `deletePortfolioItem()` | 종목 삭제 |
| `executeTrade()` | 매수/매도 실행 |
| `loadPerformanceChart()` | 수익률 차트 로드 |
| `loadHealthStatus()` | 건강 상태 로드 |
| `loadTransactions()` | 거래 기록 로드 |
| `loadRebalanceGuide()` | 리밸런싱 가이드 로드 |
| `generatePortfolioAnalysis()` | AI 분석 보고서 생성 |

---

## 7. 응답 데이터 구조

### 7.1 `GET /api/portfolio` — 보유 종목 목록

```json
{
  "items": [
    {
      "종목코드": "005930",
      "종목명": "삼성전자",
      "종목구분": "보통주",
      "수량": 100,
      "평균매입가": 50000,
      "현재가": 60000,
      "매입금액": 5000000,
      "평가금액": 6000000,
      "수익금액": 1000000,
      "수익률": 20.0,
      "비중": 45.5,
      "섹터": "전자",
      "PER": 15.5,
      "종합점수": 75.0,
      "매입일": "2024-01-15",
      "메모": "장기보유"
    }
  ],
  "summary": {
    "총매입금액": 11000000,
    "총평가금액": 13200000,
    "총수익금액": 2200000,
    "총수익률": 20.0,
    "종목수": 2,
    "예수금": 500000,
    "총자산": 13700000
  },
  "섹터별": [
    {
      "섹터": "전자",
      "평가금액": 6000000,
      "비중": 45.5,
      "종목": ["삼성전자"]
    }
  ]
}
```

### 7.2 `GET /api/portfolio/health` — 건강 상태

```json
{
  "stocks": [
    {
      "종목코드": "005930",
      "종목명": "삼성전자",
      "수익률": 20.0,
      "indicators": {
        "수익률":       {"value": 20.0, "status": "green"},
        "RS등급":       {"value": 85,   "status": "green"},
        "종합점수":     {"value": 75,   "status": "green"},
        "52주고점비율": {"value": 82,   "status": "green"},
        "F-Score":      {"value": 7,    "status": "green"},
        "AI거장평균":   {"value": 7.5,  "status": "green"},
        "해자등급":     {"value": "Wide","status": "green"}
      },
      "alert_count": 0
    }
  ],
  "alerts": [],
  "ai_summary": {
    "generated_date": "2026-03-08 10:30:00",
    "model": "claude-sonnet-4-5-20250929",
    "stale": false,
    "hash_mismatch": false
  }
}
```

### 7.3 AI 분석 보고서 구조 (`scores_json`)

```json
{
  "portfolio_health": {
    "score": 78,
    "grade": "B+",
    "diversification": "섹터 분산 양호",
    "valuation": "적정 밸류에이션",
    "growth_quality": "성장성 우수",
    "overall_assessment": "전반적으로 균형 잡힌 포트폴리오"
  },
  "stock_actions": [
    {
      "code": "005930",
      "name": "삼성전자",
      "action": "HOLD",
      "current_weight": 45.5,
      "recommended_weight": 40.0,
      "target_shares": 0,
      "target_price_low": 55000,
      "target_price_high": 65000,
      "estimated_amount": 0,
      "rationale": "현 수준 유지 권고"
    }
  ],
  "sector_analysis": {
    "overweight": ["전자"],
    "underweight": ["바이오", "소비재"],
    "summary": "전자 섹터 과다 집중"
  },
  "portfolio_risks": ["반도체 사이클 리스크", "환율 변동"],
  "portfolio_catalysts": ["AI 수요 증가", "금리 인하 기대"],
  "missing_themes": ["헬스케어", "친환경 에너지"],
  "watchlist_recommendations": [
    {
      "code": "068270",
      "name": "셀트리온",
      "action": "ADD",
      "rationale": "헬스케어 테마 보완"
    }
  ],
  "rebalancing_plan": {
    "urgency": "보통",
    "priority_actions": ["전자 비중 5%p 축소", "바이오 신규 편입"]
  },
  "summary": "포트폴리오 전반 안정적, 섹터 분산 개선 권고"
}
```

---

## 8. 파일 경로 요약

| 컴포넌트 | 파일 경로 |
|----------|----------|
| DB 스키마 | `db.py:117-184` |
| DB 함수 (보유 종목) | `db.py:585-668` |
| DB 함수 (예수금) | `db.py:671-688` |
| DB 함수 (거래) | `db.py:691-867` |
| DB 함수 (리밸런싱) | `db.py:872-893` |
| DB 함수 (AI 분석) | `db.py:752-823` |
| API 엔드포인트 | `webapp/app.py:532-1286` |
| API 헬퍼 함수 | `webapp/app.py:532-649` |
| AI 분석 생성 | `analysis/claude_analyzer.py:1886-2627` |
| HTML 템플릿 | `webapp/templates/dashboard.html` |
| 프론트엔드 JS | `webapp/static/js/dashboard.js:2719-3991` |
