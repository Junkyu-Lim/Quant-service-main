# Quant Service

KOSPI/KOSDAQ 전종목을 대상으로 데이터를 수집하고, 6가지 투자 전략으로 스크리닝하며, Claude AI 정성 분석 보고서를 생성하는 한국 주식 퀀트 서비스입니다.

## 목차

- [아키텍처 개요](#아키텍처-개요)
- [디렉터리 구조](#디렉터리-구조)
- [설치](#설치)
- [환경 변수](#환경-변수)
- [실행](#실행)
- [데이터 흐름](#데이터-흐름)
- [모듈 상세](#모듈-상세)
  - [데이터 수집 (quant_collector_enhanced.py)](#데이터-수집-quant_collector_enhancedpy)
  - [스크리닝 엔진 (quant_screener.py)](#스크리닝-엔진-quant_screenerpy)
  - [데이터베이스 (db.py)](#데이터베이스-dbpy)
  - [파이프라인 (pipeline.py)](#파이프라인-pipelinepy)
  - [웹 서버 (webapp/app.py)](#웹-서버-webappapppy)
  - [AI 분석 (analysis/claude_analyzer.py)](#ai-분석-analysisclaude_analyzerpy)
  - [배치 스케줄러 (batch/scheduler.py)](#배치-스케줄러-batchschedulerpy)
- [스크리닝 전략](#스크리닝-전략)
- [스코어링 시스템](#스코어링-시스템)
- [REST API](#rest-api)
- [데이터베이스 스키마](#데이터베이스-스키마)
- [주요 구현 패턴](#주요-구현-패턴)
- [개발 가이드](#개발-가이드)

---

## 아키텍처 개요

```
[데이터 소스]                    [파이프라인]                [출력]
  FnGuide (크롤링) ──┐
  FinanceDataReader ──┼──► quant_collector ──► DuckDB ──► quant_screener ──► Excel (6종)
  KRX (pykrx)    ──┘          │                              │
                              └──────────────────────────────┤
                                                             ▼
                                                      dashboard_result
                                                             │
                                                      webapp/app.py (Flask)
                                                             │
                                                      Claude / Gemini API (AI 분석)
```

- **데이터 수집**: FnGuide HTML 크롤링 + FinanceDataReader API
- **저장소**: DuckDB 단일 파일(`data/quant.duckdb`), `collected_date`로 버전 관리
- **스크리닝**: 퍼센타일 기반 스코어링 + 전략별 필터
- **웹 인터페이스**: Flask REST API + 대시보드 UI
- **AI 보고서**: Claude / Gemini API (6대 투자 대가 프레임워크)

---

## 디렉터리 구조

```
Quant-service/
├── run.py                        # 메인 진입점 (CLI)
├── pipeline.py                   # 파이프라인 오케스트레이터
├── quant_collector_enhanced.py   # 데이터 수집기
├── quant_screener.py             # 스크리닝 엔진 (v14)
├── db.py                         # DuckDB 헬퍼
├── config.py                     # 설정 (환경변수 로드)
├── requirements.txt
├── .env.example
├── batch/
│   ├── __init__.py
│   └── scheduler.py              # APScheduler 배치 스케줄러
├── analysis/
│   ├── __init__.py
│   └── claude_analyzer.py        # Claude AI 정성 분석
├── webapp/
│   ├── __init__.py
│   ├── app.py                    # Flask 앱 + REST API
│   ├── templates/
│   │   └── dashboard.html        # 대시보드 UI
│   └── static/                   # CSS, JS
└── data/                         # 런타임 생성 (gitignore)
    ├── quant.duckdb              # 메인 DB
    ├── reports/                  # AI 분석 보고서 캐시
    └── *.xlsx                    # 스크리닝 결과 엑셀
```

---

## 설치

```bash
# Python 3.11+ 권장
pip install -r requirements.txt
```

**주요 의존성**

| 패키지 | 용도 |
|--------|------|
| `flask`, `flask-cors` | 웹 서버 |
| `duckdb` | 데이터베이스 |
| `pandas`, `numpy` | 데이터 처리 |
| `FinanceDataReader` | 주가/상장정보 수집 |
| `pykrx` | KRX 데이터 |
| `anthropic` | Claude AI API |
| `google-genai` | Gemini AI API |
| `apscheduler` | 배치 스케줄러 |
| `gunicorn` | 프로덕션 서버 |
| `openpyxl`, `lxml` | 엑셀/HTML 파싱 |

---

## 환경 변수

`.env.example`을 참고하여 환경변수를 설정하세요.

| 변수 | 기본값 | 설명 |
|------|--------|------|
| `BATCH_HOUR` | `18` | 일일 배치 실행 시간 (KST, 0-23) |
| `BATCH_MINUTE` | `0` | 배치 실행 분 (0-59) |
| `HOST` | `0.0.0.0` | 웹 서버 바인드 호스트 |
| `PORT` | `5000` | 웹 서버 포트 |
| `DEBUG` | `false` | Flask 디버그 모드 |
| `ANTHROPIC_API_KEY` | *(없음)* | Claude AI 분석용 (선택) |
| `ANALYSIS_MODEL` | `claude-sonnet-4-5-20250929` | 분석에 사용할 Claude 모델 |
| `GEMINI_API_KEY` | *(없음)* | Gemini AI 분석용 (선택, Claude 대체) |
| `GEMINI_RESEARCH_MODEL` | `gemini-2.0-flash` | 분석에 사용할 Gemini 모델 |
| `RISK_FREE_RATE` | `3.5` | S-RIM 무위험수익률 (%, 국고채 3년물 기준) |
| `EQUITY_RISK_PREMIUM` | `5.5` | S-RIM 시장위험프리미엄 (%) |

---

## 실행

### CLI 명령어

```bash
# 웹 서버 시작 (배치 스케줄러 포함)
python run.py server

# 전체 파이프라인 실행 (수집 + 스크리닝)
python run.py pipeline

# 테스트 모드 (삼성전자·카카오·SK하이닉스 3종목만)
python run.py pipeline --test

# 수집 건너뛰고 스크리닝만 재실행
python run.py pipeline --skip-collect

# 데이터 수집만 실행
python run.py collect

# 스크리닝만 실행 (DB에 데이터 있어야 함)
python run.py screen
```

### 프로덕션 서버

```bash
gunicorn -w 4 -b 0.0.0.0:5000 webapp.app:app
```

---

## 데이터 흐름

```
[수집 단계 — quant_collector_enhanced]
1. collect_master()          → KRX 전종목 마스터 (FinanceDataReader)
2. collect_daily()           → 일별 시세 + 시가총액 (FinanceDataReader)
3. fetch_fs()                → 재무제표 IS/BS/CF (FnGuide 크롤링, 병렬)
4. fetch_indicators()        → Financial Highlight + 재무비율 + DPS (FnGuide)
5. fetch_shares()            → 발행주식수, 자사주, 유통주식수 (FnGuide)
6. collect_price_history()   → 52주 OHLCV + 거래대금 히스토리 (FinanceDataReader)
7. collect_investor_trading()→ 투자자별 매매동향 (KRX)
         │
         ▼ (DB 저장: collected_date 기준 버전 관리)

[스크리닝 단계 — quant_screener]
8.  preprocess_indicators()      → 중복 제거
9.  detect_unit_multiplier()     → 삼성전자 매출로 단위 추론 (억/백만 등)
10. analyze_all()                → 종목별 펀더멘털 분석 (TTM, CAGR, F-Score, 배당 등)
11. calc_valuation()             → PER/PBR/ROE/S-RIM/PSR/PEG/FCF수익률 등 계산
12. calc_technical_indicators()  → 52주 고저, MA이격도, RSI, 거래대금, 변동성
13. calc_investor_strength()     → 수급강도 (외인+기관 순매수 기반)
14. calc_strategy_scores()       → 전략별 점수 계산
15. save_dashboard()             → dashboard_result 테이블 저장
16. save_to_excel()              → 전체종목 1개 + 전략별 5개 = 총 6개 엑셀 파일 출력
```

---

## 모듈 상세

### 데이터 수집 (`quant_collector_enhanced.py`)

FnGuide와 FinanceDataReader를 통해 KOSPI/KOSDAQ 전종목 데이터를 수집합니다.

**수집 항목**

| 단계 | 함수 | 소스 | 수집 내용 |
|------|------|------|-----------|
| 1 | `collect_master()` | FinanceDataReader | 종목코드, 종목명, 시장구분, 종목구분(보통주/우선주/스팩/리츠) |
| 2 | `collect_daily()` | FinanceDataReader | 종가, 시가총액, 상장주식수 |
| 3 | `fetch_fs()` | FnGuide | IS/BS/CF 재무제표 (연간·분기, 실적+추정치) |
| 4 | `fetch_indicators()` | FnGuide | Financial Highlight, 재무비율, 주당배당금(DPS) |
| 5 | `fetch_shares()` | FnGuide | 발행주식수, 자사주, 유통주식수 |
| 6 | `collect_price_history()` | FinanceDataReader | 52주 OHLCV + 거래대금 |

**병렬 처리**

- `ThreadPoolExecutor(max_workers=15)` 로 FnGuide 크롤링을 병렬 수행
- 이미 DB에 해당 `collected_date`의 데이터가 있으면 수집을 건너뜀 (이어하기 지원)

**인코딩 처리**

FnGuide HTML 응답을 `cp949 → euc-kr → utf-8` 순으로 시도하여 한글 깨짐을 방지합니다.

---

### 스크리닝 엔진 (`quant_screener.py`)

v14 버전. 펀더멘털 분석, 밸류에이션 계산, 기술적 지표, 5가지 스크리닝 전략을 담당합니다.

#### 핵심 함수

| 함수 | 설명 |
|------|------|
| `detect_unit_multiplier(ind_df)` | 삼성전자(005930) 매출 규모로 재무 데이터 단위 추론 |
| `analyze_one_stock()` | 종목 1개 펀더멘털 분석 (TTM, CAGR, F-Score, 배당 등) |
| `analyze_all()` | 전체 종목 루프 처리 |
| `calc_valuation()` | PER/PBR/ROE/PSR/PEG/FCF수익률/S-RIM 등 계산 |
| `calc_technical_indicators()` | 52주 고저·이격도·RSI·거래대금·변동성 계산 |
| `calc_investor_strength()` | 수급강도 계산 (외인+기관 순매수 기반) |
| `calc_strategy_scores()` | 전략별 종합 점수 계산 |
| `apply_leaders_screen()` | 주도주 (수급·거래대금 기반) |
| `apply_quality_value_screen()` | 우량가치 (ROE·PEG·F-Score) |
| `apply_growth_mom_screen()` | 고성장 모멘텀 (CAGR·분기YoY) |
| `apply_cash_div_screen()` | 현금배당 (FCF수익률·배당수익률) |
| `apply_turnaround_screen()` | 턴어라운드 (흑자전환·이익률 급개선) |

#### TTM (Trailing 12 Months) 계산 방식

분기 데이터(RATIO_Q)에서 최근 4분기 합계를 우선 사용하고, 없으면 연간 데이터(RATIO_Y)의 최신값으로 대체합니다.

#### 계절성 통제 지표 (v8 추가)

분기별 전년동기비(YoY) 성장률을 계산하여 계절성 영향을 제거합니다.

- `Q_매출_YoY(%)` / `Q_영업이익_YoY(%)` / `Q_순이익_YoY(%)`: 최근 분기 YoY
- `Q_*_연속YoY성장`: 연속으로 YoY 플러스인 분기 수
- `TTM_*_YoY(%)`: 최근 4분기 합 vs 전년 4분기 합 비교

#### Piotroski F-Score (9개 항목)

| 항목 | 조건 |
|------|------|
| F1 수익성 | 순이익(TTM) > 0 |
| F2 영업CF | 영업현금흐름 > 0 |
| F3 ROA 개선 | 순이익/자산총계 전년 대비 증가 |
| F4 이익 품질 | 영업CF > 순이익 (발생주의 회계 왜곡 없음) |
| F5 레버리지 감소 | 부채비율(부채/자본) 전년 대비 하락 |
| F6 유동성 개선 | 유동비율(유동자산/유동부채) 전년 대비 상승 |
| F7 희석 없음 | 발행주식수 미증가 |
| F8 매출총이익률 개선 | 매출총이익/매출액 전년 대비 상승 |
| F9 자산회전율 개선 | 매출/자산총계 전년 대비 상승 |

#### S-RIM 적정주가

```
Ke = RISK_FREE_RATE + EQUITY_RISK_PREMIUM  (기본값: 3.5% + 5.5% = 9.0%)

ROE > Ke인 경우: BPS + BPS × (ROE - Ke) / Ke
ROE ≤ Ke인 경우: BPS × 0.9

괴리율(%) = (적정주가 - 현재가) / 현재가 × 100
```

---

### 데이터베이스 (`db.py`)

DuckDB 단일 파일(`data/quant.duckdb`)로 모든 데이터를 관리합니다.

#### 테이블 목록

| 테이블 | PK | 내용 |
|--------|----|------|
| `master` | 종목코드, collected_date | 종목 마스터 (시장구분, 종목구분) |
| `daily` | 종목코드, collected_date | 일별 시세 (종가, 시가총액, EPS, BPS, DPS) |
| `financial_statements` | - | 재무제표 세로형 (계정, 기준일, 주기, 값) |
| `indicators` | - | 핵심 지표 (지표구분, 계정, 기준일, 값) |
| `shares` | 종목코드, collected_date | 발행주식수, 자사주, 유통주식수 |
| `price_history` | 종목코드, 날짜, collected_date | OHLCV + 거래대금 |
| `investor_trading` | 종목코드, 날짜, collected_date | 투자자별 매매동향 (외인·기관·개인) |
| `index_history` | 날짜, collected_date | 시장 인덱스 (KOSPI/KOSDAQ) 일별 데이터 |
| `dashboard_result` | 종목코드 | 스크리닝 결과 전체 (60+ 컬럼) |
| `dashboard_result_prev` | - | 직전 배치 결과 (변동 비교용) |
| `analysis_reports` | 종목코드 | AI 분석 보고서 (HTML + JSON) |

#### 버전 관리

`collected_date` 컬럼으로 날짜별 데이터를 관리합니다. `load_latest(table)`은 항상 `MAX(collected_date)` 데이터를 반환하며, 같은 날 재실행 시 기존 데이터를 덮어씁니다.

#### 주요 함수

```python
init_db()                          # 스키마 초기화
save_df(df, table, collected_date) # DataFrame → 테이블 저장
load_latest(table)                 # 최신 데이터 로드
save_dashboard(df)                 # dashboard_result 갱신 (prev 자동 백업)
save_report(code, name, html, ...) # AI 보고서 저장
load_report(code)                  # AI 보고서 조회
get_data_status()                  # 테이블별 데이터 현황
```

---

### 파이프라인 (`pipeline.py`)

수집 → 스크리닝 → 저장을 순서대로 실행하는 오케스트레이터입니다.

```python
run_pipeline(skip_collect=False, test_mode=False)
```

실행 순서:
1. `quant_collector_enhanced.run_full()` — 데이터 수집
2. DB에서 8개 테이블 로드 (master, daily, fs, indicators, shares, price_history, investor_trading, index_history)
3. 전처리 및 단위 추론
4. 펀더멘털 분석 (`analyze_all`)
5. 밸류에이션 계산 (`calc_valuation`)
6. 기술적 지표 계산 (`calc_technical_indicators`)
7. 수급강도 계산 (`calc_investor_strength`)
8. master 테이블에서 시장/종목구분 병합
9. 전략 점수 계산 (`calc_strategy_scores`)
10. `dashboard_result` DB 저장
11. 전체종목 + 전략별 5개 = 총 6개 엑셀 파일 저장

---

### 웹 서버 (`webapp/app.py`)

Flask 기반 REST API와 대시보드 UI를 제공합니다.

**인메모리 캐시**: DB 파일의 `mtime`이 바뀔 때만 재로드하여 성능을 최적화합니다.

**스크리닝 일관성 주의**: `_apply_screen_filter()` 함수는 `quant_screener.py`의 스크리닝 로직을 동일하게 구현해야 합니다. **스크리닝 조건 변경 시 두 파일을 반드시 함께 수정하세요.**

---

### AI 분석 (`analysis/claude_analyzer.py`)

Claude 또는 Gemini API를 사용하여 6대 투자 대가 관점의 정성 분석 보고서를 생성합니다.

**6대 투자 대가 프레임워크**

| 대가 | 철학 | 가중치 |
|------|------|--------|
| Warren Buffett | 경제적 해자 & 안전마진 | 25% |
| Aswath Damodaran | 내재가치 & 내러티브 | 20% |
| Philip Fisher | 성장 잠재력 & 경영 품질 | 20% |
| Pat Dorsey | 경제적 해자 심층 분석 | 15% |
| Peter Lynch | 성장주 발굴 & 실생활 투자 | 10% |
| André Kostolany | 시장 심리 & 역발상 | 10% |

**입력 데이터**: 밸류에이션, 수익성, 성장성, 재무건전성, 배당, 기술적 지표, TTM 실적 (8개 섹션)

**출력**: 대가별 점수(1-10), 종합 점수(1-100), 투자 등급(A+~D), 리스크/촉매, HTML 보고서

`ANTHROPIC_API_KEY` 또는 `GEMINI_API_KEY` 환경변수가 없으면 AI 분석 기능이 비활성화됩니다.

---

### 배치 스케줄러 (`batch/scheduler.py`)

APScheduler를 사용하여 매일 장 마감 후 자동으로 파이프라인을 실행합니다.

```python
start_scheduler()   # 백그라운드 스케줄러 시작
stop_scheduler()    # 스케줄러 종료
```

기본값: 매일 18:00 KST (`BATCH_HOUR=18, BATCH_MINUTE=0`)

웹 서버 실행 시 자동으로 시작되며, UI에서 수동 트리거도 가능합니다(`POST /api/batch/trigger`).

---

## 스크리닝 전략

총 5가지 전략으로 종목을 분류합니다. 한 종목이 여러 전략에 동시 포함될 수 있습니다(`multi_strategy` 필터로 조회 가능).

### ① 주도주 (`apply_leaders_screen`)

수급·거래가 집중되고 이익이 확인된 대형 주도 종목.

| 조건 | 기준 | 비고 |
|------|------|------|
| 시가총액 | ≥ 1,000억 | |
| 거래대금 20일평균 | > 1억 | 데이터 없으면 통과 |
| TTM 순이익 | > 0 (흑자) | |
| RS_등급 | ≥ 70 (상위 30%) | 데이터 없으면 통과 |
| 주도주_점수 | > 0 | |

**주도주 점수** (모든 지표 퍼센타일 랭킹 0~100, NaN=0점)

| 지표 | 가중치 | 설명 |
|------|--------|------|
| RS_등급 | 25% | Composite RS 전 종목 백분위 (O'Neil 방식) |
| 수급강도 | 20% | 최근 20일 외인+기관 순매수 / 시가총액 |
| 영업이익_CAGR | 15% | 장기 이익 성장률 |
| Q_영업이익_YoY | 15% | 최근 분기 전년동기비 |
| 실적가속_연속 | 10% | 영업이익 분기 가속도 연속 개선 여부 (0/1) |
| 거래대금_20일평균 | 10% | 시장 관심도 |
| RSI_14 | 5% | 14일 상대강도지수 |

> **RS_등급** = Composite RS의 전 종목 백분위. Composite RS = RS_60d(40%) + RS_120d(30%) + RS_250d(30%) — 각 기간 수익률을 먼저 퍼센타일로 변환 후 가중합산 (O'Neil 크기 편향 제거)

### ② 우량가치 (`apply_quality_value_screen`)

합리적 가격의 고ROE 우량 기업.

| 조건 | 기준 |
|------|------|
| ROE | ≥ 10% |
| PEG | < 1.5 |
| PER | 1 ~ 40 |
| F-Score | ≥ 4 |
| 시가총액 | ≥ 500억 |

**우량가치 점수** = PEG_inv×0.3 + PER_inv×0.1 + ROE×0.3 + F스코어×0.2 + 부채비율_inv×0.1

### ③ 고성장 모멘텀 (`apply_growth_mom_screen`)

빠른 성장세 + 현재 상승 추세.

| 조건 | 기준 |
|------|------|
| 매출 또는 영업이익 CAGR | ≥ 15% (OR 조건) |
| Q 영업이익 YoY | > 0% |
| MA20 이격도 | ≥ -5% |
| 시가총액 | ≥ 500억 |

**고성장 점수** = 매출CAGR×0.2 + 영업이익CAGR×0.3 + Q_영업이익YoY×0.3 + MA20이격도×0.1 + 52주최고대비×0.1

### ④ 현금배당 (`apply_cash_div_screen`)

FCF가 풍부하고 배당이 안정적인 저부채 기업.

| 조건 | 기준 |
|------|------|
| FCF 수익률 | ≥ 3% |
| 배당수익률 | ≥ 1% |
| 부채비율 | < 150% |
| 시가총액 | ≥ 500억 |

**현금배당 점수** = FCF수익률×0.3 + 배당수익률×0.3 + DPS_CAGR×0.2 + F스코어×0.1 + 부채비율_inv×0.1

### ⑤ 턴어라운드 (`apply_turnaround_screen`)

역발상 — 실적 반등 초기 발굴.

| 조건 | 기준 | 비고 |
|------|------|------|
| 흑자전환=1 OR 이익률_급개선=1 | 적자→흑자 전환 또는 영업이익률 +5%p 이상 | OR 조건 |
| TTM 순이익 | > 0 (현재 흑자) | |
| 시가총액 | ≥ 300억 | 소형주 포함 |
| 스마트머니_승률 | ≥ 0.5 OR VCP_신호=1 | 수급 데이터 있을 때만 적용, 없으면 통과 |

> **스마트머니_승률**: 최근 20일 중 외인 또는 기관이 순매수한 날의 비율
> **VCP_신호**: 스마트머니_승률≥60% + 가격 축소(변동성 감소) + 거래량 축소 동시 충족 시 1

**턴어라운드 점수** (모든 지표 퍼센타일 랭킹 0~100)

| 지표 | 가중치 | 설명 |
|------|--------|------|
| 이익률_변동폭 | 20% | 영업이익률 개선 폭 |
| 흑자전환 | 20% | 적자→흑자 전환 여부 (0/1) |
| 스마트머니_승률 | 20% | 외인+기관 순매수 비율 |
| GPM_변화(pp) | 15% | 매출총이익률 변화 |
| F스코어 | 15% | Piotroski 재무건전성 |
| 괴리율(%) | 10% | S-RIM 대비 저평가 정도 |

---

## 스코어링 시스템

모든 지표는 **전 종목 퍼센타일 랭킹(0~100)** 으로 정규화합니다.
NaN 처리: PER·PBR·PEG·ROE·FCF수익률·RS_등급·실적가속_연속은 NaN=0점, 그 외는 NaN=중앙값(50점) 처리.

### 종합점수 (`calc_strategy_scores`)

세 축을 균등 1/3씩 합산합니다.

```
종합점수 = (성장성_점수 + 안정성_점수 + 가격_점수) / 3
```

| 축 | 구성 (가중치) |
|----|--------------|
| **성장성_점수** (1/3) | 영업이익_CAGR × 35% + 매출_CAGR × 30% + Q_영업이익_YoY × 25% + 실적가속_연속 × 10% |
| **안정성_점수** (1/3) | ROE × 40% + F스코어 × 35% + FCF수익률 × 25% |
| **가격_점수** (1/3) | PER역순 × 40% + 괴리율(S-RIM) × 35% + PBR역순 × 25% |

### 전략별 점수

| 점수 컬럼 | 공식 (퍼센타일 가중합) |
|-----------|----------------------|
| `주도주_점수` | RS_등급×25% + 수급강도×20% + 영업이익CAGR×15% + Q_YoY×15% + 실적가속_연속×10% + 거래대금×10% + RSI_14×5% |
| `우량가치_점수` | PEG역순×30% + ROE×30% + F스코어×20% + PER역순×10% + 부채비율역순×10% |
| `고성장_점수` | Q_YoY×30% + 영업이익CAGR×30% + 매출CAGR×20% + MA20이격도×10% + 52주최고대비×10% |
| `현금배당_점수` | FCF수익률×30% + 배당수익률×30% + DPS_CAGR×20% + F스코어×10% + 부채비율역순×10% |
| `턴어라운드_점수` | 이익률변동폭×20% + 흑자전환×20% + 스마트머니_승률×20% + GPM변화×15% + F스코어×15% + 괴리율×10% |

---

## REST API

| 메서드 | 엔드포인트 | 설명 |
|--------|-----------|------|
| `GET` | `/` | 대시보드 UI |
| `GET` | `/api/stocks` | 종목 목록 (페이지네이션, 필터, 정렬) |
| `GET` | `/api/stocks/<code>` | 종목 상세 정보 |
| `GET` | `/api/stocks/<code>/financials` | 연간 재무제표 시계열 (차트용) |
| `GET` | `/api/markets/summary` | KOSPI/KOSDAQ 시장 요약 통계 |
| `POST` | `/api/batch/trigger` | 파이프라인 수동 트리거 |
| `GET` | `/api/batch/status` | 파이프라인 실행 상태 |
| `GET` | `/api/batch/changes` | 전략별 종목 편입/제거 변동 |
| `GET` | `/api/reports` | AI 분석 보고서 목록 |
| `GET` | `/api/reports/<code>` | AI 보고서 조회 |
| `POST` | `/api/reports/<code>` | AI 보고서 생성 (Claude API 호출) |
| `DELETE` | `/api/reports/<code>` | AI 보고서 삭제 |
| `GET` | `/api/status` | DB 데이터 현황 |

### `/api/stocks` 쿼리 파라미터

| 파라미터 | 설명 | 예시 |
|----------|------|------|
| `screen` | 전략 필터 | `all`, `leaders`, `quality_value`, `growth_mom`, `cash_div`, `turnaround`, `multi_strategy` |
| `market` | 시장 필터 | `KOSPI`, `KOSDAQ` |
| `q` | 종목명/코드 검색 | `삼성` |
| `sort` | 정렬 컬럼 | `종합점수` |
| `order` | 정렬 방향 | `asc`, `desc` |
| `page` | 페이지 번호 (1부터) | `1` |
| `size` | 페이지 크기 (최대 200) | `50` |
| `codes` | 관심종목 코드 목록 | `005930,000660` |
| `min_PER` | PER 최솟값 필터 | `5` |
| `max_PER` | PER 최댓값 필터 | `20` |

---

## 데이터베이스 스키마

### financial_statements

재무제표를 세로형(long format)으로 저장합니다.

| 컬럼 | 타입 | 설명 |
|------|------|------|
| 종목코드 | TEXT | 6자리 제로패딩 |
| 기준일 | TEXT | YYYY-MM-DD |
| 계정 | TEXT | 매출액, 영업이익, 당기순이익 등 |
| 주기 | TEXT | `y`=연간, `q`=분기 |
| 값 | DOUBLE | 재무 수치 |
| 추정치 | INTEGER | 0=실적, 1=추정(E) |
| collected_date | TEXT | 수집 날짜 |

### indicators

Financial Highlight와 재무비율 데이터.

| 지표구분 | 설명 |
|----------|------|
| `HIGHLIGHT` | FnGuide 메인 Financial Highlight |
| `RATIO_Y` | 연간 재무비율 |
| `RATIO_Q` | 분기 재무비율 |
| `DPS` | 주당배당금 |

---

## 주요 구현 패턴

### 종목코드 형식

항상 6자리 제로패딩을 사용합니다.

```python
code = str(x).zfill(6)   # "5930" → "005930"
```

### 단위 추론

삼성전자(005930) 매출액 규모를 기준으로 재무 데이터의 단위 배수를 자동 감지합니다.

```python
latest_rev > 1e14  →  multiplier = 1          # 원 단위
latest_rev > 1e8   →  multiplier = 1_000_000  # 백만원 단위
else               →  multiplier = 100_000_000 # 억원 단위
```

### 스크리닝 일관성 유지 (중요)

스크리닝 필터 로직은 두 곳에 존재합니다:

1. `quant_screener.py` — `apply_*_screen()` 함수
2. `webapp/app.py` — `_apply_screen_filter()` 함수

**스크리닝 조건을 변경할 때 반드시 두 파일을 동시에 수정해야 합니다.**

### dashboard_result 백업

`save_dashboard()` 호출 시 기존 `dashboard_result` 테이블을 `dashboard_result_prev`로 자동 백업하여 전 배치 결과와 비교(`/api/batch/changes`)할 수 있습니다.

---

## 개발 가이드

### 새 스크리닝 전략 추가

1. `quant_screener.py`에 `apply_new_screen(df)` 함수 추가
2. `pipeline.py`에서 해당 함수를 호출하고 엑셀 저장
3. `webapp/app.py`의 `_apply_screen_filter()`에 새 전략 조건 추가
4. `/api/stocks`의 `screen` 파라미터 처리에 새 전략명 추가

### 새 지표 추가

1. `quant_screener.py`의 `analyze_one_stock()` 또는 `calc_valuation()`에 계산 로직 추가
2. `db.py`의 `dashboard_result` 스키마에 컬럼 추가
3. `webapp/app.py`의 `DISPLAY_COLS` 리스트에 컬럼명 추가
4. (선택) `analysis/claude_analyzer.py`의 `QUANT_SECTIONS`에 AI 분석 입력으로 추가

### 테스트 모드

3개 샘플 종목(삼성전자·카카오·SK하이닉스)으로 전체 파이프라인을 빠르게 검증합니다.

```bash
python run.py pipeline --test
```

`TEST_TICKERS = ["005930", "035720", "000660"]` (`quant_collector_enhanced.py`)
