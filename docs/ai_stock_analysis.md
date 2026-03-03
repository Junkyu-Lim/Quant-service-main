# AI 개별 종목 분석 보고서

> **소스 코드**: `analysis/claude_analyzer.py` → `generate_report()`

## 개요

6대 투자 거장(Warren Buffett, Aswath Damodaran, Philip Fisher, Pat Dorsey, Peter Lynch, André Kostolany)의 핵심 철학을 기반으로 9단계(Stage 0~8) 심층 분석 프로토콜을 수행합니다.

- **모델**: `config.ANALYSIS_MODEL` (기본값: `claude-sonnet-4-6`)
- **max_tokens**: 16,384
- **웹 검색**: `web_search_20250305` 도구 사용 (최대 2회)
- **타임아웃**: 300초 (connect 30초)
- **재시도**: 최대 5회 (429, 529, 500, 502, 503 에러 시 지수 백오프)

---

## 분석 9단계 (Stage 0~8)

| Stage | 제목 | 핵심 내용 |
|-------|------|-----------|
| **0** | 핵심 사업 모델 정의 | 비즈니스 모델 3줄 요약, 핵심 제품·매출 비중, 환각 방지 의무 검증 |
| **1** | 거시 환경 & 밸류체인 | 전방산업 CAGR, 밸류체인 포지셔닝, 경쟁사 비교 우위 |
| **2** | 비즈니스 모델 수익성 해부 | P×Q×C 수익 구조 분석, 캐시카우 vs 신성장 동력 구분 |
| **3** | 기업 수명주기 & 4대 해자 | 도입기/성장기/성숙기/쇠퇴기 정의, 무형자산·전환비용·네트워크·원가우위 검증 |
| **4** | 실적 해부 & 재무 건전성 | 매출총이익률 추이, FCF 품질, 부채비율, 컨센서스 괴리율 |
| **5** | 향후 전망 & 모멘텀 | CAPEX·수주잔고·신사업, 12개월 내 상승/하락 촉매 |
| **6** | 밸류에이션 & 코스톨라니 달걀 | 수명주기 맞춤 밸류에이션, 달걀 모형 1~6단계 위치 판단 |
| **7** | 거장 6인 평가 & 통합 등급 | 각 거장 1줄평 + 3-5문장 분석, S~F 통합 등급 |
| **8** | 트레이딩 액션 플랜 | 진입가/목표가/손절가, 포트폴리오 비중·보유 기간 |

---

## 6대 투자 거장 철학

### 1. Warren Buffett (경제적 해자 & 안전마진)
- 경쟁우위 지속가능성, 사업모델 이해 용이성, 경영진 역량
- S-RIM 괴리율로 안전마진 측정, 장기보유 적합성

### 2. Aswath Damodaran (내재가치 & 내러티브)
- 성장단계 정의, 내러티브-숫자 일관성, 리스크 대비 보상
- ROIC vs WACC 관점의 재투자 효율성

### 3. Philip Fisher (성장잠재력 & 경영품질)
- R&D/혁신 역량, 이익률 개선 추세, 장기성장 잠재력
- 조직문화와 노사관계

### 4. Pat Dorsey (경제적 해자 심층분석)
- 4가지 해자 유형의 존재 여부와 강도
- 해자 트렌드(확대/유지/축소)

### 5. Peter Lynch (GARP & 생활밀착형 투자)
- PEG 비율로 성장 대비 가격 적정성 평가
- 10-bagger 잠재력, 이익 성장과 주가 연동성

### 6. André Kostolany (시장심리 & 역발상)
- 코스톨라니 달걀 모형의 현재 위치
- 역발상 투자 기회, 유동성/수급 분석

---

## 웹 검색 활용

분석 시 `web_search` 도구를 최대 2회 사용합니다:

1. **1차 검색**: `"{종목명} 최신 실적 공시 뉴스 {현재연도}"` — 기업 고유 이슈 파악
2. **2차 검색 (필요시)**: `"{산업명} 시장 전망 동향 {현재연도}"` — 산업 레벨 트렌드 보완

검색 결과는 Stage 1(거시환경), Stage 5(전망/촉매), Stage 8(액션 플랜)에 반영됩니다.

---

## 입력 데이터 (정량 지표)

`format_quant_data()` 함수가 `dashboard_result` 테이블의 종목 데이터를 아래 섹션별로 포맷팅하여 프롬프트에 전달합니다:

### 밸류에이션
`PER`, `PBR`, `PSR`, `PEG`, `ROE(%)`, `EPS`, `BPS`, `이익수익률(%)`, `적정주가_SRIM`, `괴리율(%)`

### 수익성
`영업이익률(%)`, `영업이익률_최근`, `영업이익률_전년`, `이익률_개선`, `이익률_급개선`, `이익률_변동폭`, `이익품질_양호`, `현금전환율(%)`, `FCF수익률(%)`

### 성장성
`매출_CAGR`, `영업이익_CAGR`, `순이익_CAGR`, `영업CF_CAGR`, `FCF_CAGR`, `매출_연속성장`, `영업이익_연속성장`, `순이익_연속성장`, `영업CF_연속성장`

### 재무건전성
`F스코어`, `부채비율(%)`, `부채상환능력`, `CAPEX비율(%)`, `흑자전환`

### 배당
`배당수익률(%)`, `DPS_최근`, `DPS_CAGR`, `배당_연속증가`, `배당_수익동반증가`

### 기술적 지표
`52주_최고대비(%)`, `52주_최저대비(%)`, `MA20_이격도(%)`, `MA60_이격도(%)`, `RSI_14`, `거래대금_20일평균`, `거래대금_증감(%)`, `변동성_60일(%)`

### TTM 실적
`TTM_매출`, `TTM_영업이익`, `TTM_순이익`, `TTM_영업CF`, `TTM_CAPEX`, `TTM_FCF`, `자본`, `부채`, `자산총계`

### 시가총액
`종가`, `시가총액`

---

## System Prompt

```
당신은 한국 주식시장 Grand Master 애널리스트입니다.
6대 투자 거장의 철학을 통합한 9단계 심층 분석 프로토콜(Stage 0~8)을 수행합니다.

[치명적 오류 방지 규정 - 최우선 준수]
분석 대상 기업의 '주력 사업 아이템'과 '속한 산업군'을 교차 검증하십시오.
종목명만 보고 반도체/바이오/2차전지 등으로 넘겨짚지 마십시오.
만약 기업 정보가 불확실하다면 hallucination_flag를 true로 설정하고,
"데이터 검증 불가로 분석을 보류합니다"라고 출력하십시오.

## Stage 0~8: (상단 9단계 표 참조)

## 6대 투자 거장 철학: (상단 참조)

## 최신 정보 활용 (웹 검색)
- web_search 도구를 활용하여 최신 뉴스·공시·실적발표·산업 동향을 반드시 확인
- 검색은 최대 2회로 제한
```

---

## User Prompt Template

```
아래 종목의 정량 데이터를 분석하여, 9단계 Grand Master 분석 보고서를 작성해주세요.

## 종목 정보
- 종목코드: {code}
- 종목명: {name}
- 시장: {market}

## 정량 데이터
{quant_data}

## 분석 지침

### [환각 방지 필수 확인]
"{name}" (코드: {code})의 실제 핵심 사업을 먼저 확인하세요.

### 정량 데이터 활용 가이드
- PEG, 매출CAGR, ROE → Stage 7 Lynch 분석에 직접 활용
- 괴리율(%)과 적정주가_SRIM → Buffett 안전마진 분석의 핵심 근거
- F스코어 → Stage 4 재무건전성 분석에 활용
- RSI, MA이격도 → Stage 6 코스톨라니 달걀 위치 판단에 활용
- TTM 실적과 CAGR → Stage 2 P×Q×C 분석의 수량적 근거
```

---

## 출력 JSON 스키마

```json
{
  "business_identity": {
    "core_business": "3줄 이내 핵심 사업 모델 요약",
    "key_products": "주요 제품/서비스",
    "revenue_breakdown": "매출 구성 (비중 포함)",
    "industry_classification": "산업 분류명",
    "confidence": "high|medium|low",
    "hallucination_flag": false
  },
  "stage1_macro": {
    "upstream_cagr": "전방산업 CAGR",
    "value_chain_position": "밸류체인 위치",
    "competitive_advantages": "경쟁 우위",
    "analysis": "3-5문장"
  },
  "stage2_business_model": {
    "p_times_q_analysis": "가격/수량/비용 동인",
    "cash_cow_drivers": "안정적 현금창출 사업부",
    "growth_drivers": "미래 성장 견인 사업부",
    "analysis": "3-5문장"
  },
  "stage3_moat": {
    "lifecycle_stage": "도입기|성장기|성숙기|쇠퇴기",
    "intangible_assets": { "exists": true, "evidence": "근거" },
    "switching_costs": { "exists": false, "evidence": "근거" },
    "network_effects": { "exists": false, "evidence": "근거" },
    "cost_advantage": { "exists": false, "evidence": "근거" },
    "moat_rating": "wide|narrow|none",
    "analysis": "3-5문장"
  },
  "stage4_financials": {
    "gross_margin_trend": "매출총이익률 추세",
    "fcf_quality": "FCF 품질",
    "debt_assessment": "부채 구조",
    "consensus_deviation": "컨센서스 대비",
    "analysis": "3-5문장"
  },
  "stage5_outlook": {
    "capex_signals": "CAPEX 현황",
    "order_backlog": "수주잔고",
    "new_business": "신사업",
    "catalysts_12m": ["촉매1", "촉매2", "촉매3"],
    "analysis": "3-5문장"
  },
  "stage6_valuation": {
    "lifecycle_matched_method": "밸류에이션 방법론",
    "fair_value_range": "적정 주가 범위",
    "kostolany_egg_position": 1-6,
    "market_psychology": "과열|중립|공포",
    "analysis": "3-5문장"
  },
  "stage7_masters": {
    "buffett":   { "score": 1-10, "one_liner": "한 줄 평", "analysis": "3-5문장" },
    "damodaran": { "score": 1-10, "one_liner": "한 줄 평", "analysis": "3-5문장" },
    "fisher":    { "score": 1-10, "one_liner": "한 줄 평", "analysis": "3-5문장" },
    "dorsey":    { "score": 1-10, "one_liner": "한 줄 평", "analysis": "3-5문장" },
    "lynch":     { "score": 1-10, "one_liner": "한 줄 평", "analysis": "3-5문장" },
    "kostolany": { "score": 1-10, "one_liner": "한 줄 평", "analysis": "3-5문장" }
  },
  "stage8_action": {
    "entry_price": "매수 진입 가격대",
    "target_price": "12개월 목표 주가",
    "stop_loss": "손절 기준",
    "portfolio_weight": "권장 비중 (예: 3-5%)",
    "holding_period": "권장 보유 기간",
    "analysis": "2-3문장 매매 근거"
  },
  "composite_score": 1-100,
  "investment_grade": "S|A|B+|B|C+|C|D|F",
  "summary": "5-7문장 종합 투자 의견",
  "risks": ["리스크1", "리스크2", "리스크3"],
  "catalysts": ["촉매1", "촉매2", "촉매3"]
}
```

### composite_score 가중치

| 항목 | 비중 |
|------|------|
| Buffett | 20% |
| Damodaran | 15% |
| Fisher | 15% |
| Dorsey | 15% |
| Lynch | 15% |
| Kostolany | 10% |
| 사업정체성 신뢰도 | 10% |

### investment_grade 체계

| 등급 | 의미 |
|------|------|
| S | 탁월 |
| A | 강매수 |
| B+ | 매수 |
| B | 보유 |
| C+ | 약보유 |
| C | 회피 |
| D | 우려 |
| F | 분석보류 |

---

## API 엔드포인트

| Method | URL | 설명 |
|--------|-----|------|
| `GET` | `/api/stocks/<code>/analysis` | 캐시된 보고서 조회 |
| `POST` | `/api/stocks/<code>/analysis` | 새 보고서 생성 (body: `{ "mode": "claude" }`) |

---

## 보고서 히스토리

분석 보고서는 `analysis_reports` 테이블에 최신 1건, `analysis_reports_history` 테이블에 이력이 저장됩니다. 재생성 시 기존 보고서와의 diff가 계산되어 `diff_html`로 반환됩니다.
