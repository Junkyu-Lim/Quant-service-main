# AI 포트폴리오 종합 분석 보고서

> **소스 코드**: `analysis/claude_analyzer.py` → `generate_portfolio_report()`

## 개요

투자자의 보유 포트폴리오 전체를 분석하여 포트폴리오 수준의 전략적 조언을 제공합니다. 개별 종목 분석과 달리 **종목 간 관계, 섹터 배분, 리밸런싱 전략**에 초점을 맞춥니다.

- **모델**: `config.PORTFOLIO_MODEL` (기본값: `claude-sonnet-4-6`)
- **max_tokens**: 32,768
- **웹 검색**: 사용하지 않음 (개별 분석과 다름)
- **타임아웃**: 600초 (connect 30초)
- **재시도**: 최대 5회 (429, 529, 500, 502, 503 에러 시 지수 백오프)

---

## 분석 프레임워크 (7단계)

| # | 분석 항목 | 핵심 내용 |
|---|-----------|-----------|
| **1** | 포트폴리오 건강도 평가 | 품질 점수 0-100, 분산도·밸류에이션·성장성·안정성 종합 |
| **2** | 종목별 액션 권고 | BUY_MORE / HOLD / TRIM / SELL + 구체적 매매 수량 |
| **3** | 섹터 집중도 분석 | 과집중/과소 섹터, 리밸런싱 권고 |
| **4** | 리스크 & 촉매 | 상관관계 기반 리스크, 포트폴리오 전체 촉매 |
| **5** | 보완 제안 | 부족한 섹터/테마/스타일 식별 |
| **6** | 관심종목 편입 권고 | ADD / WATCH / SKIP 판정 + 편입 시 시너지 분석 |
| **7** | 리밸런싱 실행 계획 | 우선순위 액션 + 실행 시기 권고 |

---

## 입력 데이터

### 포트폴리오 요약
- 총 종목수, 총평가금액, 총수익률, 섹터 분포

### 종목별 데이터 (`format_portfolio_stock()`)
각 종목마다 아래 정보가 포함됩니다:

- **보유 정보**: 보유수량, 평균매입가, 현재가, 수익률, 비중, 섹터
- **정량 데이터** (`format_quant_data()`): 밸류에이션, 수익성, 성장성, 재무건전성, 배당, 기술적 지표, TTM 실적, 시가총액
- **기존 AI 분석 요약** (있는 경우): 투자 등급, 종합 점수, 핵심 사업, 거장 6인 점수, 리스크/촉매

### 관심종목 (워치리스트)
- 포트폴리오에 없는 관심종목의 정량 데이터 제공
- 편입 여부 검토용

### 상관관계 행렬
- 종목 간 가격 상관관계 (최근 250 거래일 일별 수익률 기준)
- 0.7 이상: 고상관(분산 효과 부족), -0.3 미만: 역상관(분산 효과 양호)

---

## 종목별 액션 권고 상세

### 매매 수량 산출 로직

```
target_shares = abs(총평가금액 × (권장비중 - 현재비중) / 100) ÷ 목표가 중간값
estimated_amount = target_shares × 목표가 중간값
```

| 액션 | target_shares | 가격 범위 | 설명 |
|------|---------------|-----------|------|
| **BUY_MORE** | 추가 매수할 주수 | 매수 목표가 범위 | 비중 확대 |
| **HOLD** | 0 | 현재가 ±5% 감시 범위 | 유지 |
| **TRIM** | 매도할 주수 | 매도 목표가 범위 | 비중 축소 |
| **SELL** | 보유수량 전량 | 매도 목표가 범위 | 전량 매도 |

---

## System Prompt

```
당신은 한국 주식시장 포트폴리오 전략 어드바이저입니다.
투자자의 보유 포트폴리오 전체를 분석하여 포트폴리오 수준의 전략적 조언을 제공합니다.

## 분석 프레임워크
1. 포트폴리오 건강도 평가 (0-100)
2. 종목별 액션 권고 (BUY_MORE/HOLD/TRIM/SELL + 구체적 매매 수량)
3. 섹터 집중도 분석
4. 포트폴리오 리스크 & 촉매
5. 보완 제안
6. 관심종목 편입 권고 (ADD/WATCH/SKIP)
7. 리밸런싱 실행 계획

## 분석 지침
- 포트폴리오 비중이 높은 종목에 더 주의를 기울이세요
- 종목 간 상관관계와 중복 리스크를 식별하세요
- 데이터가 없는 종목(ETF, 우선주 등)은 비중 분석에만 포함
- 6대 투자 거장 관점이 기존 분석에 포함되어 있으면 종합 활용
- 한국어로 분석
```

---

## User Prompt Template

```
아래 포트폴리오의 전체 데이터를 분석하여, 포트폴리오 수준의 전략적 분석 보고서를 작성해주세요.

## 포트폴리오 요약
- 총 종목수: {stock_count}
- 총평가금액: {total_eval}원
- 총수익률: {total_return}%
- 섹터 분포: {sector_distribution}

## 종목별 데이터
{per_stock_sections}

{watchlist_section}
{correlation_section}
```

---

## 출력 JSON 스키마

```json
{
  "portfolio_health": {
    "score": 0,
    "grade": "B",
    "diversification": "분산도 평가 2-3문장",
    "valuation": "밸류에이션 평가 2-3문장",
    "growth_quality": "성장성/품질 평가 2-3문장",
    "overall_assessment": "종합 평가 3-5문장"
  },
  "stock_actions": [
    {
      "code": "종목코드",
      "name": "종목명",
      "action": "BUY_MORE|HOLD|TRIM|SELL",
      "current_weight": 0.0,
      "recommended_weight": 0.0,
      "target_shares": 0,
      "target_price_low": 0,
      "target_price_high": 0,
      "estimated_amount": 0,
      "rationale": "2-3문장 근거"
    }
  ],
  "sector_analysis": {
    "concentration_risk": "섹터 집중 리스크 2-3문장",
    "overweight_sectors": ["과비중 섹터"],
    "underweight_sectors": ["과소비중 섹터"],
    "rebalancing_suggestion": "리밸런싱 권고 2-3문장"
  },
  "portfolio_risks": [
    {
      "risk": "리스크 설명",
      "severity": "high|medium|low",
      "affected_stocks": ["종목코드"],
      "correlation_note": "상관관계 메모"
    }
  ],
  "portfolio_catalysts": [
    {
      "catalyst": "촉매 설명",
      "impact": "high|medium|low",
      "benefiting_stocks": ["종목코드"]
    }
  ],
  "missing_themes": [
    {
      "theme": "부족한 테마/섹터명",
      "reason": "추가 필요 근거 1-2문장"
    }
  ],
  "watchlist_recommendations": [
    {
      "code": "종목코드",
      "name": "종목명",
      "action": "ADD|WATCH|SKIP",
      "recommended_weight": 0.0,
      "target_shares": 0,
      "target_price_low": 0,
      "target_price_high": 0,
      "estimated_amount": 0,
      "rationale": "2-3문장 근거",
      "synergy": "포트폴리오 시너지/보완 효과"
    }
  ],
  "rebalancing_plan": {
    "urgency": "immediate|monthly|quarterly|none",
    "priority_actions": ["액션 1", "액션 2"],
    "execution_note": "실행 순서·시기 2-3문장"
  },
  "summary": "5-7문장 종합 포트폴리오 전략 의견"
}
```

---

## 캐시 & 무효화

- 분석 결과는 `portfolio_analysis` 테이블에 1건만 저장
- 포트폴리오 구성(종목코드, 수량, 평균매입가)의 MD5 해시로 캐시 유효성 판단
- 포트폴리오 변경 시 `stale=true` 반환 → UI에 "포트폴리오가 변경되었습니다" 배너 표시

---

## API 엔드포인트

| Method | URL | 설명 |
|--------|-----|------|
| `GET` | `/api/portfolio/analysis` | 캐시된 보고서 조회 (stale 여부 포함) |
| `POST` | `/api/portfolio/analysis` | 새 보고서 생성 (body: `{ "watchlist_codes": ["코드1", ...] }`) |

---

## 개별 분석과의 차이점

| 항목 | 개별 종목 분석 | 포트폴리오 분석 |
|------|----------------|-----------------|
| 모델 | `ANALYSIS_MODEL` (sonnet) | `PORTFOLIO_MODEL` (sonnet) |
| 웹 검색 | 사용 (최대 2회) | 미사용 |
| max_tokens | 16,384 | 32,768 |
| 분석 단위 | 종목 1개 | 포트폴리오 전체 |
| 핵심 관점 | 거장 6인 프레임워크 | 배분·상관관계·리밸런싱 |
| 입력 | 정량 데이터만 | 정량 + 기존 AI 분석 + 상관관계 + 관심종목 |
| 캐시 키 | 종목코드 | 포트폴리오 구성 해시 |
