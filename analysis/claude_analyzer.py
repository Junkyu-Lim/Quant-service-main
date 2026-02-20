"""
AI 종목 정성 분석 보고서 생성기 (Grand Master Protocol v2).

6대 투자 거장(Warren Buffett, Aswath Damodaran, Philip Fisher,
Pat Dorsey, Peter Lynch, André Kostolany)의 핵심 철학을 기반으로
9단계(Stage 0~8) 심층 분석 프로토콜을 수행합니다.

지원 모드:
  - gemini: Gemini API 단독 분석 (무료, Google Search grounding)
  - claude: Claude API 분석 (프리미엄)
"""

import json
import logging
from datetime import datetime

import anthropic

try:
    from google import genai
    from google.genai import types as genai_types
    _GENAI_AVAILABLE = True
except ImportError:
    _GENAI_AVAILABLE = False

import config

log = logging.getLogger("Analyzer")

# ─────────────────────────────────────────
# 프롬프트 템플릿
# ─────────────────────────────────────────

SYSTEM_PROMPT = """\
당신은 한국 주식시장 Grand Master 애널리스트입니다.
6대 투자 거장의 철학을 통합한 9단계 심층 분석 프로토콜(Stage 0~8)을 수행합니다.

[치명적 오류 방지 규정 - 최우선 준수]
분석 대상 기업의 '주력 사업 아이템'과 '속한 산업군'을 교차 검증하십시오.
종목명만 보고 반도체/바이오/2차전지 등으로 넘겨짚지 마십시오.
만약 기업 정보가 불확실하다면 hallucination_flag를 true로 설정하고,
"데이터 검증 불가로 분석을 보류합니다"라고 출력하십시오.

## Stage 0: 핵심 사업 모델 정의 [환각 방지 - 의무]
- 기업의 핵심 비즈니스 모델(무엇을 팔아서 돈을 버는가?)을 3줄로 요약
- 핵심 제품명과 매출 비중 필수 기재
- 종목명이 영문 약자(예: VT, SK, LG)인 경우 반드시 한국어 사명과 연결하여 사업 파악
- 코스메틱 기업을 반도체로, 게임사를 제조업으로 오인하는 오류를 절대 범하지 말 것

## Stage 1: 거시 환경 & 밸류체인 분석
- 전방산업 성장률 CAGR, 밸류체인 내 포지셔닝
- 국내외 핵심 경쟁사 대비 비교 우위

## Stage 2: 비즈니스 모델 수익성 해부
- P(판매가) × Q(판매량) × C(원가) 관점에서 수익 구조 분석
- 캐시카우 사업부와 신성장 동력을 구분

## Stage 3: 기업 수명주기 정의 & 4대 해자 검증
- 수명주기 단계: 도입기/성장기/성숙기/쇠퇴기
- 무형자산, 전환비용, 네트워크 효과, 원가우위 중 해당 사항을 데이터로 입증

## Stage 4: 부문별 실적 해부 & 재무 건전성
- 매출총이익률 추이, FCF 품질, 부채비율
- 컨센서스 대비 괴리율

## Stage 5: 향후 전망 & 모멘텀 분석
- CAPEX 증설 현황, 수주 잔고, 신사업 파이프라인
- 향후 1년 내 주가 상승/하락 촉매제

## Stage 6: 라이프사이클 맞춤 밸류에이션 & 코스톨라니 달걀 모형
- 기업 수명주기에 적합한 밸류에이션 방법론 적용
- 코스톨라니 달걀 모형에서 현재 시장 심리 위치(1~6단계) 판단

## Stage 7: 거장 6인의 개별 한 줄 평 & 통합 최종 판결 (S~F 등급)
- Buffett, Damodaran, Fisher, Dorsey, Lynch, Kostolany 각 1줄 요약 + 3-5문장 분석
- 통합 등급: S(탁월)/A(강매수)/B+(매수)/B(보유)/C+(약보유)/C(회피)/D(우려)/F(분석보류)

## Stage 8: 트레이딩 액션 플랜
- 진입가, 목표가, 손절가 제시
- 포트폴리오 비중 전략 및 권장 보유 기간

## 6대 투자 거장 철학

1. Warren Buffett (경제적 해자 & 안전마진)
   - 경쟁우위 지속가능성, 사업모델 이해 용이성, 경영진 역량
   - S-RIM 괴리율로 안전마진 측정, 장기보유 적합성

2. Aswath Damodaran (내재가치 & 내러티브)
   - 성장단계 정의, 내러티브-숫자 일관성, 리스크 대비 보상
   - ROIC vs WACC 관점의 재투자 효율성

3. Philip Fisher (성장잠재력 & 경영품질)
   - R&D/혁신 역량, 이익률 개선 추세, 장기성장 잠재력
   - 조직문화와 노사관계

4. Pat Dorsey (경제적 해자 심층분석)
   - 4가지 해자 유형의 존재 여부와 강도
   - 해자 트렌드(확대/유지/축소)

5. Peter Lynch (GARP & 생활밀착형 투자)
   - PEG 비율로 성장 대비 가격 적정성 평가
   - 투자자가 일상에서 이해할 수 있는 사업인가?
   - 10-bagger 잠재력, 이익 성장과 주가의 연동성

6. André Kostolany (시장심리 & 역발상)
   - 코스톨라니 달걀 모형의 현재 위치
   - 역발상 투자 기회, 유동성/수급 분석
   - 인내심 필요 정도
"""

# Claude 모드용: 정량 + 정성 데이터를 받아 9단계 Grand Master 분석
USER_PROMPT_TEMPLATE = """\
아래 종목의 정량 데이터를 분석하여, 9단계 Grand Master 분석 보고서를 작성해주세요.

## 종목 정보
- 종목코드: {code}
- 종목명: {name}
- 시장: {market}

## 정량 데이터
{quant_data}
{qualitative_section}
## 분석 지침

### [환각 방지 필수 확인]
"{name}" (코드: {code})의 실제 핵심 사업을 먼저 확인하세요.
- 이 기업이 어느 산업에 속하는지 명확히 판단하세요
- 금융 데이터만 보고 사업 분야를 추정하지 마세요
- 종목명만 보고 반도체/바이오/2차전지 등으로 넘겨짚지 마세요
- 불확실한 경우 hallucination_flag를 true로 설정하세요

### 정량 데이터 활용 가이드
- PEG, 매출CAGR, ROE → Stage 7 Lynch 분석에 직접 활용
- 괴리율(%)과 적정주가_SRIM → Buffett 안전마진 분석의 핵심 근거
- F스코어 → Stage 4 재무건전성 분석에 활용
- RSI, MA이격도 → Stage 6 코스톨라니 달걀 위치 판단에 활용
- TTM 실적과 CAGR → Stage 2 P×Q×C 분석의 수량적 근거

## 출력 형식

반드시 아래 JSON 형식으로만 응답하세요. JSON 외 다른 텍스트는 절대 포함하지 마세요.

```json
{{
  "business_identity": {{
    "core_business": "<3줄 이내 핵심 사업 모델 요약 - 무엇을 팔아서 돈을 버는가?>",
    "key_products": "<주요 제품/서비스 나열>",
    "revenue_breakdown": "<매출 구성 설명 (비중 포함)>",
    "industry_classification": "<산업 분류명>",
    "confidence": "<high|medium|low>",
    "hallucination_flag": false
  }},
  "stage1_macro": {{
    "upstream_cagr": "<전방산업 연간 성장률>",
    "value_chain_position": "<밸류체인 내 위치 설명>",
    "competitive_advantages": "<경쟁사 대비 주요 우위>",
    "analysis": "<3-5문장 분석>"
  }},
  "stage2_business_model": {{
    "p_times_q_analysis": "<가격/수량/비용 동인 분석>",
    "cash_cow_drivers": "<안정적 현금창출 사업부>",
    "growth_drivers": "<미래 성장 견인 사업부>",
    "analysis": "<3-5문장 분석>"
  }},
  "stage3_moat": {{
    "lifecycle_stage": "<도입기|성장기|성숙기|쇠퇴기>",
    "intangible_assets": {{"exists": true, "evidence": "<브랜드/특허/라이선스 근거>"}},
    "switching_costs": {{"exists": false, "evidence": "<전환비용 근거>"}},
    "network_effects": {{"exists": false, "evidence": "<네트워크효과 근거>"}},
    "cost_advantage": {{"exists": false, "evidence": "<비용우위 근거>"}},
    "moat_rating": "<wide|narrow|none>",
    "analysis": "<3-5문장 분석>"
  }},
  "stage4_financials": {{
    "gross_margin_trend": "<매출총이익률 추세 평가>",
    "fcf_quality": "<FCF 품질 및 지속가능성>",
    "debt_assessment": "<부채 구조 및 상환능력>",
    "consensus_deviation": "<시장 컨센서스 대비 현황>",
    "analysis": "<3-5문장 분석>"
  }},
  "stage5_outlook": {{
    "capex_signals": "<CAPEX 증설/축소 현황>",
    "order_backlog": "<수주잔고 및 파이프라인>",
    "new_business": "<신사업 및 사업 다각화>",
    "catalysts_12m": ["<12개월 내 촉매1>", "<촉매2>", "<촉매3>"],
    "analysis": "<3-5문장 분석>"
  }},
  "stage6_valuation": {{
    "lifecycle_matched_method": "<적용 밸류에이션 방법론 및 근거>",
    "fair_value_range": "<적정 주가 범위>",
    "kostolany_egg_position": <1-6 정수>,
    "market_psychology": "<과열|중립|공포>",
    "analysis": "<3-5문장 분석>"
  }},
  "stage7_masters": {{
    "buffett": {{"score": <1-10>, "one_liner": "<한 줄 평>", "analysis": "<3-5문장>"}},
    "damodaran": {{"score": <1-10>, "one_liner": "<한 줄 평>", "analysis": "<3-5문장>"}},
    "fisher": {{"score": <1-10>, "one_liner": "<한 줄 평>", "analysis": "<3-5문장>"}},
    "dorsey": {{"score": <1-10>, "one_liner": "<한 줄 평>", "analysis": "<3-5문장>"}},
    "lynch": {{"score": <1-10>, "one_liner": "<한 줄 평>", "analysis": "<3-5문장>"}},
    "kostolany": {{"score": <1-10>, "one_liner": "<한 줄 평>", "analysis": "<3-5문장>"}}
  }},
  "stage8_action": {{
    "entry_price": "<매수 진입 가격대 (원)>",
    "target_price": "<12개월 목표 주가 (원)>",
    "stop_loss": "<손절 기준 가격 또는 조건>",
    "portfolio_weight": "<권장 포트폴리오 비중 (예: 3-5%)>",
    "holding_period": "<권장 보유 기간>",
    "analysis": "<2-3문장 매매 근거>"
  }},
  "composite_score": <1-100 정수, 가중평균: Buffett 20%, Damodaran 15%, Fisher 15%, Dorsey 15%, Lynch 15%, Kostolany 10%, 사업정체성 신뢰도 10%>,
  "investment_grade": "<S|A|B+|B|C+|C|D|F 중 하나>",
  "summary": "<5-7문장 종합 투자 의견>",
  "risks": ["<핵심 리스크1>", "<리스크2>", "<리스크3>"],
  "catalysts": ["<핵심 촉매1>", "<촉매2>", "<촉매3>"]
}}
```
"""

# Gemini 단독 분석용 프롬프트 (시스템 프롬프트 + 9단계 분석 지시를 하나로 통합)
GEMINI_ANALYSIS_PROMPT = """\
당신은 한국 주식시장 Grand Master 애널리스트입니다.
Google 검색을 통해 이 기업의 사업 정보, 최신 뉴스, 산업 동향을 직접 수집하고,
제공된 정량 데이터와 함께 9단계 Grand Master 분석 보고서를 작성해주세요.

## [환각 방지 최우선 규칙]

**가장 먼저** Google 검색으로 다음을 확인하세요:
1. "{name}" 기업의 실제 핵심 사업 (검색어: "{name} 사업 제품")
2. 종목코드 {code}가 어떤 기업인지 교차 확인
3. 이 기업이 속한 산업 분류

만약 검색 결과와 재무 데이터가 모순된다면(예: 검색에서 코스메틱 기업으로 나오는데
데이터만 보면 반도체처럼 보임), 반드시 검색 결과를 우선하세요.
기업 정보가 불확실하면 hallucination_flag를 true로 설정하고
"데이터 검증 불가로 분석을 보류합니다"라고 출력하세요.

## 9단계 분석 프레임워크

### Stage 0: 사업 정체성 확인 [Google Search 필수]
- 핵심 사업 모델 3줄 요약, 주요 제품/서비스 및 매출 구성

### Stage 1: 거시환경 & 밸류체인 분석
- 전방산업 CAGR, 밸류체인 포지셔닝, 경쟁사 대비 우위

### Stage 2: 사업모델 수익성 해부
- P×Q×C 분석, 캐시카우 vs 성장 드라이버

### Stage 3: 기업 수명주기 & 4대 해자 검증
- 무형자산/전환비용/네트워크효과/비용우위 - 각각 데이터 근거 포함

### Stage 4: 부문별 실적 & 재무건전성
- 매출총이익률 추세, FCF, 부채, 컨센서스 괴리

### Stage 5: 향후 전망 & 모멘텀
- CAPEX, 수주잔고, 신사업, 1년내 촉매

### Stage 6: 라이프사이클 맞춤 밸류에이션 & 코스톨라니 달걀
- 적합한 밸류에이션 방법론, 달걀 위치(1~6단계)

### Stage 7: 6대 거장 원라이너 + 최종 판결 (S~F 등급)
- Buffett(해자/안전마진), Damodaran(내재가치/내러티브),
  Fisher(성장/경영품질), Dorsey(해자심층), Lynch(GARP/생활밀착), Kostolany(시장심리)

### Stage 8: 트레이딩 액션 플랜
- 진입가, 목표주가, 손절기준, 포트폴리오 비중

## 종목 정보
- 종목코드: {code}
- 종목명: {name}
- 시장: {market}

## 정량 데이터
{quant_data}

## 지시사항
1. Google Search로 이 기업의 사업 정체성을 먼저 확인하세요 (환각 방지)
2. 검색으로 최신 뉴스, 산업 동향, 경쟁 환경을 수집하세요
3. 반드시 아래 JSON 형식으로만 응답하세요. JSON 외 다른 텍스트는 절대 포함하지 마세요.

```json
{{
  "business_identity": {{
    "core_business": "<3줄 이내 핵심 사업 모델 요약>",
    "key_products": "<주요 제품/서비스>",
    "revenue_breakdown": "<매출 구성>",
    "industry_classification": "<산업 분류>",
    "confidence": "<high|medium|low>",
    "hallucination_flag": false
  }},
  "stage1_macro": {{
    "upstream_cagr": "<전방산업 성장률>",
    "value_chain_position": "<밸류체인 포지션>",
    "competitive_advantages": "<경쟁 우위>",
    "analysis": "<3-5문장>"
  }},
  "stage2_business_model": {{
    "p_times_q_analysis": "<P×Q×C 분석>",
    "cash_cow_drivers": "<캐시카우 사업부>",
    "growth_drivers": "<성장 드라이버>",
    "analysis": "<3-5문장>"
  }},
  "stage3_moat": {{
    "lifecycle_stage": "<도입기|성장기|성숙기|쇠퇴기>",
    "intangible_assets": {{"exists": true, "evidence": "<근거>"}},
    "switching_costs": {{"exists": false, "evidence": "<근거>"}},
    "network_effects": {{"exists": false, "evidence": "<근거>"}},
    "cost_advantage": {{"exists": false, "evidence": "<근거>"}},
    "moat_rating": "<wide|narrow|none>",
    "analysis": "<3-5문장>"
  }},
  "stage4_financials": {{
    "gross_margin_trend": "<매출총이익률 추세>",
    "fcf_quality": "<FCF 품질>",
    "debt_assessment": "<부채 평가>",
    "consensus_deviation": "<컨센서스 괴리>",
    "analysis": "<3-5문장>"
  }},
  "stage5_outlook": {{
    "capex_signals": "<CAPEX 신호>",
    "order_backlog": "<수주/파이프라인>",
    "new_business": "<신사업>",
    "catalysts_12m": ["<촉매1>", "<촉매2>", "<촉매3>"],
    "analysis": "<3-5문장>"
  }},
  "stage6_valuation": {{
    "lifecycle_matched_method": "<밸류에이션 방법론>",
    "fair_value_range": "<적정가치 범위>",
    "kostolany_egg_position": <1-6>,
    "market_psychology": "<과열|중립|공포>",
    "analysis": "<3-5문장>"
  }},
  "stage7_masters": {{
    "buffett": {{"score": <1-10>, "one_liner": "<한 줄 평>", "analysis": "<3-5문장>"}},
    "damodaran": {{"score": <1-10>, "one_liner": "<한 줄 평>", "analysis": "<3-5문장>"}},
    "fisher": {{"score": <1-10>, "one_liner": "<한 줄 평>", "analysis": "<3-5문장>"}},
    "dorsey": {{"score": <1-10>, "one_liner": "<한 줄 평>", "analysis": "<3-5문장>"}},
    "lynch": {{"score": <1-10>, "one_liner": "<한 줄 평>", "analysis": "<3-5문장>"}},
    "kostolany": {{"score": <1-10>, "one_liner": "<한 줄 평>", "analysis": "<3-5문장>"}}
  }},
  "stage8_action": {{
    "entry_price": "<진입가>",
    "target_price": "<목표주가>",
    "stop_loss": "<손절기준>",
    "portfolio_weight": "<비중>",
    "holding_period": "<보유기간>",
    "analysis": "<2-3문장>"
  }},
  "composite_score": <1-100 정수, 가중평균: Buffett 20%, Damodaran 15%, Fisher 15%, Dorsey 15%, Lynch 15%, Kostolany 10%, 사업정체성 신뢰도 10%>,
  "investment_grade": "<S|A|B+|B|C+|C|D|F>",
  "summary": "<5-7문장 종합 투자 의견>",
  "risks": ["<리스크1>", "<리스크2>", "<리스크3>"],
  "catalysts": ["<촉매1>", "<촉매2>", "<촉매3>"]
}}
```
"""

# Gemini 정성 자료 수집용 프롬프트 (Claude 모드에서 사전 수집)
GEMINI_RESEARCH_PROMPT = """\
다음 한국 상장기업에 대해 Google 검색을 통해 최신 정성 정보를 수집해주세요.

## 기업 정보
- 종목코드: {code}
- 종목명: {name}
- 시장: {market}

## 수집 항목 (각 항목 3-5문장, 한국어로 작성)

### 0. 핵심 사업 정체성 확인 [최우선 - 환각 방지]
"{name}" (종목코드: {code})의 실제 핵심 사업을 확인하세요.
검색어 예시: "{name} 사업", "{name} 제품", "{name} IR"
- 이 기업이 어느 산업에 속하는지 (예: 화장품, 반도체, 게임, 식품 등)
- 주요 제품/서비스 3가지
- 대략적인 매출 구성 비율

### 1. 기업 개요 및 사업 모델
이 기업의 주요 사업 영역, 핵심 제품/서비스, 매출 구조, 경쟁 포지션을 간략히 설명해주세요.

### 2. 최근 뉴스 및 주요 이벤트 (최근 6개월 이내)
최근 공시, 실적 발표, 신사업 진출, 경영진 변화, M&A, 대규모 계약 등 주요 이벤트를 나열해주세요.

### 3. 산업 트렌드 및 업황
이 기업이 속한 산업의 현재 트렌드, 성장 동력, 규제 환경, 경쟁 구도 변화를 설명해주세요.

### 4. CAPEX & 성장 투자 신호
최근 설비투자, 공장 증설, R&D 확대, 신사업 진출 관련 공시나 뉴스를 수집해주세요.

### 5. 주요 리스크 및 기회 요인
현재 이 기업이 직면한 주요 외부 리스크(원자재, 환율, 경쟁, 규제 등)와 성장 기회(신시장, 기술혁신 등)를 설명해주세요.

검색 결과를 바탕으로 사실에 근거한 정보만 작성하세요.
정보가 불확실한 경우 "확인 필요"로 표시하세요.
특히 섹션 0(핵심 사업 정체성)은 반드시 정확히 작성해야 합니다.
"""


# ─────────────────────────────────────────
# 정량 데이터 포맷팅
# ─────────────────────────────────────────

# 분석에 포함할 지표 그룹
QUANT_SECTIONS = {
    "밸류에이션": [
        ("PER", "f2"), ("PBR", "f2"), ("PSR", "f2"), ("PEG", "f2"),
        ("ROE(%)", "f2"), ("EPS", "int"), ("BPS", "int"),
        ("이익수익률(%)", "f2"), ("적정주가_SRIM", "int"), ("괴리율(%)", "f2"),
    ],
    "수익성": [
        ("영업이익률(%)", "f2"), ("영업이익률_최근", "f2"), ("영업이익률_전년", "f2"),
        ("이익률_개선", "flag"), ("이익률_급개선", "flag"), ("이익률_변동폭", "f2"),
        ("이익품질_양호", "flag"), ("현금전환율(%)", "f1"), ("FCF수익률(%)", "f2"),
    ],
    "성장성": [
        ("매출_CAGR", "f1"), ("영업이익_CAGR", "f1"), ("순이익_CAGR", "f1"),
        ("영업CF_CAGR", "f1"), ("FCF_CAGR", "f1"),
        ("매출_연속성장", "int"), ("영업이익_연속성장", "int"),
        ("순이익_연속성장", "int"), ("영업CF_연속성장", "int"),
    ],
    "재무건전성": [
        ("F스코어", "int"), ("부채비율(%)", "f1"),
        ("부채상환능력", "f2"), ("CAPEX비율(%)", "f1"),
        ("흑자전환", "flag"),
    ],
    "배당": [
        ("배당수익률(%)", "f2"), ("DPS_최근", "int"), ("DPS_CAGR", "f2"),
        ("배당_연속증가", "int"), ("배당_수익동반증가", "flag"),
    ],
    "기술적 지표": [
        ("52주_최고대비(%)", "f1"), ("52주_최저대비(%)", "f1"),
        ("MA20_이격도(%)", "f1"), ("MA60_이격도(%)", "f1"),
        ("RSI_14", "f1"), ("거래대금_20일평균", "int"),
        ("거래대금_증감(%)", "f1"), ("변동성_60일(%)", "f1"),
    ],
    "TTM 실적": [
        ("TTM_매출", "int"), ("TTM_영업이익", "int"), ("TTM_순이익", "int"),
        ("TTM_영업CF", "int"), ("TTM_CAPEX", "int"), ("TTM_FCF", "int"),
        ("자본", "int"), ("부채", "int"), ("자산총계", "int"),
    ],
    "시가총액": [
        ("종가", "int"), ("시가총액", "int"),
    ],
}


def _fmt_val(v, fmt_type: str) -> str:
    if v is None:
        return "N/A"
    try:
        if fmt_type == "int":
            return f"{int(float(v)):,}"
        if fmt_type == "f1":
            return f"{float(v):.1f}"
        if fmt_type == "f2":
            return f"{float(v):.2f}"
        if fmt_type == "flag":
            return "O" if int(float(v)) == 1 else "X"
    except (ValueError, TypeError):
        return str(v)
    return str(v)


def format_quant_data(stock: dict) -> str:
    """종목 데이터를 분석용 텍스트로 포맷팅."""
    lines = []
    for section, metrics in QUANT_SECTIONS.items():
        lines.append(f"\n### {section}")
        for col, fmt_type in metrics:
            val = stock.get(col)
            lines.append(f"- {col}: {_fmt_val(val, fmt_type)}")
    return "\n".join(lines)


def _parse_json_response(raw_text: str) -> dict:
    """AI 응답에서 JSON을 파싱합니다. ```json 블록을 자동 제거합니다."""
    json_str = raw_text
    if "```json" in json_str:
        json_str = json_str.split("```json", 1)[1]
    if "```" in json_str:
        json_str = json_str.split("```", 1)[0]
    return json.loads(json_str.strip())


# ─────────────────────────────────────────
# Gemini API 호출
# ─────────────────────────────────────────

def collect_qualitative_data(stock: dict) -> str:
    """
    Gemini API (Google Search grounding)로 종목의 정성 데이터를 수집합니다.

    Returns:
        수집된 정성 데이터 텍스트. 실패 시 빈 문자열 반환.
    """
    if not config.GEMINI_API_KEY:
        log.warning("GEMINI_API_KEY가 설정되지 않아 정성 데이터 수집을 건너뜁니다.")
        return ""
    if not _GENAI_AVAILABLE:
        log.warning("google-genai 패키지가 설치되지 않아 정성 데이터 수집을 건너뜁니다.")
        return ""

    code = str(stock.get("종목코드", "")).zfill(6)
    name = stock.get("종목명", "Unknown")
    market = stock.get("시장구분", "")

    prompt = GEMINI_RESEARCH_PROMPT.format(code=code, name=name, market=market)

    try:
        client = genai.Client(api_key=config.GEMINI_API_KEY)
        response = client.models.generate_content(
            model=config.GEMINI_RESEARCH_MODEL,
            contents=prompt,
            config=genai_types.GenerateContentConfig(
                tools=[genai_types.Tool(google_search=genai_types.GoogleSearch())],
                temperature=0.3,
                max_output_tokens=2000,
            ),
        )
        qualitative_text = response.text.strip()
        log.info("Gemini 정성 데이터 수집 완료: %s %s (%d chars)",
                 code, name, len(qualitative_text))
        return qualitative_text
    except Exception as e:
        log.warning("Gemini 정성 데이터 수집 실패 (%s %s): %s", code, name, str(e)[:120])
        return ""


def _generate_report_gemini(stock: dict) -> dict:
    """Gemini API 단독으로 분석 보고서를 생성합니다 (무료)."""
    if not config.GEMINI_API_KEY:
        raise ValueError("GEMINI_API_KEY 환경변수가 설정되지 않았습니다.")
    if not _GENAI_AVAILABLE:
        raise ValueError("google-genai 패키지가 설치되지 않았습니다. pip install google-genai")

    code = str(stock.get("종목코드", "")).zfill(6)
    name = stock.get("종목명", "Unknown")
    market = stock.get("시장구분", "")

    quant_text = format_quant_data(stock)
    prompt = GEMINI_ANALYSIS_PROMPT.format(
        code=code, name=name, market=market, quant_data=quant_text,
    )

    client = genai.Client(api_key=config.GEMINI_API_KEY)
    response = client.models.generate_content(
        model=config.GEMINI_RESEARCH_MODEL,
        contents=prompt,
        config=genai_types.GenerateContentConfig(
            tools=[genai_types.Tool(google_search=genai_types.GoogleSearch())],
            temperature=0.5,
            max_output_tokens=8192,
        ),
    )

    raw_text = response.text.strip()
    model_name = config.GEMINI_RESEARCH_MODEL

    try:
        scores = _parse_json_response(raw_text)
    except json.JSONDecodeError as e:
        log.error("Gemini JSON 파싱 실패 (%s %s): %s", code, name, str(e)[:100])
        log.debug("시도한 텍스트: %s", raw_text[:300])
        return {
            "scores": {},
            "report_html": "<p>오류: JSON 파싱 실패</p>",
            "error": str(e),
            "model": config.GEMINI_RESEARCH_MODEL,
            "generated_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "mode": "gemini",
        }

    generated_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    report_html = render_html(code, name, market, stock, scores,
                              generated_date, model_label=f"Gemini ({model_name})")

    return {
        "scores": scores,
        "report_html": report_html,
        "model": model_name,
        "generated_date": generated_date,
        "mode": "gemini",
    }


# ─────────────────────────────────────────
# Claude API 호출 (+ Gemini 정성 자료 수집)
# ─────────────────────────────────────────

def _generate_report_claude(stock: dict) -> dict:
    """Claude API만 사용하여 분석 보고서를 생성합니다 (비용 절약)."""
    if not config.ANTHROPIC_API_KEY:
        raise ValueError("ANTHROPIC_API_KEY 환경변수가 설정되지 않았습니다.")

    code = str(stock.get("종목코드", "")).zfill(6)
    name = stock.get("종목명", "Unknown")
    market = stock.get("시장구분", "")

    quant_text = format_quant_data(stock)

    # Claude 모드는 정량 데이터만 사용 (비용 절약, Gemini 호출 없음)
    qualitative_section = ""

    user_prompt = USER_PROMPT_TEMPLATE.format(
        code=code, name=name, market=market,
        quant_data=quant_text,
        qualitative_section=qualitative_section,
    )

    client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)
    message = client.messages.create(
        model=config.ANALYSIS_MODEL,
        max_tokens=16384,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_prompt}],
    )

    raw_text = message.content[0].text.strip()

    try:
        scores = _parse_json_response(raw_text)
    except json.JSONDecodeError as e:
        log.error("Claude JSON 파싱 실패 (%s %s): %s", code, name, str(e)[:100])
        log.debug("시도한 JSON: %s", raw_text[:200])
        return {
            "scores": {},
            "report_html": "<p>오류: JSON 파싱 실패</p>",
            "error": str(e),
            "model": config.ANALYSIS_MODEL,
            "generated_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "mode": "claude",
        }

    model_label = f"Claude ({config.ANALYSIS_MODEL})"

    generated_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    report_html = render_html(code, name, market, stock, scores,
                              generated_date, model_label=model_label)

    return {
        "scores": scores,
        "report_html": report_html,
        "model": config.ANALYSIS_MODEL,
        "generated_date": generated_date,
        "mode": "claude",
    }


# ─────────────────────────────────────────
# 통합 엔트리포인트
# ─────────────────────────────────────────

def generate_report(stock: dict, mode: str = "gemini") -> dict:
    """
    종목 분석 보고서를 생성합니다.

    Args:
        stock: dashboard_result의 한 종목 데이터 (dict)
        mode: "gemini" (무료, Gemini 단독) 또는 "claude" (프리미엄, Gemini 수집 + Claude 분석)

    Returns:
        {
            "scores": { ... },
            "report_html": "...",
            "model": "...",
            "generated_date": "...",
            "mode": "gemini" | "claude",
        }
    """
    if mode == "claude":
        return _generate_report_claude(stock)
    return _generate_report_gemini(stock)


# ─────────────────────────────────────────
# HTML 보고서 렌더링
# ─────────────────────────────────────────

MASTER_INFO = {
    "buffett": {
        "name": "Warren Buffett",
        "icon": "WB",
        "color": "#1a5276",
        "philosophy": "경제적 해자 & 안전마진",
    },
    "damodaran": {
        "name": "Aswath Damodaran",
        "icon": "AD",
        "color": "#7d3c98",
        "philosophy": "내재가치 & 내러티브",
    },
    "fisher": {
        "name": "Philip Fisher",
        "icon": "PF",
        "color": "#1e8449",
        "philosophy": "성장 잠재력 & 경영 품질",
    },
    "dorsey": {
        "name": "Pat Dorsey",
        "icon": "PD",
        "color": "#b9770e",
        "philosophy": "경제적 해자 심층 분석",
    },
    "lynch": {
        "name": "Peter Lynch",
        "icon": "PL",
        "color": "#148f77",
        "philosophy": "GARP & 생활밀착형 투자",
    },
    "kostolany": {
        "name": "André Kostolany",
        "icon": "AK",
        "color": "#c0392b",
        "philosophy": "시장 심리 & 역발상",
    },
}

KOSTOLANY_EGG_LABELS = {
    1: "극도 비관 (매수 적기)",
    2: "비관적 상승 전환",
    3: "낙관 시작",
    4: "극도 낙관 (매도 적기)",
    5: "낙관적 하락 전환",
    6: "비관 시작",
}


def _grade_color(grade: str) -> str:
    colors = {
        "S": "#0d1b2a", "A": "#1a5276", "B+": "#1e8449",
        "B": "#7d8c3c", "C+": "#b9770e", "C": "#d35400",
        "D": "#c0392b", "F": "#7b241c",
        # 구버전 호환
        "A+": "#1a5276",
    }
    return colors.get(grade, "#6c757d")


def _score_bar_width(score: int, max_score: int = 10) -> int:
    return max(5, min(100, int(score / max_score * 100)))


def render_html(code: str, name: str, market: str, stock: dict,
                scores: dict, generated_date: str,
                model_label: str = None) -> str:
    """분석 결과를 HTML 보고서로 렌더링 (9단계 Grand Master 프로토콜)."""

    if model_label is None:
        model_label = f"Claude ({config.ANALYSIS_MODEL})"

    # ── 구버전 호환: stage7_masters가 없으면 기존 root-level 구조를 변환 ──
    if "stage7_masters" in scores:
        stage7 = scores.get("stage7_masters", {})
    else:
        stage7 = {}
        for k in ["buffett", "damodaran", "fisher", "dorsey", "kostolany"]:
            old = scores.get(k, {})
            stage7[k] = {
                "score": old.get("score", 0),
                "one_liner": old.get("title", ""),
                "analysis": old.get("analysis", ""),
            }
        stage7["lynch"] = {"score": 0, "one_liner": "구버전 보고서", "analysis": ""}

    business_identity = scores.get("business_identity", {})
    stage1 = scores.get("stage1_macro", {})
    stage2 = scores.get("stage2_business_model", {})
    stage3 = scores.get("stage3_moat", {})
    stage4 = scores.get("stage4_financials", {})
    stage5 = scores.get("stage5_outlook", {})
    stage6 = scores.get("stage6_valuation", {})
    stage8 = scores.get("stage8_action", {})

    composite = scores.get("composite_score", 0)
    grade = scores.get("investment_grade", "N/A")
    summary = scores.get("summary", "")
    risks = scores.get("risks", [])
    catalysts = scores.get("catalysts", [])

    grade_color = _grade_color(grade)

    # ── 환각 경고 배너 ──
    hallucination_flag = business_identity.get("hallucination_flag", False)
    hallucination_banner = ""
    if hallucination_flag:
        hallucination_banner = """
  <div class="hallucination-alert">
    <strong>데이터 검증 불가로 분석을 보류합니다.</strong><br>
    사업 분류를 직접 확인 후 재분석을 요청하시기 바랍니다.
  </div>"""

    # ── Stage 0: 사업 정체성 박스 ──
    confidence = business_identity.get("confidence", "medium")
    conf_cls = {"high": "confidence-high", "medium": "confidence-medium",
                "low": "confidence-low"}.get(confidence, "confidence-medium")
    conf_label = {"high": "신뢰도 높음", "medium": "신뢰도 보통",
                  "low": "신뢰도 낮음"}.get(confidence, "")

    identity_html = ""
    if business_identity:
        identity_html = f"""
  <div class="business-identity-box">
    <div class="stage-card-title">STAGE 0 &mdash; 사업 정체성 확인
      <span class="identity-confidence-badge {conf_cls}">{conf_label}</span>
    </div>
    <div class="identity-industry"><strong>산업:</strong> {business_identity.get("industry_classification", "N/A")}</div>
    <div class="identity-core"><strong>핵심 사업:</strong> {business_identity.get("core_business", "N/A")}</div>
    <div class="identity-products"><strong>주요 제품:</strong> {business_identity.get("key_products", "N/A")}</div>
    <div class="identity-revenue"><strong>매출 구성:</strong> {business_identity.get("revenue_breakdown", "N/A")}</div>
  </div>"""

    # ── 주요 지표 요약 Pills ──
    key_metrics = ""
    for label, col, fmt in [
        ("PER", "PER", "f2"), ("PBR", "PBR", "f2"), ("ROE", "ROE(%)", "f1"),
        ("F-Score", "F스코어", "int"), ("영업이익률", "영업이익률(%)", "f1"),
        ("괴리율", "괴리율(%)", "f1"),
    ]:
        key_metrics += (
            f'<div class="kv-pill">'
            f'<span class="kv-label">{label}</span>'
            f'<span class="kv-value">{_fmt_val(stock.get(col), fmt)}</span>'
            f'</div>'
        )

    # ── Stage 1-2: 거시환경 & 사업모델 (2열 그리드) ──
    stage12_html = ""
    if stage1 or stage2:
        stage12_html = f"""
  <div class="stage-grid-2col">
    <div class="stage-card">
      <div class="stage-card-title">STAGE 1 &mdash; 거시환경 &amp; 밸류체인</div>
      <div class="stage-field"><strong>전방산업 CAGR:</strong> {stage1.get("upstream_cagr", "N/A")}</div>
      <div class="stage-field"><strong>밸류체인 포지션:</strong> {stage1.get("value_chain_position", "N/A")}</div>
      <div class="stage-field"><strong>경쟁 우위:</strong> {stage1.get("competitive_advantages", "N/A")}</div>
      <div class="stage-analysis">{stage1.get("analysis", "")}</div>
    </div>
    <div class="stage-card">
      <div class="stage-card-title">STAGE 2 &mdash; 수익성 해부 (P&times;Q&times;C)</div>
      <div class="stage-field"><strong>수익 구조:</strong> {stage2.get("p_times_q_analysis", "N/A")}</div>
      <div class="stage-field"><strong>캐시카우:</strong> {stage2.get("cash_cow_drivers", "N/A")}</div>
      <div class="stage-field"><strong>성장 동력:</strong> {stage2.get("growth_drivers", "N/A")}</div>
      <div class="stage-analysis">{stage2.get("analysis", "")}</div>
    </div>
  </div>"""

    # ── Stage 3: 해자 분석 ──
    moat_html = ""
    if stage3:
        moat_types = [
            ("intangible_assets", "무형자산"),
            ("switching_costs", "전환비용"),
            ("network_effects", "네트워크효과"),
            ("cost_advantage", "비용우위"),
        ]
        moat_items = ""
        for moat_key, moat_label in moat_types:
            m = stage3.get(moat_key, {})
            exists = m.get("exists", False)
            evidence = m.get("evidence", "")
            cls = "has-moat" if exists else "no-moat"
            icon = "&#10003;" if exists else "&#10007;"
            moat_items += f"""
        <div class="moat-item {cls}">
          <strong>{icon} {moat_label}</strong><br>
          <span class="moat-evidence">{evidence}</span>
        </div>"""

        moat_rating = stage3.get("moat_rating", "none")
        moat_rating_label = {"wide": "넓은 해자 (Wide)", "narrow": "좁은 해자 (Narrow)",
                             "none": "해자 없음 (None)"}.get(moat_rating, moat_rating)

        moat_html = f"""
  <div class="stage-card moat-card">
    <div class="stage-card-title">STAGE 3 &mdash; 수명주기 &amp; 4대 해자 검증</div>
    <div class="stage-field"><strong>수명주기:</strong> {stage3.get("lifecycle_stage", "N/A")}
      &nbsp;&nbsp;<strong>해자 등급:</strong> {moat_rating_label}</div>
    <div class="moat-grid">{moat_items}
    </div>
    <div class="stage-analysis">{stage3.get("analysis", "")}</div>
  </div>"""

    # ── Stage 4-5: 재무건전성 & 전망 (2열 그리드) ──
    stage45_html = ""
    if stage4 or stage5:
        catalysts_12m = stage5.get("catalysts_12m", [])
        catalysts_12m_html = ", ".join(catalysts_12m) if catalysts_12m else "N/A"

        stage45_html = f"""
  <div class="stage-grid-2col">
    <div class="stage-card">
      <div class="stage-card-title">STAGE 4 &mdash; 실적 해부 &amp; 재무 건전성</div>
      <div class="stage-field"><strong>매출총이익률 추세:</strong> {stage4.get("gross_margin_trend", "N/A")}</div>
      <div class="stage-field"><strong>FCF 품질:</strong> {stage4.get("fcf_quality", "N/A")}</div>
      <div class="stage-field"><strong>부채 평가:</strong> {stage4.get("debt_assessment", "N/A")}</div>
      <div class="stage-field"><strong>컨센서스 괴리:</strong> {stage4.get("consensus_deviation", "N/A")}</div>
      <div class="stage-analysis">{stage4.get("analysis", "")}</div>
    </div>
    <div class="stage-card">
      <div class="stage-card-title">STAGE 5 &mdash; 전망 &amp; 모멘텀</div>
      <div class="stage-field"><strong>CAPEX 신호:</strong> {stage5.get("capex_signals", "N/A")}</div>
      <div class="stage-field"><strong>수주/파이프라인:</strong> {stage5.get("order_backlog", "N/A")}</div>
      <div class="stage-field"><strong>신사업:</strong> {stage5.get("new_business", "N/A")}</div>
      <div class="stage-field"><strong>12개월 촉매:</strong> {catalysts_12m_html}</div>
      <div class="stage-analysis">{stage5.get("analysis", "")}</div>
    </div>
  </div>"""

    # ── Stage 6: 밸류에이션 + 코스톨라니 달걀 ──
    valuation_html = ""
    if stage6:
        egg_pos = stage6.get("kostolany_egg_position", 0)
        try:
            egg_pos = int(egg_pos)
        except (ValueError, TypeError):
            egg_pos = 0
        egg_label = KOSTOLANY_EGG_LABELS.get(egg_pos, "")
        egg_steps = ""
        for i in range(1, 7):
            active = "active" if i == egg_pos else ""
            egg_steps += f'<div class="egg-step {active}">{i}</div>'

        valuation_html = f"""
  <div class="stage-card valuation-card">
    <div class="stage-card-title">STAGE 6 &mdash; 밸류에이션 &amp; 코스톨라니 달걀 모형</div>
    <div class="stage-field"><strong>방법론:</strong> {stage6.get("lifecycle_matched_method", "N/A")}</div>
    <div class="stage-field"><strong>적정 주가 범위:</strong> {stage6.get("fair_value_range", "N/A")}</div>
    <div class="stage-field"><strong>시장 심리:</strong> {stage6.get("market_psychology", "N/A")}</div>
    <div class="egg-section">
      <div class="stage-field"><strong>코스톨라니 달걀 위치:</strong> {egg_pos}단계 - {egg_label}</div>
      <div class="egg-scale">{egg_steps}</div>
    </div>
    <div class="stage-analysis">{stage6.get("analysis", "")}</div>
  </div>"""

    # ── Stage 7: 6대 거장 카드 ──
    master_cards = ""
    for key, info in MASTER_INFO.items():
        m = stage7.get(key, {})
        s = m.get("score", 0)
        one_liner = m.get("one_liner", "")
        analysis = m.get("analysis", "")
        bar_w = _score_bar_width(s)

        master_cards += f"""
        <div class="master-card">
          <div class="master-header" style="border-left: 4px solid {info['color']};">
            <div class="master-icon" style="background: {info['color']};">{info['icon']}</div>
            <div class="master-info">
              <div class="master-name">{info['name']}</div>
              <div class="master-philosophy">{info['philosophy']}</div>
            </div>
            <div class="master-score">
              <span class="score-num">{s}</span><span class="score-max">/10</span>
            </div>
          </div>
          <div class="score-bar-wrap">
            <div class="score-bar-fill" style="width: {bar_w}%; background: {info['color']};"></div>
          </div>
          <div class="master-title">{one_liner}</div>
          <div class="master-analysis">{analysis}</div>
        </div>"""

    # ── 리스크 & 촉매 리스트 ──
    risk_items = "".join(f'<li class="risk-item">{r}</li>' for r in risks)
    catalyst_items = "".join(f'<li class="catalyst-item">{c}</li>' for c in catalysts)

    # ── Stage 8: 매매 액션 플랜 ──
    action_html = ""
    if stage8:
        action_html = f"""
  <div class="action-plan-box">
    <div class="stage-card-title">STAGE 8 &mdash; 트레이딩 액션 플랜</div>
    <div class="action-plan-grid">
      <div class="action-item">
        <div class="action-label">매수 진입가</div>
        <div class="action-value entry">{stage8.get("entry_price", "N/A")}</div>
      </div>
      <div class="action-item">
        <div class="action-label">목표 주가</div>
        <div class="action-value target">{stage8.get("target_price", "N/A")}</div>
      </div>
      <div class="action-item">
        <div class="action-label">손절 기준</div>
        <div class="action-value stoploss">{stage8.get("stop_loss", "N/A")}</div>
      </div>
    </div>
    <div class="action-meta">
      <span><strong>포트폴리오 비중:</strong> {stage8.get("portfolio_weight", "N/A")}</span>
      <span><strong>보유 기간:</strong> {stage8.get("holding_period", "N/A")}</span>
    </div>
    <div class="stage-analysis">{stage8.get("analysis", "")}</div>
  </div>"""

    return f"""\
<div class="analysis-report">
  <div class="report-header">
    <div class="stock-identity">
      <h2 class="stock-name">{name}</h2>
      <span class="stock-code">{code}</span>
      <span class="stock-market badge {'bg-primary' if market == 'KOSPI' else 'bg-danger'}">{market}</span>
    </div>
    <div class="composite-section">
      <div class="composite-grade" style="background: {grade_color};">{grade}</div>
      <div class="composite-score-wrap">
        <div class="composite-label">Grand Master 종합 점수</div>
        <div class="composite-num">{composite}<span class="composite-max">/100</span></div>
      </div>
    </div>
  </div>
{hallucination_banner}
{identity_html}

  <div class="key-metrics">{key_metrics}</div>
{stage12_html}
{moat_html}
{stage45_html}
{valuation_html}

  <div class="summary-box">
    <h4>종합 투자 의견</h4>
    <p>{summary}</p>
  </div>

  <h4 class="section-title">6대 투자 거장 관점 분석</h4>
  <div class="master-cards">{master_cards}</div>

  <div class="risk-catalyst-grid">
    <div class="risk-section">
      <h4>주요 리스크</h4>
      <ul class="risk-list">{risk_items}</ul>
    </div>
    <div class="catalyst-section">
      <h4>상승 촉매</h4>
      <ul class="catalyst-list">{catalyst_items}</ul>
    </div>
  </div>
{action_html}

  <div class="report-footer">
    <span>Generated by {model_label} &mdash; Grand Master Protocol v2</span>
    <span>{generated_date}</span>
  </div>
</div>"""
