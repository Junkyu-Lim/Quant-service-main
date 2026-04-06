"""
AI 종목 정성 분석 보고서 생성기 (Grand Master Protocol v3).

9대 투자 거장(Warren Buffett, Aswath Damodaran, Philip Fisher,
Pat Dorsey, Peter Lynch, André Kostolany, Charlie Munger,
Howard Marks, Seth Klarman)의 핵심 철학을 기반으로
9단계(Stage 0~8) 심층 분석 프로토콜을 수행합니다.
"""

import json
import logging
import re
import sys
import os
import math
import hashlib
import time
from datetime import datetime
from pathlib import Path

import anthropic
import httpx
import numpy as np
import pandas as pd

# db.py는 루트에 있으므로 sys.path 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import db as _db

import config

log = logging.getLogger("Analyzer")
ANALYSIS_INPUT_VERSION = "stock-analysis-v7"

# ─────────────────────────────────────────
# 프롬프트 템플릿
# ─────────────────────────────────────────

SYSTEM_PROMPT = """\
당신은 한국 주식시장 Grand Master 애널리스트입니다.
9대 투자 거장의 철학을 통합한 9단계 심층 분석 프로토콜(Stage 0~8)을 수행합니다.

[환각 방지 - 최우선] 종목명만 보고 업종을 단정짓지 마세요. 불확실하면 hallucination_flag=true.

## 분석 단계 요약
- Stage 0: 핵심 사업 모델 (무엇을 팔아 돈을 버는가? 3줄, 핵심 제품명·매출비중 필수)
- Stage 1: 거시환경 & 밸류체인 (전방산업 CAGR, 경쟁우위)
- Stage 2: 수익성 해부 (P×Q×C 분석, 캐시카우 vs 성장동력 구분)
- Stage 3: 수명주기 & 해자 (도입/성장/성숙/쇠퇴, 4대 해자 데이터로 입증)
- Stage 4: 재무건전성 & 지배구조 (매출총이익률 추이, FCF 품질, 부채비율, 컨센서스 괴리, 지배구조 건전성)
- Stage 4.5: Peer 비교 → 아래 별도 규칙 참조
- Stage 5: 전망 & 모멘텀 (CAPEX, 수주잔고, 신사업, 12개월 촉매)
- Stage 6: 밸류에이션 & 코스톨라니 (수명주기 맞춤 방법론, 달걀 1~6 위치)
- Stage 7: 거장 9인 평가 (각 1줄+분석, 통합 S~F 등급)
- Stage 8: 트레이딩 액션 플랜 (진입가·목표가·손절가·비중·보유기간·매도조건)

### 코스톨라니 달걀 위치 (정량 직접 대입)
1=극도공포: RSI<30, 최저대비<10%, MA60이격<-15% | 2=비관→전환: RSI 30~45 | 3=낙관시작: RSI 45~60 | 4=극도낙관: RSI>70, 최고대비-5%내 | 5=낙관→하락: RSI 55~70 | 6=비관시작: RSI 40~55, 고점대비-15~-30%

### 수급/타이밍 신호 해석 (데이터에 해당 항목이 있을 때만 적용)
- "수급 다이버전스 경고" → Stage 6 달걀 위치를 5(낙관→하락) 이상으로 조정 검토, Stage 7 Kostolany 분석에 반영
- "과열+매도 동시 경고" → Stage 8 entry_price를 현재가 대비 10% 이상 할인으로 설정 필수
- "피크 실적 리스크" → risks 배열에 "피크 실적 리스크: 영업이익 성장 감속 중" severity=high 필수 추가
- "상승 초기 진입 기회" → Stage 6 달걀 위치 2~3(전환 초기) 검토, Stage 8 entry_price를 현재가 근처로 설정 가능, 적극적 비중 권고 검토
- "VCP 돌파 대기" → Stage 8 holding_period를 단기~중기로 설정, 촉매 발생 시 빠른 진입 전략 언급

### 거장 점수 기준 (1-10, 전체 한국 상장사 절대평가)
10=상위2%·9=탁월·7~8=우수·5~6=평균·3~4=미흡(약점 근거 필수)·1~2=부적합
[필수] 각 거장 분석에서 핵심 약점 1개 이상 명시. 긍정만 나열 금지.

### 9대 거장 핵심 관점 (키워드)
- Buffett: 해자지속성·경영진건전성(지배구조_점수·오너리스크)·S-RIM 안전마진·장기보유·주주환원
- Damodaran: 내러티브-숫자일관성·ROIC vs WACC·리스크대비보상
- Fisher: R&D혁신·이익률개선추세·장기성장·조직문화
- Dorsey: 4대해자(무형/전환비용/네트워크/원가) 강도·트렌드(확대/축소)
- Lynch: PEG·이해가능사업·10-bagger잠재력·이익↔주가연동
- Kostolany: 달걀위치·역발상·유동성수급·인내심
- Munger: 역전사고(이 투자가 실패하는 시나리오 Top3)·멘탈모델·ROIC지속성·Lollapalooza효과
- Marks: 2차사고(시장이 이미 아는가?)·사이클위치·가격vs가치괴리·컨센서스갭·영구자본손실확률
- Klarman: 촉매식별(가치 재평가 트리거)·보수적안전마진·무촉매밸류트랩경고·하방시나리오

## Stage 4.5: Peer 비교
- USER_PROMPT에 "동종업계 Peer DB 데이터" 섹션이 있으면 그 값을 peer_comparison.peers에 그대로 사용 (웹 추정 금지)
- DB 데이터 없을 때만 웹 검색으로 보완. 지표별 대상 종목 상대 순위 명시. 저평가/적정/고평가 판정. 더 매력적 대안이 있으면 솔직히 언급.

## 웹 검색 (최대 3회)
1. [필수] "{종목명} 실적 영업이익 {연도}" → 최신 실적·뉴스 → Stage 5·recent_news에 반영
2. [필수] "{종목명} 증권사 리포트 목표주가" → 컨센서스 → Stage 5·8에 반영
3. [지배구조 데이터가 있거나 의심 시] "{종목명} 지배구조 오너리스크 세습 횡령 배임 내부거래 소액주주" → 최근 3년 이내 지배구조 이슈 파악 → Stage 4 governance_assessment·risks에 반영
   - 지배구조_점수 ≤ 2이거나, 최대주주_지분율 > 60%이거나, 감사의견 ≠ "적정"이면 반드시 이 검색 수행
   - 오너 일가 사익편취, 일감 몰아주기, 유상증자 후 주가 하락 패턴, 회사 자금 유용 등을 중점 확인
검색 결과는 분석 근거에 구체적으로 인용하세요.

## 근거 인용 규칙 (중요)
- 출처 우선순위: DART/전자공시/회사 IR/실적발표 > 증권사 리포트 > 경제지/통신사 > 일반 기사
- 가능하면 아래 화이트리스트 계열 출처를 우선 사용: DART, 전자공시, 회사 IR/실적발표, 한국경제, 매일경제, 서울경제, 연합뉴스, Reuters, Bloomberg, 주요 증권사 리포트
- 같은 사실을 여러 출처가 다루면 더 상위 출처를 먼저 recent_news에 배치
- recent_news 각 항목은 날짜(date), 출처(source), 핵심 사실 1개 이상을 반드시 포함
- 핵심 사실에는 가능한 한 수치 1개 이상을 포함 (예: 매출, 영업이익, 점유율, 목표주가, 수주금액, CAPEX)
- stage5_outlook.analysis는 2~4문장으로 근거 중심 작성하고, 최소 2문장 이상에 날짜 또는 출처명을 포함
- stage5_outlook.analysis와 summary의 핵심 주장(실적 개선, 목표주가, 수주, 점유율, 주주환원, 산업 전망)은 가능하면 화이트리스트 출처 근거를 우선 사용
- 일반 기사만 근거일 경우, DART/IR/증권사/주요 경제지로 교차확인되지 않은 핵심 수치는 과장하지 말고 보수적으로 서술
- "전망", "기대", "가능성"만 반복하지 말고, 확인된 사실과 추정/의견을 구분해서 쓰세요
- 출처 없는 업계 루머, 커뮤니티 글, 출처 불명 숫자는 사용 금지

## recent_news (3~4건 우선)
웹 검색에서 투자 판단에 중요한 뉴스·공시를 선별. 각 항목: title, date, summary(1문장 우선), impact(긍정/부정/중립), source.

## 2차 사고 검증 (Howard Marks — 모든 판단에 적용)
모든 긍정적 판단에 대해 자문하세요: "시장이 이것을 이미 알고 현재 주가에 반영했는가?"
컨센서스와 동일한 의견이면, 그것이 왜 아직 미반영인지 구체적 근거를 제시해야 합니다.
긍정 요인이 이미 주가에 반영된 경우: Marks 점수 상한 5, stage8 entry_price를 현재가 대비 할인 설정.

## 역전사고 (Charlie Munger — Stage 7 필수)
"이 투자가 실패하는 시나리오 Top 3"를 반드시 작성하세요. 시나리오는 구체적(수치/사건 포함)이어야 합니다.
Stage 7 모든 거장 평균 ≥8이면, 반드시 Munger bear_case에서 과열/과신 리스크를 지적하세요.

## 촉매 식별 (Seth Klarman — Stage 5·7 연동)
저평가 종목은 반드시 12-18개월 내 가치 재평가 촉매를 식별하세요.
각 촉매에 예상시기·발생확률(high/medium/low)을 명시합니다.
촉매 식별 실패 시: Klarman 점수 상한 4, "무촉매 저평가" 경고를 summary에 포함.

## 가치함정 판별 (Stage 4 value_trap_risk 기준)
- 매출이익_동행성=0(매출↑마진↓) → divergent, value_trap_risk≥medium
- PER<8/PBR<0.7 + 지속가치_품질≤2 → "cheap for reason", value_trap_risk 상향
- 현금전환율<50% → 이익 품질 의심, stage4 analysis에 언급
- 지속가치_품질≥4 + 괴리율 양수 → "진정한 저평가", value_trap_risk=low
- [신규] PER<10 + PBR<1 + Klarman 촉매 0건 → "무촉매 저평가 = 밸류트랩 가능성", value_trap_risk 상향

## 지배구조 건전성 판별 (Stage 4 확장)
지배구조 섹션 데이터가 제공된 경우 반드시 적용하세요.
- 지배구조_점수 5: 건전 → stage4 governance_assessment="건전", 긍정 언급
- 지배구조_점수 3-4: 보통 → stage4 governance_assessment="보통", 미흡한 항목 1개 지적
- 지배구조_점수 0-2: 우려 → stage4 governance_assessment="우려", risks에 category="지배구조" severity=medium 이상 필수 추가
- 감사의견 ≠ "적정" → risks에 category="지배구조" severity=high 필수, Buffett 점수 상한 4
- 최대주주_지분율 > 70% → "과도한 지분 집중" 경고; 오너 일가 사익편취·일감 몰아주기·세습 리스크를 웹 검색으로 확인
- 최대주주_지분율 > 50% + 웹 검색에서 오너리스크 이슈 발견 → risks에 category="지배구조" severity=high, Buffett/Dorsey 점수 각 상한 5
- 최대주주_지분율 < 20% + 외국인_지분율 < 5% → "경영권 불안정" 리스크 평가
- F7_희석없음=0 (유상증자 의심) → stage4 analysis에 희석 리스크 명시, Buffett 점수 상한 6
- 외국인_지분율 >= 20% → 국제 기준 거버넌스 모니터링 존재로 긍정적 평가 가능

## 내부 일관성 (JSON 출력 전 자체 검증)
1. moat_rating=none → fair_value_range PBR/BPS 보수적 산출, 성장 프리미엄 금지
2. TTM_FCF 음수 또는 부채비율>200% → portfolio_weight 최대 2%
3. lifecycle=쇠퇴기 → Stage 6에서 성장주 PER/PSR 멀티플 금지
4. recent_news impact=부정 → risks에 반드시 반영
5. peer relative_valuation=고평가 → entry_price는 현재가 대비 할인가 필수
6. better_alternative≠null → summary에 대안 종목 언급 필수
7. 매출이익_동행성=0(매출↑마진↓) → value_trap_risk를 medium 이상으로 설정, Buffett 점수 상한 6
8. 매출이익_동행성=-1(매출↓마진↓) → value_trap_risk=high, Buffett/Dorsey 점수 각 상한 4
9. 가치함정_경고=1 → summary에 "가치함정 리스크" 반드시 언급, portfolio_weight 상한 3%
10. Stage 7 모든 거장 평균 ≥8 → Munger bear_case에서 과열/과신 리스크 필수 지적
11. Klarman catalysts 0건 + 괴리율>0 → summary에 "무촉매 저평가 경고" 필수 언급, portfolio_weight 상한 3%
12. 수출비중 높은 종목 + 매크로 불리 → Marks 점수 상한 6
13. RS등급 > 80 고모멘텀 종목 → risks에 추가: {"description": "생존자 편향 주의: 모멘텀 스크리닝은 상장폐지 종목 제외로 과거 수익률 과대평가 가능. 모멘텀 역전 시 낙폭 확대 위험.", "severity": "low"}
14. 감사의견 ≠ "적정" → Buffett 점수 상한 4, risks에 category="지배구조" severity=high 필수
15. 지배구조_점수 ≤ 2 → portfolio_weight 상한 3%, summary에 "지배구조 리스크" 필수 언급
16. 웹 검색에서 세습·횡령·배임·일감 몰아주기 이슈 확인 시 → risks에 category="지배구조" severity=high, Munger bear_case에 해당 시나리오 포함 필수

## risks 필수 규칙
단순 "경쟁 심화·금리·환율" 기재 금지 — 수치/사건 연결 필수. 재무 약점 1개 이상, severity=high 1개 이상, DART·뉴스 근거 1개 이상.

## 출력 압축 규칙
- JSON 외 텍스트 금지
- 같은 수치·출처·문장을 recent_news, stage analysis, summary에 반복 복붙하지 마세요
- 핵심 근거만 쓰고 불필요한 서론/수식어/상투 표현은 생략하세요

"""

USER_PROMPT_TEMPLATE = """\
종목: {code} {name} ({market})

## 정량 데이터
{quant_data}
{qualitative_section}
[환각 방지] 위 종목의 실제 핵심 사업을 웹 검색으로 먼저 확인하세요. 금융 데이터만 보고 업종을 추정하지 마세요.
데이터 활용: PEG·매출CAGR·ROE→Lynch | 괴리율·SRIM적정가→Buffett | F스코어→Stage4 | RSI·MA이격도→코스톨라니 | TTM·CAGR→Stage2
{length_guardrails}

반드시 아래 JSON 형식으로만 응답하세요.

```json
{{
  "business_identity": {{
    "core_business": "<핵심 사업 모델 3줄>",
    "key_products": "<주요 제품/서비스>",
    "revenue_breakdown": "<매출 구성 및 비중>",
    "industry_classification": "<산업 분류>",
    "confidence": "<high|medium|low>",
    "hallucination_flag": false
  }},
  "stage1_macro": {{
    "upstream_cagr": "<전방산업 성장률>",
    "value_chain_position": "<밸류체인 위치>",
    "competitive_advantages": "<경쟁우위>",
    "analysis": "<2-4문장>"
  }},
  "stage2_business_model": {{
    "p_times_q_analysis": "<P×Q×C 분석>",
    "cash_cow_drivers": "<캐시카우 사업부>",
    "growth_drivers": "<성장 동력>",
    "analysis": "<2-4문장>"
  }},
  "stage3_moat": {{
    "lifecycle_stage": "<도입기|성장기|성숙기|쇠퇴기>",
    "intangible_assets": {{"exists": true, "evidence": "<근거>"}},
    "switching_costs": {{"exists": false, "evidence": "<근거>"}},
    "network_effects": {{"exists": false, "evidence": "<근거>"}},
    "cost_advantage": {{"exists": false, "evidence": "<근거>"}},
    "moat_rating": "<wide|narrow|none>",
    "analysis": "<2-4문장>"
  }},
  "stage4_financials": {{
    "gross_margin_trend": "<매출총이익률 추세>",
    "revenue_margin_comovement": "<co-growth|divergent|cost-cutting>",
    "fcf_quality": "<FCF 품질 — OCF vs 순이익, FCF 방향>",
    "value_trap_risk": "<low|medium|high>",
    "debt_assessment": "<부채 구조>",
    "consensus_deviation": "<컨센서스 대비>",
    "governance_assessment": "<건전|보통|우려 — 지배구조_점수·최대주주 지분율·감사의견·오너리스크 이슈 근거 1줄>",
    "analysis": "<2-4문장 — ROIC/ROE 추세·FCF 품질·가치함정·지배구조 건전성 포함>"
  }},
  "peer_comparison": {{
    "peers": [
      {{
        "name": "<경쟁사명>",
        "code": "<종목코드 또는 null>",
        "market_cap": "<시총(억)>",
        "per": "<PER>",
        "pbr": "<PBR>",
        "roe": "<ROE(%)>",
        "operating_margin": "<영업이익률(%)>",
        "revenue_growth": "<매출성장률(%)>"
      }}
    ],
    "target_rank": {{
      "per": "<N개사 중 M위>",
      "pbr": "<N개사 중 M위>",
      "roe": "<N개사 중 M위>",
      "operating_margin": "<N개사 중 M위>",
      "revenue_growth": "<N개사 중 M위>"
    }},
    "relative_valuation": "<저평가|적정|고평가>",
    "better_alternative": "<대안 종목과 이유, 없으면 null>",
    "analysis": "<2-4문장>"
  }},
  "stage5_outlook": {{
    "capex_signals": "<CAPEX 현황>",
    "order_backlog": "<수주잔고>",
    "new_business": "<신사업>",
    "catalysts_12m": ["<촉매1>", "<촉매2>", "<촉매3>"],
    "analysis": "<2-4문장. 최소 2문장 이상은 날짜/출처/수치 포함>"
  }},
  "stage6_valuation": {{
    "lifecycle_matched_method": "<밸류에이션 방법론>",
    "fair_value_range": "<적정 주가 범위>",
    "kostolany_egg_position": <1-6>,
    "market_psychology": "<과열|중립|공포>",
    "analysis": "<2-4문장>"
  }},
  "stage7_masters": {{
    "buffett": {{"score": <1-10>, "one_liner": "<한 줄 평>", "analysis": "<2-3문장>"}},
    "damodaran": {{"score": <1-10>, "one_liner": "<한 줄 평>", "analysis": "<2-3문장>"}},
    "fisher": {{"score": <1-10>, "one_liner": "<한 줄 평>", "analysis": "<2-3문장>"}},
    "dorsey": {{"score": <1-10>, "one_liner": "<한 줄 평>", "analysis": "<2-3문장>"}},
    "lynch": {{"score": <1-10>, "one_liner": "<한 줄 평>", "analysis": "<2-3문장>"}},
    "kostolany": {{"score": <1-10>, "one_liner": "<한 줄 평>", "analysis": "<2-3문장>"}},
    "munger": {{"score": <1-10>, "one_liner": "<한 줄 평>", "bear_case_top3": ["<실패 시나리오1: 구체적 수치/사건>", "<실패 시나리오2>", "<실패 시나리오3>"], "analysis": "<2-3문장>"}},
    "marks": {{"score": <1-10>, "one_liner": "<한 줄 평>", "consensus_gap": "<시장 기대 vs 실제 괴리 — 시장이 이미 아는가?>", "analysis": "<2-3문장>"}},
    "klarman": {{"score": <1-10>, "one_liner": "<한 줄 평>", "catalysts": [{{"event": "<촉매 이벤트>", "timing": "<예상 시기>", "probability": "<high|medium|low>"}}], "analysis": "<2-3문장>"}}
  }},
  "stage8_action": {{
    "entry_price": "<진입 가격대(원)>",
    "entry_basis": "<산출 근거(SRIM/지지선/MA60 등)>",
    "target_price": "<12개월 목표주가(원)>",
    "target_basis": "<산출 근거(PER/SRIM/컨센서스 등)>",
    "stop_loss": "<손절 기준>",
    "portfolio_weight": "<권장 비중(예: 3-5%)>",
    "holding_period": "<단기3개월|중기6-12개월|장기2-3년>",
    "exit_conditions": ["<매도 조건1>", "<매도 조건2>"],
    "analysis": "<1-2문장>"
  }},
  "summary": "<4-5문장 종합 투자 의견>",
  "recent_news": [
    {{
      "title": "<제목>",
      "date": "<YYYY-MM-DD 또는 추정시기>",
      "summary": "<1문장, 가능한 한 핵심 수치 1개 이상 포함>",
      "impact": "<긍정|부정|중립>",
      "source": "<출처>"
    }}
  ],
  "risks": [
    {{
      "category": "<재무|산업|경쟁|규제|지배구조|거시경제|기술>",
      "description": "<수치/사건 포함 구체적 설명>",
      "severity": "<high|medium|low>",
      "evidence": "<정량 근거>"
    }}
  ]
}}
```
"""

DEFAULT_LENGTH_GUARDRAILS = """\

## 응답 길이 규칙
- recent_news 3건 우선, Stage 1~6 2~4문장, Stage 7 각 거장 2~3문장(Munger bear_case 3건 별도), summary 4~5문장, Stage 8 1~2문장
"""


def _build_stock_user_prompt(code: str, name: str, market: str,
                             quant_text: str, qualitative_section: str,
                             length_guardrails: str = DEFAULT_LENGTH_GUARDRAILS) -> str:
    return USER_PROMPT_TEMPLATE.format(
        code=code,
        name=name,
        market=market,
        quant_data=quant_text,
        qualitative_section=qualitative_section,
        length_guardrails=length_guardrails.rstrip(),
    )


# ─────────────────────────────────────────
# 정량 데이터 포맷팅
# ─────────────────────────────────────────

QUANT_SECTIONS = {
    "기본 정보": [
        ("종가", "int"), ("시가총액", "int"), ("섹터", "str"),
    ],
    "밸류에이션 핵심": [
        ("PER", "f2"), ("PBR", "f2"), ("PEG", "f2"),
        ("ROE(%)", "f2"), ("이익수익률(%)", "f2"), ("적정주가_SRIM", "int"), ("괴리율(%)", "f2"),
    ],
    "퀄리티/재무": [
        ("영업이익률(%)", "f2"), ("현금전환율(%)", "f1"), ("FCF수익률(%)", "f2"),
        ("F스코어", "int"), ("부채비율(%)", "f1"), ("부채상환능력", "f2"),
        ("ROIC(%)", "f2"), ("이자보상배율", "f2"),
        ("지속가치_품질", "int"), ("매출이익_동행성", "int"), ("가치함정_경고", "flag"),
    ],
    "성장/모멘텀": [
        ("매출_CAGR", "f1"), ("영업이익_CAGR", "f1"), ("순이익_CAGR", "f1"),
        ("Q_매출_YoY(%)", "f1"), ("Q_영업이익_YoY(%)", "f1"), ("Q_순이익_YoY(%)", "f1"),
        ("TTM_영업이익_YoY(%)", "f1"), ("TTM_순이익_YoY(%)", "f1"),
        ("실적가속_연속", "flag"), ("RS_등급", "f1"), ("Fwd_모멘텀_점수", "f1"),
    ],
    "배당/수급/기술": [
        ("배당수익률(%)", "f2"), ("배당성향(%)", "f2"), ("배당_경고신호", "flag"),
        ("수급강도", "f2"), ("스마트머니_승률", "f2"),
        ("외인순매수_20d", "int"), ("기관순매수_20d", "int"),
        ("52주_최고대비(%)", "f1"), ("52주_최저대비(%)", "f1"),
        ("MA60_이격도(%)", "f1"), ("RSI_14", "f1"),
        ("과열도", "f1"), ("상승조짐", "f1"), ("실적감속_경고", "flag"),
    ],
    "TTM 요약": [
        ("TTM_매출", "int"), ("TTM_영업이익", "int"), ("TTM_순이익", "int"),
        ("TTM_영업CF", "int"), ("TTM_FCF", "int"), ("자본", "int"), ("부채", "int"),
    ],
    "지배구조 건전성": [
        ("지배구조_점수", "int"), ("최대주주_지분율", "f1"), ("외국인_지분율", "f1"),
        ("감사의견", "str"), ("F7_희석없음", "int"),
    ],
}

# ─────────────────────────────────────────
# 포트폴리오 전용 경량 정량 데이터 (토큰 최적화)
# ─────────────────────────────────────────
# AI 보고서 있는 종목: 핵심 밸류에이션/퀄리티만 (AI가 이미 30+개 메트릭 분석 완료)
_PORTFOLIO_QUANT_WITH_AI = [
    ("PER", "f2"), ("PBR", "f2"), ("ROE(%)", "f2"),
    ("영업이익률(%)", "f2"), ("F스코어", "int"), ("부채비율(%)", "f1"),
]
# AI 보고서 없는 종목: 주요 밸류에이션 + 성장 + 퀄리티 (12개)
_PORTFOLIO_QUANT_WITHOUT_AI = [
    ("PER", "f2"), ("PBR", "f2"), ("ROE(%)", "f2"),
    ("영업이익률(%)", "f2"), ("F스코어", "int"), ("부채비율(%)", "f1"),
    ("매출_CAGR", "f1"), ("영업이익_CAGR", "f1"),
    ("적정주가_SRIM", "int"), ("괴리율(%)", "f2"),
    ("배당수익률(%)", "f2"), ("RS_등급", "f1"),
]
# 관심종목: 편입 스크리닝 수준 (8개)
_PORTFOLIO_QUANT_WATCHLIST = [
    ("PER", "f2"), ("PBR", "f2"), ("ROE(%)", "f2"),
    ("F스코어", "int"), ("매출_CAGR", "f1"), ("영업이익_CAGR", "f1"),
    ("배당수익률(%)", "f2"), ("RS_등급", "f1"),
]


def format_portfolio_quant_compact(stock: dict, has_ai_report: bool,
                                   is_watchlist: bool = False) -> str:
    """포트폴리오 분석용 경량 정량 데이터 포맷팅.

    개별 분석용 format_quant_data() 대비 토큰 ~80% 절감.
    AI 보고서가 있으면 핵심 지표만, 없으면 확장 지표 포함.
    """
    if is_watchlist:
        metrics = _PORTFOLIO_QUANT_WATCHLIST
    elif has_ai_report:
        metrics = _PORTFOLIO_QUANT_WITH_AI
    else:
        metrics = _PORTFOLIO_QUANT_WITHOUT_AI

    parts = []
    for col, fmt_type in metrics:
        val = stock.get(col)
        parts.append(f"{col}: {_fmt_val(val, fmt_type)}")
    lines = ["- " + ", ".join(parts[i:i+3]) for i in range(0, len(parts), 3)]

    # Forward 컨센서스 (있는 경우만)
    fwd = _format_forward_snapshot(stock)
    if fwd:
        lines.append(fwd)

    # 타이밍 신호 (경고/기회 있는 경우만, 조건부 출력)
    if not is_watchlist:
        timing = _format_timing_signals(stock)
        if timing:
            lines.append(timing)

    # AI 보고서 없는 종목: 분기실적 추가
    if not has_ai_report and not is_watchlist:
        quarterly = _format_quarterly_snapshot(
            str(stock.get("종목코드", "")).zfill(6)
        )
        if quarterly:
            lines.append(quarterly)

    return "\n".join(lines)


def _fmt_val(v, fmt_type: str) -> str:
    if v is None:
        return "N/A"
    try:
        if fmt_type == "str":
            s = str(v).strip()
            return s if s else "N/A"
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


def _strategy_tags(stock: dict) -> str:
    tags = []
    score_tags = [
        ("주도주_점수", "leaders"),
        ("우량가치_점수", "quality_value"),
        ("고성장_점수", "growth_mom"),
        ("현금배당_점수", "cash_div"),
        ("턴어라운드_점수", "turnaround"),
    ]
    for key, label in score_tags:
        try:
            if float(stock.get(key, 0) or 0) >= 80:
                tags.append(label)
        except (TypeError, ValueError):
            continue
    if stock.get("전략수"):
        try:
            tags.append(f"multi_strategy={int(float(stock.get('전략수') or 0))}")
        except (TypeError, ValueError):
            pass
    if stock.get("컨센서스_커버리지"):
        tags.append("forward_covered")
    return ", ".join(tags) if tags else "없음"


def _quarter_label(date_str: str) -> str:
    s = str(date_str).replace("-", "")[:6]
    try:
        year = int(s[:4])
        month = int(s[4:6])
        quarter = (month - 1) // 3 + 1
        return f"{year}Q{quarter}"
    except (TypeError, ValueError):
        return str(date_str)


def _format_quarterly_snapshot(code: str) -> str:
    df = _db.load_stock_financials(code, period="quarter")
    if df.empty:
        return ""
    df = df.copy()
    df["label"] = df["기준일"].astype(str).apply(_quarter_label)
    labels = list(df["label"].drop_duplicates())[-4:]
    if not labels:
        return ""
    lines = ["\n### 최근 4분기 실적"]
    lines.append("- 분기: " + " | ".join(labels))
    for acc in ("매출액", "영업이익", "당기순이익"):
        vals = []
        for label in labels:
            row = df[(df["label"] == label) & (df["계정"] == acc)]["값"]
            vals.append(_fmt_val(row.iloc[0], "int") if not row.empty else "N/A")
        lines.append(f"- {acc}: " + " | ".join(vals))
    return "\n".join(lines)


def _format_forward_snapshot(stock: dict) -> str:
    fields = ["Fwd_PER", "Fwd_ROE(%)", "Fwd_영업이익_성장률(%)", "Fwd_2yr_영업이익_성장(%)"]
    if not any(stock.get(k) is not None for k in fields):
        return ""
    return (
        "\n### Forward 컨센서스 요약\n"
        f"- Fwd_PER {_fmt_val(stock.get('Fwd_PER'), 'f2')}, "
        f"Fwd_ROE {_fmt_val(stock.get('Fwd_ROE(%)'), 'f1')}%, "
        f"Fwd_OP성장 {_fmt_val(stock.get('Fwd_영업이익_성장률(%)'), 'f1')}%, "
        f"2Y_OP성장 {_fmt_val(stock.get('Fwd_2yr_영업이익_성장(%)'), 'f1')}%"
    )


def _format_sector_relative_snapshot(stock: dict) -> str:
    sector = stock.get("섹터")
    code = str(stock.get("종목코드", "")).zfill(6)
    if not sector:
        return ""
    try:
        with _db.get_conn() as conn:
            row = conn.execute(
                """SELECT
                       median(PER),
                       median(PBR),
                       median("ROE(%)"),
                       median("영업이익률(%)")
                   FROM dashboard_result
                   WHERE 섹터 = ? AND 종목코드 != ?""",
                [sector, code],
            ).fetchone()
    except Exception:
        return ""
    if not row:
        return ""
    med_per, med_pbr, med_roe, med_opm = row
    comps_out = []
    comps = [
        ("PER", stock.get("PER"), med_per, "낮음"),
        ("PBR", stock.get("PBR"), med_pbr, "낮음"),
        ("ROE(%)", stock.get("ROE(%)"), med_roe, "높음"),
        ("영업이익률(%)", stock.get("영업이익률(%)"), med_opm, "높음"),
    ]
    for label, value, median, good_dir in comps:
        if value is None or median is None:
            continue
        try:
            value_f = float(value)
            median_f = float(median)
        except (TypeError, ValueError):
            continue
        delta = value_f - median_f
        if abs(delta) < 1e-9:
            pos = "유사"
        elif (delta < 0 and good_dir == "낮음") or (delta > 0 and good_dir == "높음"):
            pos = "우위"
        else:
            pos = "열위"
        comps_out.append(f"{label} {pos}({ _fmt_val(value_f, 'f2') } vs { _fmt_val(median_f, 'f2') })")
    if not comps_out:
        return ""
    return "\n### 섹터 상대 위치\n- " + ", ".join(comps_out)


def _format_allocation_snapshot(stock: dict) -> str:
    if not any(stock.get(k) is not None for k in ("배당성향(%)", "배당_경고신호", "이자보상배율", "CAPEX비율(%)")):
        return ""
    return (
        "\n### 자본배분/리스크 메모\n"
        f"- 배당성향 {_fmt_val(stock.get('배당성향(%)'), 'f1')}%, "
        f"배당경고 {_fmt_val(stock.get('배당_경고신호'), 'flag')}, "
        f"이자보상배율 {_fmt_val(stock.get('이자보상배율'), 'f2')}, "
        f"CAPEX비율 {_fmt_val(stock.get('CAPEX비율(%)'), 'f1')}%"
    )


def _format_timing_signals(stock: dict) -> str:
    """수급/과열/상승조짐 타이밍 신호 텍스트 생성.

    경고 신호(부정): 수급 다이버전스, 과열+매도, 피크 실적
    기회 신호(긍정): 상승 초기 진입 기회, VCP 돌파 대기
    """
    supply = stock.get("수급강도")
    fscore = stock.get("F스코어")
    overheat = stock.get("과열도")
    breakout = stock.get("상승조짐")
    decel = stock.get("실적감속_경고")
    vcp = stock.get("VCP_신호")
    foreign_20d = stock.get("외인순매수_20d")
    inst_20d = stock.get("기관순매수_20d")

    def _safe_float(v):
        try:
            return float(v) if v is not None else None
        except (TypeError, ValueError):
            return None

    supply_f = _safe_float(supply)
    fscore_f = _safe_float(fscore)
    overheat_f = _safe_float(overheat)
    breakout_f = _safe_float(breakout)
    decel_i = int(_safe_float(decel) or 0)
    vcp_i = int(_safe_float(vcp) or 0)

    warnings = []
    opportunities = []

    # ── 경고 신호 ──
    if supply_f is not None and supply_f < 0 and fscore_f is not None and fscore_f >= 6:
        warnings.append("수급 다이버전스 경고: 재무우량(F스코어 6+)에도 외국인+기관 순매도 중")

    if overheat_f is not None and overheat_f >= 60 and supply_f is not None and supply_f < 0:
        warnings.append(f"과열+매도 동시 경고: 과열도 {overheat_f:.0f}/100, 수급강도 음수")

    if decel_i == 1 and overheat_f is not None and overheat_f >= 50:
        warnings.append("피크 실적 리스크: 영업이익 YoY 양수이지만 3분기 연속 감속 중")

    # ── 기회 신호 ──
    if breakout_f is not None and breakout_f >= 70 and overheat_f is not None and overheat_f <= 30:
        opportunities.append(
            f"상승 초기 진입 기회: 상승조짐 {breakout_f:.0f}/100, 과열도 {overheat_f:.0f}/100 — "
            "펀더멘털 우수 + 비과열 + 수급 유입 중"
        )

    if breakout_f is not None and breakout_f >= 50 and vcp_i == 1:
        opportunities.append(
            "VCP 돌파 대기: 가격/거래량 축소 후 스마트머니 매집 중 — 촉매 발생 시 빠른 진입 검토"
        )

    if not warnings and not opportunities:
        return ""

    lines = ["\n### 수급/타이밍 신호"]
    for w in warnings:
        lines.append(f"- ⚠ {w}")
    for o in opportunities:
        lines.append(f"- ★ {o}")

    # 원시 수급 데이터
    parts = []
    if foreign_20d is not None:
        try:
            parts.append(f"외인 20일 순매수 {int(float(foreign_20d)):,}원")
        except (TypeError, ValueError):
            pass
    if inst_20d is not None:
        try:
            parts.append(f"기관 20일 순매수 {int(float(inst_20d)):,}원")
        except (TypeError, ValueError):
            pass
    if parts:
        lines.append(f"- ({', '.join(parts)})")

    return "\n".join(lines)


def format_quant_data(stock: dict) -> str:
    """종목 데이터를 분석용 텍스트로 포맷팅."""
    lines = [
        "### 분석 메모",
        f"- 전략 태그: {_strategy_tags(stock)}",
    ]
    for section, metrics in QUANT_SECTIONS.items():
        lines.append(f"\n### {section}")
        for col, fmt_type in metrics:
            val = stock.get(col)
            lines.append(f"- {col}: {_fmt_val(val, fmt_type)}")
    quarterly = _format_quarterly_snapshot(str(stock.get("종목코드", "")).zfill(6))
    if quarterly:
        lines.append(quarterly)
    forward = _format_forward_snapshot(stock)
    if forward:
        lines.append(forward)
    sector_relative = _format_sector_relative_snapshot(stock)
    if sector_relative:
        lines.append(sector_relative)
    allocation = _format_allocation_snapshot(stock)
    if allocation:
        lines.append(allocation)
    timing = _format_timing_signals(stock)
    if timing:
        lines.append(timing)
    return "\n".join(lines)


def _parse_json_response(raw_text: str) -> dict:
    """AI 응답에서 JSON을 파싱합니다. ```json 블록 자동 제거 + 잘린 JSON 복원."""
    json_str = raw_text
    if "```json" in json_str:
        json_str = json_str.split("```json", 1)[1]
    if "```" in json_str:
        json_str = json_str.split("```", 1)[0]
    json_str = json_str.strip()

    try:
        return json.loads(json_str)
    except json.JSONDecodeError:
        # 잘린 JSON 복원 시도: 닫히지 않은 괄호를 닫아본다
        repaired = _try_repair_json(json_str)
        return json.loads(repaired)


def _try_repair_json(s: str) -> str:
    """잘린 JSON 문자열을 복원 시도 (닫히지 않은 브래킷/따옴표 처리)."""
    import re

    # 1) 열린 문자열 닫기
    in_string = False
    escaped = False
    for ch in s:
        if escaped:
            escaped = False
            continue
        if ch == '\\':
            escaped = True
            continue
        if ch == '"':
            in_string = not in_string
    if in_string:
        s += '"'

    # 2) 후행 콤마 제거 (}, ] 앞의 콤마 및 문자열 끝의 콤마)
    s = re.sub(r',\s*([}\]])', r'\1', s)
    s = s.rstrip().rstrip(',')

    # 3) 불완전한 키-값 쌍 처리: 끝이 "key": 로 끝나면 null 추가
    s = re.sub(r':\s*$', ': null', s)
    # "key" 만 있고 : 가 없는 경우 제거
    s = re.sub(r',\s*"[^"]*"\s*$', '', s)

    # 4) 닫히지 않은 괄호 닫기
    stack = []
    in_str = False
    esc = False
    for ch in s:
        if esc:
            esc = False
            continue
        if ch == '\\':
            esc = True
            continue
        if ch == '"':
            in_str = not in_str
            continue
        if in_str:
            continue
        if ch in ('{', '['):
            stack.append(ch)
        elif ch == '}' and stack and stack[-1] == '{':
            stack.pop()
        elif ch == ']' and stack and stack[-1] == '[':
            stack.pop()

    for bracket in reversed(stack):
        s += ']' if bracket == '[' else '}'

    return s


def _call_with_retry(client, *, max_retries: int = 5, use_beta: bool = False, **kwargs):
    """Anthropic API 호출 + 과부하(529)/서버오류(5xx) 시 자동 재시도.
    use_beta=True이면 client.beta.messages.create 사용 (Files API 문서 블록 지원).
    """
    api_create = client.beta.messages.create if use_beta else client.messages.create
    for attempt in range(1, max_retries + 1):
        try:
            return api_create(**kwargs)
        except anthropic.APIStatusError as e:
            if e.status_code in (429, 529, 500, 502, 503) and attempt < max_retries:
                wait = min(2 ** attempt * 3, 60)  # 6s, 12s, 24s, 48s
                log.warning("API 오류 %d (attempt %d/%d), %ds 후 재시도: %s",
                            e.status_code, attempt, max_retries, wait, str(e)[:100])
                time.sleep(wait)
            else:
                raise


def build_stock_analysis_input_hash(stock: dict, data_version: str = "") -> str:
    """분석 입력이 실제로 바뀌었는지 판별하는 해시."""
    payload = {
        "version": ANALYSIS_INPUT_VERSION,
        "data_version": data_version,
        "stock": {k: stock.get(k) for k in sorted(stock.keys())},
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


MASTER_SCORE_WEIGHTS = {
    "buffett": 15,
    "damodaran": 12,
    "fisher": 12,
    "dorsey": 12,
    "lynch": 12,
    "kostolany": 10,
    "munger": 12,
    "marks": 8,
    "klarman": 7,
}


def _normalize_portfolio_weight(weight_text: str, stock: dict) -> str:
    if not weight_text:
        return weight_text
    nums = [float(x) for x in re.findall(r"\d+(?:\.\d+)?", str(weight_text))]
    if not nums:
        return weight_text

    cap = None
    sector = str(stock.get("섹터", "") or "")
    name = str(stock.get("종목명", "") or "")
    is_financial = any(keyword in f"{sector} {name}" for keyword in ["은행", "금융", "증권", "보험", "카드", "지주"])
    debt = float(stock.get("부채비율(%)", 0) or 0)
    ttm_fcf = float(stock.get("TTM_FCF", 0) or 0)
    if not is_financial:
        if debt >= 300:
            cap = 1.5
        elif ttm_fcf < 0 or debt > 200:
            cap = 2.0

    if cap is None:
        return weight_text

    clamped = [min(n, cap) for n in nums]
    if len(clamped) >= 2:
        if abs(clamped[0] - clamped[1]) < 1e-9:
            return f"{clamped[0]:g}%"
        return f"{clamped[0]:g}-{clamped[1]:g}%"
    return f"{clamped[0]:g}%"


def _compute_composite_fields(stock: dict, scores: dict) -> tuple[int | None, str | None]:
    masters = scores.get("stage7_masters", {}) or {}
    if not masters:
        return None, None

    # 기존 6인 보고서 호환: 없는 거장은 가중치에서 제외
    active_weights = {}
    for key, weight in MASTER_SCORE_WEIGHTS.items():
        entry = masters.get(key) or {}
        if "score" in entry:
            try:
                float(entry["score"])
                active_weights[key] = weight
            except (TypeError, ValueError):
                pass
    # 최소 기존 6인은 있어야 함
    if len(active_weights) < 6:
        return None, None

    total = 0.0
    for key, weight in active_weights.items():
        total += float((masters.get(key) or {}).get("score") or 0) * weight

    composite = total / sum(active_weights.values()) * 10
    confidence = ((scores.get("business_identity") or {}).get("confidence") or "").lower()
    confidence_adj = {"high": 5, "medium": 0, "low": -10}.get(confidence, 0)
    composite += confidence_adj

    def _recalc():
        t = 0.0
        for k, w in active_weights.items():
            t += float((masters.get(k) or {}).get("score") or 0) * w
        return t / sum(active_weights.values()) * 10 + confidence_adj

    try:
        if float(stock.get("괴리율(%)", 0) or 0) <= -30:
            buffett = masters.get("buffett") or {}
            if "score" in buffett:
                buffett["score"] = min(float(buffett.get("score") or 0), 4)
                masters["buffett"] = buffett
                composite = _recalc()
    except (TypeError, ValueError):
        pass

    if float(stock.get("F스코어", 0) or 0) <= 3:
        composite = min(composite, 45)

    # 무촉매 밸류트랩 캡핑: 저평가이지만 촉매 없음
    klarman_data = masters.get("klarman") or {}
    catalysts = klarman_data.get("catalysts") or []
    per_val = float(stock.get("PER", 0) or 0)
    pbr_val = float(stock.get("PBR", 0) or 0)
    if len(catalysts) == 0 and 0 < per_val < 10 and 0 < pbr_val < 1:
        composite = min(composite, 50)

    composite = round(max(1, min(100, composite)))
    if composite >= 85:
        grade = "S"
    elif composite >= 75:
        grade = "A"
    elif composite >= 65:
        grade = "B+"
    elif composite >= 55:
        grade = "B"
    elif composite >= 45:
        grade = "C+"
    elif composite >= 35:
        grade = "C"
    elif composite >= 20:
        grade = "D"
    else:
        grade = "F"
    return composite, grade


def _news_source_priority(source: str) -> tuple[int, str]:
    trusted_keywords_tier1 = (
        "dart", "전자공시", "사업보고서", "분기보고서", "반기보고서",
        "실적발표", "ir", "ir자료", "company release", "press release",
    )
    trusted_keywords_tier2 = (
        "증권", "리포트", "research", "애널리스트", "투자증권",
    )
    trusted_keywords_tier3 = (
        "연합", "reuters", "블룸버그", "bloomberg", "서울경제",
        "매일경제", "한국경제", "머니투데이", "edaily", "이데일리",
    )
    s = str(source or "").lower()
    if any(k in s for k in trusted_keywords_tier1):
        return (0, s)
    if any(k in s for k in trusted_keywords_tier2):
        return (1, s)
    if any(k in s for k in trusted_keywords_tier3):
        return (2, s)
    return (3, s)


def _normalize_recent_news_items(items: list[dict]) -> list[dict]:
    normalized = []
    for item in items[:5]:
        if not isinstance(item, dict):
            continue
        title = str(item.get("title") or "").strip()
        summary = str(item.get("summary") or "").strip()
        if not title and not summary:
            continue
        date = str(item.get("date") or "").strip() or "날짜 미상"
        source = str(item.get("source") or "").strip() or "출처 미상"
        impact = str(item.get("impact") or "중립").strip() or "중립"
        if len(summary) > 220:
            summary = summary[:217].rstrip() + "..."
        normalized.append({
            "title": title or "제목 미상",
            "date": date,
            "summary": summary or "요약 없음",
            "impact": impact,
            "source": source,
        })
    normalized.sort(key=lambda x: str(x.get("date", "")), reverse=True)
    normalized.sort(key=lambda x: _news_source_priority(x.get("source", "")))

    trusted = [item for item in normalized if _news_source_priority(item.get("source", ""))[0] <= 2]
    fallback = [item for item in normalized if _news_source_priority(item.get("source", ""))[0] > 2]
    if len(trusted) >= 3:
        return trusted[:5]
    return (trusted + fallback)[:5]


def _normalize_stage5_analysis(stage5: dict, recent_news: list[dict]) -> dict:
    if not stage5:
        return stage5
    analysis = str(stage5.get("analysis") or "").strip()
    if analysis:
        has_date_or_source = any(token in analysis for token in ("202", "증권", "DART", "공시", "뉴스", "리포트", "발표"))
        if not has_date_or_source and recent_news:
            trusted_news = [n for n in recent_news if _news_source_priority(n.get("source", ""))[0] <= 2]
            top = trusted_news[0] if trusted_news else recent_news[0]
            source = top.get("source", "출처 미상")
            date = top.get("date", "날짜 미상")
            stage5["analysis"] = f"{analysis} 참고 근거: {date} {source}."
    return stage5


def _normalize_summary_text(summary: str, recent_news: list[dict]) -> str:
    summary = str(summary or "").strip()
    if not summary:
        return summary
    has_source_cue = any(token in summary for token in ("202", "증권", "DART", "공시", "IR", "발표", "리포트"))
    if has_source_cue or not recent_news:
        return summary
    trusted_news = [n for n in recent_news if _news_source_priority(n.get("source", ""))[0] <= 2]
    top = trusted_news[0] if trusted_news else recent_news[0]
    source = top.get("source", "출처 미상")
    date = top.get("date", "날짜 미상")
    fact = top.get("summary", "")
    if fact:
        fact = fact[:120].rstrip()
        return f"{summary} 핵심 근거: {date} {source} - {fact}"
    return f"{summary} 핵심 근거: {date} {source}."


def _postprocess_scores(stock: dict, scores: dict) -> dict:
    scores = scores or {}
    stage5 = scores.get("stage5_outlook") or {}
    catalysts_12m = stage5.get("catalysts_12m") or []
    if catalysts_12m and not scores.get("catalysts"):
        scores["catalysts"] = catalysts_12m[:3]
    recent_news = _normalize_recent_news_items(scores.get("recent_news") or [])
    scores["recent_news"] = recent_news
    scores["stage5_outlook"] = _normalize_stage5_analysis(stage5, recent_news)
    scores["summary"] = _normalize_summary_text(scores.get("summary", ""), recent_news)

    stage8 = scores.get("stage8_action") or {}
    if stage8:
        stage8["portfolio_weight"] = _normalize_portfolio_weight(
            stage8.get("portfolio_weight", ""), stock
        )
        scores["stage8_action"] = stage8

    composite, grade = _compute_composite_fields(stock, scores)
    if composite is not None:
        scores["composite_score"] = composite
    if grade is not None:
        scores["investment_grade"] = grade
    return scores


# ─────────────────────────────────────────
# Peer 비교: DB 조회 헬퍼
# ─────────────────────────────────────────

PEER_IDENTIFY_PROMPT = """\
아래 종목의 동종업계 경쟁사를 식별하세요.

종목코드: {code}
종목명: {name}
산업/섹터: {sector}

DB에 보유한 동종업계 후보 종목 목록 (시가총액 순):
{candidates}

위 후보 중에서 "{name}"의 실제 동종업계 경쟁사 3~5개를 선정하세요.
- 같은 사업 모델/서비스를 직접 경쟁하는 기업 우선
- 후보에 없으면 웹 검색으로 추가 식별 가능하나, 후보 내 선택을 우선
- 반드시 아래 JSON 형식으로만 응답 (다른 텍스트 없음)

```json
{{"peers": ["종목코드1", "종목코드2", "종목코드3"]}}
```
"""


def _fetch_sector_candidates(code: str, sector: str, limit: int = 15) -> list[dict]:
    """DB에서 같은 섹터 종목을 시총 순으로 조회."""
    if not sector:
        return []
    try:
        with _db.get_conn() as conn:
            rows = conn.execute(
                """SELECT 종목코드, 종목명, PER, PBR, "ROE(%)", "영업이익률(%)", 시가총액,
                          COALESCE("Q_매출_YoY(%)", "TTM_매출_YoY(%)", "매출_CAGR")
                   FROM dashboard_result
                   WHERE 섹터=? AND 종목코드 != ?
                   ORDER BY 시가총액 DESC
                   LIMIT ?""",
                [sector, code, limit],
            ).fetchall()
        return [
            {"종목코드": r[0], "종목명": r[1], "PER": r[2], "PBR": r[3],
             "ROE(%)": r[4], "영업이익률(%)": r[5], "시가총액": r[6], "매출_CAGR": r[7]}
            for r in rows
        ]
    except Exception as e:
        log.warning("섹터 후보 조회 실패: %s", e)
        return []


def _fetch_peer_data(codes: list[str]) -> list[dict]:
    """DB에서 특정 종목코드 목록의 지표를 조회."""
    if not codes:
        return []
    try:
        placeholders = ", ".join("?" * len(codes))
        with _db.get_conn() as conn:
            rows = conn.execute(
                f"""SELECT 종목코드, 종목명, PER, PBR, "ROE(%)", "영업이익률(%)", 시가총액,
                           COALESCE("Q_매출_YoY(%)", "TTM_매출_YoY(%)", "매출_CAGR")
                    FROM dashboard_result
                    WHERE 종목코드 IN ({placeholders})""",
                codes,
            ).fetchall()
        result = []
        for r in rows:
            mktcap_eok = round(r[6] / 1e8) if r[6] else None
            result.append({
                "종목코드": r[0],
                "종목명": r[1],
                "PER": round(r[2], 2) if r[2] else None,
                "PBR": round(r[3], 2) if r[3] else None,
                "ROE(%)": round(r[4], 1) if r[4] else None,
                "영업이익률(%)": round(r[5], 1) if r[5] else None,
                "시가총액(억)": mktcap_eok,
                "매출성장률(%)": round(r[7], 1) if r[7] else None,
            })
        return result
    except Exception as e:
        log.warning("Peer 데이터 조회 실패: %s", e)
        return []


def _identify_peers(stock: dict, code: str, candidates: list[dict]) -> list[str]:
    """DB 후보군에서 정량 유사도로 경쟁사 종목코드를 선택."""
    if not candidates:
        return []

    def _safe_float(v):
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    target_cap = _safe_float(stock.get("시가총액"))
    target_per = _safe_float(stock.get("PER"))
    target_pbr = _safe_float(stock.get("PBR"))
    target_roe = _safe_float(stock.get("ROE(%)"))
    target_opm = _safe_float(stock.get("영업이익률(%)"))
    target_growth = _safe_float(stock.get("Q_매출_YoY(%)") or stock.get("TTM_매출_YoY(%)") or stock.get("매출_CAGR"))

    ranked = []
    for cand in candidates:
        score = 0.0
        if target_cap and cand.get("시가총액"):
            score += abs(math.log10(max(target_cap, 1)) - math.log10(max(float(cand["시가총액"]), 1))) * 2.0
        for target, key, weight in (
            (target_per, "PER", 1.2),
            (target_pbr, "PBR", 1.0),
            (target_roe, "ROE(%)", 1.0),
            (target_opm, "영업이익률(%)", 1.0),
            (target_growth, "매출_CAGR", 0.8),
        ):
            cand_val = _safe_float(cand.get(key))
            if target is None or cand_val is None:
                score += weight * 1.5
            else:
                denom = max(abs(target), 1.0)
                score += abs(target - cand_val) / denom * weight
        ranked.append((score, str(cand["종목코드"]).zfill(6)))

    ranked.sort(key=lambda x: x[0])
    return [code for _, code in ranked[:5]]


# ─────────────────────────────────────────
# Claude API 호출
# ─────────────────────────────────────────

def generate_report(stock: dict, mode: str = "claude") -> dict:
    """
    종목 분석 보고서를 생성합니다 (Claude API).

    Args:
        stock: dashboard_result의 한 종목 데이터 (dict)
        mode: 사용하지 않음 (호환성 유지)

    Returns:
        {
            "scores": { ... },
            "report_html": "...",
            "model": "...",
            "generated_date": "...",
            "mode": "claude",
        }
    """
    if not config.ANTHROPIC_API_KEY:
        raise ValueError("ANTHROPIC_API_KEY 환경변수가 설정되지 않았습니다.")

    code = str(stock.get("종목코드", "")).zfill(6)
    name = stock.get("종목명", "Unknown")
    market = stock.get("시장구분", "")
    sector = stock.get("섹터", "") or ""

    quant_text = format_quant_data(stock)

    client = anthropic.Anthropic(
        api_key=config.ANTHROPIC_API_KEY,
        timeout=httpx.Timeout(config.ANALYSIS_TIMEOUT_SEC, connect=30.0),
    )

    # ── 1차 호출: DB 섹터 후보 → AI로 경쟁사 코드 식별 → DB에서 정확한 지표 조회 ──
    peer_db_section = ""
    candidates = _fetch_sector_candidates(code, sector, limit=15)
    if candidates:
        log.info("Peer DB 선정 시작 (%s %s, 섹터=%s, 후보=%d개)", code, name, sector, len(candidates))
        peer_codes = _identify_peers(stock, code, candidates)
        if peer_codes:
            peer_data = _fetch_peer_data(peer_codes)
            if peer_data:
                lines = ["## 동종업계 Peer DB 데이터 (우리 시스템 실측값, 웹 추정 금지)"]
                lines.append("아래 데이터는 우리 DB의 실측값입니다. peer_comparison.peers 작성 시 이 값을 그대로 사용하세요.")
                lines.append("")
                for p in peer_data:
                    lines.append(
                        f"- {p['종목코드']} {p['종목명']}: "
                        f"시총={p['시가총액(억)']}억, PER={p['PER']}, PBR={p['PBR']}, "
                        f"ROE={p['ROE(%)']}%, 영업이익률={p['영업이익률(%)']}%, 매출성장률={p['매출성장률(%)']}%"
                    )
                peer_db_section = "\n".join(lines) + "\n"
                log.info("Peer DB 데이터 준비 완료 (%d개 종목)", len(peer_data))
        else:
            log.info("Peer DB 선정 결과 없음 (%s %s)", code, name)

    user_prompt = _build_stock_user_prompt(
        code=code,
        name=name,
        market=market,
        quant_text=quant_text,
        qualitative_section=peer_db_section,
    )

    # --- 메시지 content 구성 ---
    user_content: list = [{"type": "text", "text": user_prompt}]

    # --- Prompt caching: 시스템 프롬프트 캐시 적용 ---
    system_with_cache = [{"type": "text", "text": SYSTEM_PROMPT, "cache_control": {"type": "ephemeral"}}]

    web_search_max_uses = max(1, min(3, int(config.WEB_SEARCH_MAX_USES)))
    log.info(
        "종목 AI 분석 시작 (%s %s, model=%s, timeout=%.0fs, prompt_chars=%d, web_search max_uses=%d)",
        code, name, config.ANALYSIS_MODEL, config.ANALYSIS_TIMEOUT_SEC,
        len(SYSTEM_PROMPT) + len(user_prompt), web_search_max_uses,
    )

    started_at = time.perf_counter()
    message = _call_with_retry(
        client,
        use_beta=False,
        model=config.ANALYSIS_MODEL,
        max_tokens=config.ANALYSIS_MAX_TOKENS,
        temperature=0.2,
        system=system_with_cache,
        messages=[{"role": "user", "content": user_content}],
        tools=[{"type": "web_search_20250305", "name": "web_search", "max_uses": web_search_max_uses}],
    )

    # --- 토큰 사용량 로깅 (prompt cache hit/miss 포함) ---
    if hasattr(message, "usage") and message.usage:
        u = message.usage
        cache_read = getattr(u, "cache_read_input_tokens", 0) or 0
        cache_write = getattr(u, "cache_creation_input_tokens", 0) or 0
        log.info("토큰 사용 (%s %s): input=%d output=%d cache_read=%d cache_write=%d",
                 code, name, u.input_tokens, u.output_tokens, cache_read, cache_write)

    block_types = [getattr(b, "type", "?") for b in message.content]
    elapsed = time.perf_counter() - started_at
    log.info("종목 AI 분석 완료 (%s %s, stop_reason=%s, blocks=%s, elapsed=%.1fs)",
             code, name, message.stop_reason, block_types, elapsed)

    # content 배열에서 text 블록만 추출 (web_search_tool_result 등 다른 블록 무시)
    raw_text = ""
    for block in message.content:
        if hasattr(block, "type") and block.type == "text":
            raw_text += block.text
    raw_text = raw_text.strip()

    if not raw_text:
        log.error("종목 분석: text 블록 없음 (%s %s, stop_reason=%s, blocks=%s)",
                  code, name, message.stop_reason, block_types)
        return {
            "scores": {},
            "report_html": "<p>오류: 모델 응답에 텍스트가 없습니다. 잠시 후 다시 시도해주세요.</p>",
            "error": f"빈 응답 (stop_reason={message.stop_reason})",
            "model": config.ANALYSIS_MODEL,
            "generated_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "mode": "claude",
        }

    truncated_by_tokens = (message.stop_reason == "max_tokens")
    if truncated_by_tokens:
        log.warning("종목 분석 max_tokens 도달 (%s %s, len=%d) → JSON 불완전 가능성", code, name, len(raw_text))

    try:
        scores = _parse_json_response(raw_text)
    except json.JSONDecodeError as e:
        log.error("Claude JSON 파싱 실패 (%s %s): %s", code, name, str(e)[:100])
        log.debug("시도한 JSON (첫 500자): %s", raw_text[:500])
        log.debug("시도한 JSON (끝 200자): %s", raw_text[-200:])
        return {
            "scores": {},
            "report_html": "<p>오류: JSON 파싱 실패</p>",
            "error": str(e),
            "model": config.ANALYSIS_MODEL,
            "generated_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "mode": "claude",
        }

    scores = _postprocess_scores(stock, scores)

    model_label = f"Claude ({config.ANALYSIS_MODEL})"
    generated_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    report_html = render_html(code, name, market, stock, scores,
                              generated_date, model_label=model_label,
                              truncated=truncated_by_tokens)

    return {
        "scores": scores,
        "report_html": report_html,
        "model": config.ANALYSIS_MODEL,
        "generated_date": generated_date,
        "mode": "claude",
    }


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
    "munger": {
        "name": "Charlie Munger",
        "icon": "CM",
        "color": "#2c3e50",
        "philosophy": "역전사고 & 멘탈모델",
    },
    "marks": {
        "name": "Howard Marks",
        "icon": "HM",
        "color": "#8e44ad",
        "philosophy": "2차 사고 & 사이클",
    },
    "klarman": {
        "name": "Seth Klarman",
        "icon": "SK",
        "color": "#d35400",
        "philosophy": "촉매 & 안전마진",
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
                model_label: str = None, truncated: bool = False) -> str:
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
    peer_comparison = scores.get("peer_comparison", {})
    stage5 = scores.get("stage5_outlook", {})
    stage6 = scores.get("stage6_valuation", {})
    stage8 = scores.get("stage8_action", {})

    recent_news = scores.get("recent_news", [])

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
    if truncated:
        hallucination_banner += """
  <div class="hallucination-alert" style="background:#fff3cd;border-color:#ffc107;color:#856404">
    <strong>&#9888; 응답 토큰 한도 초과 — 분석이 불완전합니다.</strong><br>
    일부 섹션(거장 분석, 종합 의견, 리스크 등)이 누락될 수 있습니다. 재분석을 실행해 주세요.
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
      <div class="stage-field"><strong>매출-이익률 동행:</strong> {stage4.get("revenue_margin_comovement", "N/A")}</div>
      <div class="stage-field"><strong>FCF 품질:</strong> {stage4.get("fcf_quality", "N/A")}</div>
      <div class="stage-field"><strong>가치함정 리스크:</strong> {stage4.get("value_trap_risk", "N/A")}</div>
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

    # ── Stage 4.5: 동종업계 Peer 비교 ──
    peer_html = ""
    if peer_comparison and peer_comparison.get("peers"):
        peers = peer_comparison["peers"]
        rel_val = peer_comparison.get("relative_valuation", "")
        rel_val_color = {"저평가": "#1e8449", "적정": "#6c757d", "고평가": "#dc3545"}.get(rel_val, "#6c757d")
        better_alt = peer_comparison.get("better_alternative")

        # 비교 테이블 헤더
        peer_table_rows = ""
        # 대상 종목 행
        peer_table_rows += f"""
          <tr style="background:#e8f4f8;font-weight:600">
            <td style="padding:7px 10px">★ {name}</td>
            <td style="padding:7px 10px;text-align:right">{_fmt_val(stock.get("시가총액"), "int")}</td>
            <td style="padding:7px 10px;text-align:right">{_fmt_val(stock.get("PER"), "f2")}</td>
            <td style="padding:7px 10px;text-align:right">{_fmt_val(stock.get("PBR"), "f2")}</td>
            <td style="padding:7px 10px;text-align:right">{_fmt_val(stock.get("ROE(%)"), "f1")}</td>
            <td style="padding:7px 10px;text-align:right">{_fmt_val(stock.get("영업이익률(%)"), "f1")}</td>
            <td style="padding:7px 10px;text-align:right">{_fmt_val(stock.get("매출_CAGR"), "f1")}</td>
          </tr>"""
        # 경쟁사 행
        for p in peers:
            peer_table_rows += f"""
          <tr style="border-bottom:1px solid #dee2e6">
            <td style="padding:6px 10px">{p.get("name", "N/A")}</td>
            <td style="padding:6px 10px;text-align:right">{p.get("market_cap", "N/A")}</td>
            <td style="padding:6px 10px;text-align:right">{p.get("per", "N/A")}</td>
            <td style="padding:6px 10px;text-align:right">{p.get("pbr", "N/A")}</td>
            <td style="padding:6px 10px;text-align:right">{p.get("roe", "N/A")}</td>
            <td style="padding:6px 10px;text-align:right">{p.get("operating_margin", "N/A")}</td>
            <td style="padding:6px 10px;text-align:right">{p.get("revenue_growth", "N/A")}</td>
          </tr>"""

        # 순위 뱃지
        rank_badges = ""
        rank_colors = {"1위": "#d4af37", "2위": "#a8a9ad", "3위": "#cd7f32"}
        for label, field in [("PER", "per"), ("PBR", "pbr"), ("ROE", "roe"), ("영업이익률", "operating_margin"), ("매출성장률", "revenue_growth")]:
            rank_val = peer_comparison.get("target_rank", {}).get(field, "")
            badge_color = next((c for k, c in rank_colors.items() if k in rank_val), "#6c757d")
            rank_badges += f'<span style="background:{badge_color};color:#fff;padding:2px 8px;border-radius:4px;font-size:11px;margin-right:4px;font-weight:600">{label}: {rank_val}</span>'

        # 대안 종목 박스
        alt_html = ""
        if better_alt and better_alt != "null":
            alt_html = f'<div style="background:#fff3cd;border:1px solid #ffc107;border-radius:6px;padding:10px 14px;margin-top:10px;font-size:13px"><strong>💡 더 매력적인 대안:</strong> {better_alt}</div>'

        peer_html = f"""
  <div class="stage-card" style="margin-top:16px">
    <div class="stage-card-title">STAGE 4.5 &mdash; 동종업계 Peer 비교 분석</div>
    <div style="margin-bottom:10px">
      <span style="font-size:13px;font-weight:600;margin-right:8px">상대 밸류에이션:</span>
      <span style="background:{rel_val_color};color:#fff;padding:2px 10px;border-radius:4px;font-size:13px;font-weight:700">{rel_val}</span>
    </div>
    <div style="overflow-x:auto">
      <table style="width:100%;border-collapse:collapse;font-size:12px">
        <thead>
          <tr style="background:#f8f9fa;border-bottom:2px solid #dee2e6">
            <th style="padding:8px 10px;text-align:left">종목</th>
            <th style="padding:8px 10px;text-align:right">시총(억)</th>
            <th style="padding:8px 10px;text-align:right">PER</th>
            <th style="padding:8px 10px;text-align:right">PBR</th>
            <th style="padding:8px 10px;text-align:right">ROE(%)</th>
            <th style="padding:8px 10px;text-align:right">영업이익률(%)</th>
            <th style="padding:8px 10px;text-align:right">매출성장률(%)</th>
          </tr>
        </thead>
        <tbody style="font-size:12px">{peer_table_rows}
        </tbody>
      </table>
    </div>
    <div style="margin-top:12px;flex-wrap:wrap;display:flex;gap:4px">{rank_badges}</div>
    {alt_html}
    <div class="stage-analysis" style="margin-top:12px">{peer_comparison.get("analysis", "")}</div>
  </div>"""

    # ── 최신 뉴스 & 공시 섹션 ──
    news_html = ""
    if recent_news:
        news_items = ""
        for item in recent_news[:5]:
            title = item.get("title", "")
            date = item.get("date", "")
            summary_text = item.get("summary", "")
            impact = item.get("impact", "중립")
            source = item.get("source", "")

            impact_cls = {"긍정": "impact-positive", "부정": "impact-negative"}.get(impact, "impact-neutral")
            impact_icon = {"긍정": "&#9650;", "부정": "&#9660;"}.get(impact, "&#9644;")

            news_items += f"""
        <div class="news-card">
          <div class="news-card-header">
            <span class="news-impact-badge {impact_cls}">{impact_icon} {impact}</span>
            <span class="news-date">{date}</span>
          </div>
          <div class="news-title">{title}</div>
          <div class="news-summary">{summary_text}</div>
          <div class="news-source">{source}</div>
        </div>"""

        news_html = f"""
  <div class="news-section">
    <div class="stage-card-title">&#128240; 최신 뉴스 &amp; 공시</div>
    <div class="news-grid">{news_items}
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

    # ── Stage 7: 9대 거장 카드 ──
    master_cards = ""
    for key, info in MASTER_INFO.items():
        m = stage7.get(key, {})
        if not m:
            continue  # 구버전 보고서 호환: 없는 거장은 건너뜀
        s = m.get("score", 0)
        one_liner = m.get("one_liner", "")
        analysis = m.get("analysis", "")
        bar_w = _score_bar_width(s)

        # 거장별 추가 정보 렌더링
        extra_html = ""
        if key == "munger":
            bear_cases = m.get("bear_case_top3", [])
            if bear_cases:
                items = "".join(f'<li style="margin:2px 0;color:#c0392b">{bc}</li>' for bc in bear_cases)
                extra_html = f'<div style="margin-top:8px;padding:8px 10px;background:#fdf2f2;border-radius:6px;border-left:3px solid #c0392b"><strong style="font-size:12px;color:#c0392b">&#9888; 실패 시나리오 Top 3</strong><ul style="margin:4px 0 0 16px;padding:0;font-size:13px">{items}</ul></div>'
        elif key == "marks":
            gap = m.get("consensus_gap", "")
            if gap:
                extra_html = f'<div style="margin-top:8px;padding:8px 10px;background:#f4ecf7;border-radius:6px;border-left:3px solid #8e44ad"><strong style="font-size:12px;color:#8e44ad">&#x1F50D; 시장 기대 vs 실제</strong><p style="margin:4px 0 0;font-size:13px">{gap}</p></div>'
        elif key == "klarman":
            catalysts_list = m.get("catalysts", [])
            if catalysts_list:
                cat_items = ""
                for cat in catalysts_list:
                    if isinstance(cat, dict):
                        prob = cat.get("probability", "medium")
                        prob_color = {"high": "#27ae60", "medium": "#f39c12", "low": "#e74c3c"}.get(prob, "#888")
                        cat_items += f'<li style="margin:2px 0"><span style="background:{prob_color};color:#fff;padding:1px 5px;border-radius:3px;font-size:10px;margin-right:4px">{prob}</span>{cat.get("event", "")} <small style="color:#888">({cat.get("timing", "")})</small></li>'
                    else:
                        cat_items += f'<li style="margin:2px 0">{cat}</li>'
                extra_html = f'<div style="margin-top:8px;padding:8px 10px;background:#fdf2e9;border-radius:6px;border-left:3px solid #d35400"><strong style="font-size:12px;color:#d35400">&#x1F4A1; 촉매 식별</strong><ul style="margin:4px 0 0 16px;padding:0;font-size:13px">{cat_items}</ul></div>'

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
          {extra_html}
        </div>"""

    # ── 리스크 & 촉매 리스트 ──
    _sev_color = {"high": "#dc3545", "medium": "#fd7e14", "low": "#6c757d"}
    risk_items_html = []
    for r in risks:
        if isinstance(r, dict):
            sev = r.get("severity", "medium")
            badge = f'<span style="background:{_sev_color.get(sev,"#6c757d")};color:#fff;padding:1px 6px;border-radius:3px;font-size:11px;margin-right:6px;font-weight:600">{sev.upper()}</span>'
            cat = f'<strong>[{r.get("category","")}]</strong> ' if r.get("category") else ""
            desc = r.get("description", "")
            evidence = r.get("evidence", "")
            ev_html = f'<br><small style="color:#888;font-size:12px">근거: {evidence}</small>' if evidence else ""
            risk_items_html.append(f'<li class="risk-item">{badge}{cat}{desc}{ev_html}</li>')
        else:
            risk_items_html.append(f'<li class="risk-item">{r}</li>')
    risk_items = "".join(risk_items_html)
    catalyst_items = "".join(f'<li class="catalyst-item">{c}</li>' for c in catalysts)

    # ── Stage 8: 매매 액션 플랜 ──
    action_html = ""
    if stage8:
        entry_basis = stage8.get("entry_basis", "")
        target_basis = stage8.get("target_basis", "")
        exit_conds = stage8.get("exit_conditions", [])
        exit_html = ""
        if exit_conds:
            items = "".join(f"<li>{c}</li>" for c in exit_conds)
            exit_html = f'<div class="action-exit"><strong>매도 조건:</strong><ul style="margin:4px 0 0 16px;padding:0">{items}</ul></div>'
        action_html = f"""
  <div class="action-plan-box">
    <div class="stage-card-title">STAGE 8 &mdash; 트레이딩 액션 플랜</div>
    <div class="action-plan-grid">
      <div class="action-item">
        <div class="action-label">매수 진입가</div>
        <div class="action-value entry">{stage8.get("entry_price", "N/A")}</div>
        {f'<div style="font-size:11px;color:#888;margin-top:3px">{entry_basis}</div>' if entry_basis else ""}
      </div>
      <div class="action-item">
        <div class="action-label">목표 주가</div>
        <div class="action-value target">{stage8.get("target_price", "N/A")}</div>
        {f'<div style="font-size:11px;color:#888;margin-top:3px">{target_basis}</div>' if target_basis else ""}
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
    {exit_html}
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
{peer_html}
{news_html}
{valuation_html}

  <div class="summary-box">
    <h4>종합 투자 의견</h4>
    <p>{summary}</p>
  </div>

  <h4 class="section-title">9대 투자 거장 관점 분석</h4>
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


# ─────────────────────────────────────────
# 상관관계 계산
# ─────────────────────────────────────────

def compute_correlation_matrix(codes: list[str], names: dict[str, str],
                                n_days: int = 250) -> dict | None:
    """종목 리스트의 가격 상관관계 행렬 계산.

    Returns:
        {"matrix": [[float]], "codes": [str], "names": [str]} 또는 None (데이터 부족)
    """
    try:
        price_df = _db.load_price_history_multi(codes, n_days=n_days)
    except Exception as e:
        log.warning("price_history 로드 실패: %s", e)
        return None

    if price_df.empty or price_df.shape[1] < 2:
        return None

    # 60 거래일 미만 종목 제거
    valid_cols = [c for c in price_df.columns if price_df[c].notna().sum() >= 60]
    if len(valid_cols) < 2:
        return None

    price_df = price_df[valid_cols]
    returns = price_df.pct_change().dropna(how="all")
    corr = returns.corr().round(2)

    valid_codes = list(corr.columns)
    matrix = corr.values.tolist()

    # NaN → None 처리
    clean_matrix = []
    for row in matrix:
        clean_matrix.append([None if (isinstance(v, float) and np.isnan(v)) else v for v in row])

    return {
        "matrix": clean_matrix,
        "codes": valid_codes,
        "names": [names.get(c, c) for c in valid_codes],
    }


# ─────────────────────────────────────────
# 포트폴리오 AI 종합 분석
# ─────────────────────────────────────────

PORTFOLIO_SYSTEM_PROMPT = """\
당신은 한국 주식시장 포트폴리오 전략 어드바이저입니다.
투자자의 보유 포트폴리오 전체를 분석하여 포트폴리오 수준의 전략적 조언을 제공합니다.

## 투자 철학 (최우선 전제)

이 포트폴리오의 기본 전제는 **종목과 해당 산업의 장기적 우상향**입니다.
모든 분석과 권고는 이 철학을 최우선으로 적용하세요.

- **1순위 판단 기준은 산업의 장기 성장성**입니다. 전방산업 CAGR, AI 분석의 산업분류, 해자 등급을 핵심 근거로 활용하세요.
- 장기 성장하는 산업에 속한 고품질 종목(해자 wide/narrow)은 단기 등락과 무관하게 HOLD 또는 BUY_MORE를 우선 고려하세요.
- **TRIM/SELL 권고 기준**: 아래 중 하나라도 해당하면 과감하게 TRIM/SELL을 권고하세요. 단기 PER 상승만으로 TRIM하지 않되, PER이 업종 역사적 고평가 수준이면서 분기 실적 감속·RS등급 하락이 동반되면 비중 조절을 검토하세요.
  1. 산업 성장성 훼손, 해자 소실, 구조적 경쟁력 약화 확인
  2. 퀀트 지표 동시 악화: 퀀트점수 < 40 **이면서** RS등급 < 40 (모멘텀+펀더멘털 동시 이탈)
  3. 재무건전성 급격 악화: F스코어 < 4 (피오트로스키)
  4. 영업이익 2분기 이상 연속 역성장하면서 회복 근거 없음
- 현금(예수금)이 있는 경우, BUY_MORE 권고 시 예수금 범위 내에서 실행 가능한 매수량/금액을 우선 제시하세요.
- 수급(코스톨라니 달걀 위치, 시장심리)은 매수 타이밍 보조 지표로만 활용하고, 장기 성장성 판단을 뒤집어서는 안 됩니다.

### 밸류에이션 가드레일 (성장 함정 방지)
- 포트폴리오 가중평균 PER > 25x 또는 PBR > 3x → `portfolio_health.valuation`에 "밸류에이션 리스크" 경고 필수
- PEG > 2인 종목에 BUY_MORE 권고 시 → rationale에 PEG 수준과 그럼에도 매수하는 근거 명시
- PER > 40 종목 → recommended_weight 상한 7% 적용 (단, 전방산업 CAGR>15% + 해자 wide 동시 충족 시 예외)
- 성장률 높지만 과도한 밸류에이션(PER>30, PEG>2.5) → portfolio_risks에 "성장 함정 리스크" 포함 (제레미 시겔: 고성장 기업이 반드시 고수익 투자는 아님)

## 분석 프레임워크

### 1. 포트폴리오 건강도 평가
- 전체 포트폴리오의 품질 점수 (0-100)
- **산업 성장성, 해자 품질, 밸류에이션, 분산도** 관점에서 종합 평가

### 2. 종목별 액션 권고
- 각 종목에 대해 BUY_MORE / HOLD / TRIM / SELL 중 하나를 권고
- **판단 우선순위**: ① 산업 장기 성장성(전방산업 CAGR) → ② 해자 등급(wide/narrow/none) → ③ 개별 AI 분석 → ④ 현재 비중 → ⑤ 수급/타이밍 신호
- 현재 비중 vs 권장 비중, 구체적 매매 수량(target_shares) 및 목표가 범위 제시 (출력 스키마 참조)
- 예수금이 있으면 BUY_MORE 시 예수금 내 실행 가능 금액 우선

### 3. 섹터/산업 성장성 분석
- AI 분석 데이터(전방산업 CAGR, 해자 등급)를 기반으로 각 섹터의 **장기 성장성**을 평가하세요
- 장기 성장 산업에 과소 비중인 경우 → 비중 확대 권고
- 장기 쇠퇴 우려 산업에 집중된 경우 → 구조적 리스크로 표시
- 섹터 내 종목들의 해자 등급(wide/narrow/none)을 종합하여 섹터 품질을 평가하세요
- 단순 집중도 수치보다 **산업 장기 성장성 + 해자 품질** 기반으로 판단하세요

### 4. 포트폴리오 리스크 & 촉매
- 포트폴리오 전체 관점에서의 주요 리스크 (상관관계, 동일 리스크 노출 등)
- 12개월 촉매(AI 분석 제공 시) 및 포트폴리오 전체 상승 촉매
- **상관관계 행렬 데이터가 제공된 경우**: 수치를 기반으로 고상관 페어(상관계수 0.7 이상)를 구체적으로 식별하고, 분산 효과 부족 구간 지적
- 상관관계 데이터가 없는 경우: 섹터/밸류체인 기반 정성적 판단으로 대체

### 5. 적정 종목수 & 구성 최적화
- 현재 종목수가 투자 전략 대비 적정한지 평가
- 권장 종목수 범위(min~max) 제시
- 전략 유형 판정: balanced(균형형, 7~15종목), concentrated(집중형, 3~6종목), diversified(분산형, 16종목 이상)
- 종목수 조정이 필요한 경우 구체적 방향 제안 (축소/유지/확대 및 이유)

### 6. 배당 분석
- 포트폴리오 가중평균 배당수익률, 연간 예상 배당금, 배당 성향 추세(증가/유지/감소/혼재/무배당 다수) 평가

### 7. 보완 제안
- 장기 성장 산업 중 미편입 섹터/테마를 우선으로 추가 편입 후보 테마 제안 (종목 추천 아님)

### 8. 관심종목 편입 권고 (관심종목 데이터가 있는 경우)
- 관심종목 중 포트폴리오에 추가하면 좋을 종목 식별
- 각 관심종목에 대해 ADD(편입 권고) / WATCH(관찰 지속) / SKIP(제외 권고) 판정
- ADD 종목: 권장 비중, 매수 주수, 목표가 범위, 예상 금액 제시
- 기존 포트폴리오와의 섹터 분산 효과, 산업 성장성 보완, 상관관계 고려
- **대체 편입 우선 검토**: TRIM/SELL 권고 종목과 동일 섹터·테마에서 더 우수한 관심종목이 있으면, 해당 관심종목을 ADD로 권고하고 rationale에 "기존 보유 [종목명]의 대체 후보" 를 명시하세요. 매도 재원으로 매수할 수 있는 주수·금액도 함께 제시하세요.

### 9. 리밸런싱 실행 계획
- 가장 시급한 액션 1-3개 우선순위 제시
- 실행 순서 및 권장 시기 (즉시/이번 달/이번 분기/불필요)
- 예수금이 있는 경우: 현금 활용 우선순위 포함

### 10. 역발상 점검 (Devil's Advocate — 확증 편향 방지)
이 섹션은 AI의 성장주·컨센서스 편향을 교정하는 필수 검증입니다. 건너뛰지 마세요.
- **BUY_MORE 실패 시나리오**: BUY_MORE 권고 종목마다 "이 매수가 실패하는 구체적 시나리오 1개"를 rationale에 반드시 포함
- **약세 시나리오**: "만약 이 포트폴리오를 공매도한다면?" 논리를 3-5문장으로 작성 (perspective_balance.bear_case_summary)
- **최대 추정 낙폭**: 섹터별 역사적 최대 하락률 기반 포트폴리오 비관 시나리오 낙폭 추정 (perspective_balance.estimated_max_drawdown)
- **성장 vs 가치 편향 진단**: 포트폴리오가 성장주 편향인지 가치주 편향인지 객관적 판정 (perspective_balance.growth_vs_value_tilt)
- **컨센서스 동조 위험**: 주요 종목들이 시장 컨센서스와 동일 방향(모두 성장주, 모두 AI 수혜)이면 군중 동조 리스크 명시

### 한국 시장 특수 리스크 (항상 점검)
- **코리아 디스카운트**: PBR<1 + 지배구조 우려(재벌 계열) 종목 → 밸류업 프로그램 수혜 가능성 vs 구조적 저평가(밸류트랩) 여부 구분 평가
- **코스닥 좀비 경고**: 코스닥 종목 중 영업이익 2년+ 연속 적자 + 부채비율>100% → portfolio_risks에 "좀비 리스크" 경고 (코스닥 기업 약 47% 적자, 23.7% 이자보상배율 미달)
- **FX 노출 평가**: 수출주 vs 내수주 비중 평가. 어느 한쪽 70%+ 편중 시 "환율 편중 리스크" 지적
- **생존자 편향 주의**: RS등급>80 종목 3개 이상 시 portfolio_risks에 포함 — 모멘텀 전략은 상장폐지 종목 제외로 과거 수익률이 과대평가됨

## 매크로 환경 활용 지침
- 프롬프트의 "## 거시경제 환경" 섹션을 참고하여 매크로 환경을 분석하세요.
- **현금 비중**: 방어적 환경(성장 하향 + 실질금리 상승 + 금융여건 악화)이면 예수금을 BUY_MORE에 소진하지 말고 유지/확대를 권고하세요. 공격적 환경이면 예수금 범위 내 BUY_MORE를 적극 검토하세요.
- **섹터 배분**: 매크로 유리 섹터에 속한 종목에 BUY_MORE를 가중하고, 불리 섹터에 과집중된 경우 TRIM을 검토하세요. 단, 장기 성장성과 해자가 충분한 종목은 매크로 불리라도 HOLD를 우선합니다.
- **리스크 평가**: 금리 상승, 달러 강세, 신용스프레드 확대 등 매크로 리스크를 `portfolio_risks`에 포함하세요.
- **macro_assessment 필드**: 반드시 채워주세요. 제공된 매크로 분석을 기반으로 하되, 포트폴리오 종목 구성과의 적합도(portfolio_alignment)를 함께 평가하세요.
- **시장 심리 역이용**: 심리가 극단적 공포이면 방어 환경에서도 역발상 BUY_MORE 기회를 검토하세요. 극단적 탐욕이면 공격 환경에서도 신규 매수 자제를 권고하세요.
- **유동성 보정**: M2 확대 전환 시 금리 긴축에도 자산 가격 지지 가능 → 방어 판단 완화. M2 위축 시 금리 인하에도 자산 하락 가능 → 공격 판단 보수화.
- **이익 추정치 교차 검증**: 성장 상향 판단이라도 이익 추정치(earnings_revision)가 하향이면 실질 이익 감소 주의를 portfolio_risks에 포함하세요.

## 웹 검색 활용 지침
web_search 툴을 활용하여 분석 전 반드시 최신 정보를 확인하세요:
1. **보유 종목별 최신 이슈**: 각 보유 종목의 최근 뉴스, 실적 발표, 산업 업황 변화 (종목당 1건)
2. **포트폴리오 관련 섹터 업황**: 주요 섹터(반도체, 방산, 에너지 등)의 최신 동향
3. **매크로 리스크**: 현재 글로벌 지정학·금융 리스크(관세, 환율, 금리 등) 최신 상황
검색 결과를 바탕으로 portfolio_risks, portfolio_catalysts, 종목별 rationale에 최신 정보를 반영하세요.

## 웹 검색 활용 지침
web_search 툴은 최대 2회만 사용 가능합니다. 개별 종목 정보는 이미 종목별 AI 분석에서 수집되었으므로 **절대 재검색하지 마세요**.
포트폴리오 전체 관점에서 반드시 필요한 정보만 검색하세요:
1. **현재 글로벌·한국 거시경제 리스크**: 지정학 리스크, 관세·무역 갈등, 금리·환율 최신 동향, 금융시장 스트레스 이슈 (1회)
2. **포트폴리오 핵심 섹터 업황**: 포트폴리오에서 비중이 큰 섹터(예: 반도체, 방산, 에너지 등)의 최근 업황 변화 (1회)
검색 결과는 `portfolio_risks`, `macro_assessment.key_risks`, 리밸런싱 판단에 반영하세요.

## 분석 지침
- 포트폴리오 비중이 높은 종목에 더 주의를 기울이세요
- 종목 간 상관관계와 중복 리스크를 식별하세요
- 데이터가 없는 종목(ETF, 우선주 등)은 비중 분석에만 포함하세요 (비중=None인 종목은 현재가 없음)
- 9대 투자 거장(Buffett, Damodaran, Fisher, Dorsey, Lynch, Kostolany, Munger, Marks, Klarman)의 관점이 기존 분석에 포함되어 있으면 이를 종합적으로 활용하세요
- 관심종목이 없으면 watchlist_recommendations는 빈 배열로 출력하세요
- 한국어로 분석하세요
"""

PORTFOLIO_USER_PROMPT_TEMPLATE = """\
아래 포트폴리오의 전체 데이터를 분석하여, 포트폴리오 수준의 전략적 분석 보고서를 작성해주세요.

## 포트폴리오 요약
- 총 종목수: {stock_count}
- 총평가금액(주식): {total_eval}원
- 예수금(현금): {cash_balance}원 ({cash_weight})
- 총자산(주식+현금): {total_assets}원
- 총수익률: {total_return}%
- 섹터 분포: {sector_distribution}

{macro_section}
## 종목별 데이터

{per_stock_sections}

{watchlist_section}
{correlation_section}
## 출력 형식

반드시 아래 JSON 형식으로만 응답하세요. JSON 외 다른 텍스트는 절대 포함하지 마세요.

```json
{{
  "portfolio_health": {{
    "score": 0,
    "grade": "B",
    "diversification": "분산도 평가 2-3문장",
    "valuation": "밸류에이션 평가 2-3문장",
    "growth_quality": "성장성/품질 평가 2-3문장",
    "overall_assessment": "종합 평가 3-5문장"
  }},
  "macro_assessment": {{
    "environment": "공격적|중립|방어적",
    "cash_signal": "축소|유지|확대",
    "favorable_sectors": ["매크로 유리 섹터"],
    "unfavorable_sectors": ["매크로 불리 섹터"],
    "key_risks": "매크로 기반 주요 리스크 2-3문장",
    "portfolio_alignment": "현재 포트폴리오의 매크로 환경 적합도 2-3문장"
  }},
  "stock_actions": [
    {{
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
    }}
  ],
  "sector_analysis": {{
    "concentration_risk": "섹터 집중 리스크 평가 2-3문장",
    "overweight_sectors": ["과비중 섹터"],
    "underweight_sectors": ["과소비중 섹터"],
    "rebalancing_suggestion": "리밸런싱 권고 2-3문장"
  }},
  "portfolio_risks": [
    {{
      "risk": "리스크 설명",
      "severity": "high|medium|low",
      "affected_stocks": ["종목코드"],
      "correlation_note": "상관관계 관련 메모 (해당시)"
    }}
  ],
  "portfolio_catalysts": [
    {{
      "catalyst": "촉매 설명",
      "impact": "high|medium|low",
      "benefiting_stocks": ["종목코드"]
    }}
  ],
  "missing_themes": [
    {{
      "theme": "부족한 테마/섹터명",
      "reason": "왜 추가가 필요한지 1-2문장"
    }}
  ],
  "watchlist_recommendations": [
    {{
      "code": "종목코드",
      "name": "종목명",
      "action": "ADD|WATCH|SKIP",
      "recommended_weight": 0.0,
      "target_shares": 0,
      "target_price_low": 0,
      "target_price_high": 0,
      "estimated_amount": 0,
      "rationale": "2-3문장 근거",
      "synergy": "포트폴리오 시너지/보완 효과 설명"
    }}
  ],
  "portfolio_optimization": {{
    "current_count": 0,
    "recommended_count_min": 0,
    "recommended_count_max": 0,
    "strategy_type": "balanced|concentrated|diversified",
    "assessment": "현재 종목수 적정성 평가 2-3문장",
    "adjustment_suggestion": "종목수 조정 방향 및 근거 2-3문장"
  }},
  "dividend_analysis": {{
    "portfolio_yield": 0.0,
    "annual_dividend_estimate": 0,
    "dividend_growth_trend": "증가|유지|감소|혼재|무배당 다수",
    "suggestion": "배당 인컴 관점 포트폴리오 평가 및 개선 제안 2-3문장"
  }},
  "rebalancing_plan": {{
    "urgency": "immediate|monthly|quarterly|none",
    "priority_actions": ["가장 먼저 실행할 액션 1", "액션 2"],
    "execution_note": "실행 순서 및 권장 시기 2-3문장"
  }},
  "perspective_balance": {{
    "growth_vs_value_tilt": "성장 편향|균형|가치 편향",
    "valuation_risk_level": "low|medium|high",
    "contrarian_opportunities": ["역발상 기회가 있는 종목명 (없으면 빈 배열)"],
    "consensus_risk": "시장 컨센서스와 동조 정도 2-3문장",
    "bear_case_summary": "포트폴리오 전체 약세 시나리오 3-5문장",
    "estimated_max_drawdown": "비관적 시나리오 최대 추정 낙폭 (예: -25%~-35%)"
  }},
  "summary": "5-7문장 종합 포트폴리오 전략 의견"
}}
```
"""


def format_portfolio_stock(stock: dict, portfolio_item: dict,
                           ai_report: dict | None) -> str:
    """포트폴리오 종목 1개의 데이터를 텍스트로 포맷팅."""
    code = str(portfolio_item.get("종목코드", "")).zfill(6)
    name = portfolio_item.get("종목명", code)

    lines = [f"\n### {name} ({code})"]
    lines.append(f"- 보유수량: {portfolio_item.get('수량', 0):,}주")
    avg = portfolio_item.get("평균매입가", 0) or 0
    lines.append(f"- 평균매입가: {avg:,.0f}원")
    cur = portfolio_item.get("현재가")
    lines.append(f"- 현재가: {cur:,.0f}원" if cur else "- 현재가: N/A")
    pct = portfolio_item.get("수익률")
    lines.append(f"- 수익률: {pct:.2f}%" if pct is not None else "- 수익률: N/A")
    weight = portfolio_item.get("비중", 0)
    lines.append(f"- 비중: {weight:.1f}%")
    lines.append(f"- 섹터: {portfolio_item.get('섹터') or 'N/A'}")

    # 정량 데이터 (포트폴리오 전용 경량 포맷)
    if stock:
        lines.append(format_portfolio_quant_compact(stock, has_ai_report=bool(ai_report)))

    # ETF 메타데이터 (하드코딩)
    etf_meta = config.ETF_METADATA.get(code)
    if etf_meta and not stock:
        lines.append("\n#### ETF 정보")
        lines.append(f"- 테마/섹터: {etf_meta['sector']}")
        lines.append(f"- 설명: {etf_meta['description']}")
        lines.append(f"- 주요 구성종목: {', '.join(etf_meta['constituents'])}")

    # 기존 AI 분석 요약
    if ai_report:
        lines.append("\n#### 기존 AI 분석 요약")
        lines.append(f"- 종합점수: {ai_report.get('composite_score', 'N/A')}")
        lines.append(f"- 등급: {ai_report.get('investment_grade', 'N/A')}")
        lines.append(f"- 요약: {ai_report.get('summary', 'N/A')}")
        risks = ai_report.get("risks", [])
        if risks:
            def _r_str(r):
                if isinstance(r, dict):
                    return r.get("description", str(r))
                return str(r)
            lines.append(f"- 리스크: {', '.join(_r_str(r) for r in risks)}")
        catalysts = ai_report.get("catalysts", [])
        if catalysts:
            lines.append(f"- 촉매: {', '.join(str(c) for c in catalysts)}")
        # 산업/해자 정보
        biz = ai_report.get("business_identity", {})
        if biz.get("industry_classification"):
            lines.append(f"- 산업분류: {biz['industry_classification']}")
        moat = ai_report.get("stage3_moat", {})
        if moat.get("moat_rating"):
            lines.append(f"- 해자등급: {moat['moat_rating']}")
        # 전방산업 성장성
        macro = ai_report.get("stage1_macro", {})
        if macro.get("upstream_cagr"):
            lines.append(f"- 전방산업 성장률(CAGR): {macro['upstream_cagr']}")
        # 12개월 촉매
        outlook = ai_report.get("stage5_outlook", {})
        cats_12m = outlook.get("catalysts_12m", [])
        if cats_12m:
            lines.append(f"- 12개월 촉매: {', '.join(str(c) for c in cats_12m[:3])}")

    return "\n".join(lines)


MACRO_ASSESSMENT_SYSTEM_PROMPT = """\
당신은 한국 주식시장 전문 거시경제 애널리스트입니다.
웹 검색 툴을 적극 활용하여 현재 실시간 데이터(환율, 금리, 유가, 주요 뉴스)를 직접 조회한 후,
글로벌 및 한국 거시경제 환경을 분석하여 투자자에게 필요한 매크로 컨텍스트를 제공합니다.
반드시 JSON 형식으로만 최종 응답하고, JSON 외 다른 텍스트는 포함하지 마세요.
"""

MACRO_ASSESSMENT_USER_PROMPT = """\
웹 검색 툴을 사용하여 다음 최신 데이터를 조회한 후, 한국 주식시장 투자자 관점의 거시경제 분석을 수행하세요.

## 필수 검색 항목 (웹 검색으로 실시간 확인)
1. 원달러 환율 현재 수준 (USD/KRW 실시간)
2. 한국은행 기준금리 및 미 연준 기준금리 현황
3. 국제유가 현재 수준 (WTI/Brent)
4. 중국 경제지표 최근 동향 (PMI, 수출입 등)
5. 한국 수출 최근 동향
6. 미국/글로벌 주요 금융 리스크 뉴스 (지정학적 리스크, 금융시장 충격 등)
7. 반도체 업황 최근 뉴스 (메모리 가격, AI 수요 등)
8. 글로벌 주요 이슈 (무역 갈등, 지정학 리스크 등)

## 평가 항목
1. 경기성장 방향 (상향/중립/하향): GDP 성장률, 기업 이익 사이클, 경기선행지수
2. 실질금리 방향 (하락/중립/상승): 한미 금리 정책, 기대인플레이션 대비 명목금리
3. 금융여건 (완화/중립/악화): 크레딧 스프레드, 유동성, 금융 시스템 리스크
4. 원달러 환율 방향 (원화강세/중립/원화약세): 실제 환율 수준 및 방향성
5. 원자재/에너지 (하락/안정/상승): 실제 유가 수준 및 원자재 트렌드
6. 신용스프레드 (완화/중립/악화): 회사채·하이일드 스프레드, 금융시장 스트레스
7. 중국 경기 (상향/중립/하향): 중국 경기 모멘텀, 한국 수출 영향
8. 반도체 사이클 (상향/중립/하향): 메모리/비메모리 업황, AI 수요
9. 구조적 CAPEX 테마: 현재 집중되는 산업 테마 (AI 인프라, 전력망, 방산 등)
10. 시장 심리/변동성 (탐욕/중립/공포): VIX 수준, VKOSPI, 투자심리 종합
11. 유동성 여건 (확대/안정/위축): 글로벌 M2 증가율 방향, 주요 중앙은행 자산 규모 변화
12. 시장 내부 건전성 (양호/혼조/악화): 상승종목 비율, 200일 이동평균 위 종목 비율
13. 이익 추정치 방향 (상향/중립/하향): 한국 상장사 12개월 선행 EPS 수정 방향

## 현금 비중 판단 기준
- 공격적: 성장 상향 + 실질금리 하락 + 금융여건 완화 → 현금 축소 (주식 확대)
- 방어적: 성장 하향 + 실질금리 상승 + 금융여건 악화 → 현금 확대
- 중립: 그 외 → 현금 유지

중요: key_risks에는 현재 실제로 발생 중인 지정학·금융 리스크(이란/미국 갈등, 사모펀드 환매, 관세 전쟁 등)를 웹 검색으로 확인하여 포함하세요.

반드시 아래 JSON 형식으로만 최종 응답하세요:
```json
{
  "environment": "공격적|중립|방어적",
  "cash_signal": "축소|유지|확대",
  "growth": "상향|중립|하향",
  "real_rate": "하락|중립|상승",
  "financial_conditions": "완화|중립|악화",
  "usd_krw": "원화강세|중립|원화약세",
  "usd_krw_level": "현재 원달러 환율 수치 (예: 1,487원)",
  "commodities": "하락|안정|상승",
  "commodities_detail": "WTI $XX, Brent $XX 등 실제 수치",
  "credit_spread": "완화|중립|악화",
  "china": "상향|중립|하향",
  "semiconductor": "상향|중립|하향",
  "market_sentiment": "탐욕|중립|공포",
  "liquidity_conditions": "확대|안정|위축",
  "market_breadth": "양호|혼조|악화",
  "earnings_revision": "상향|중립|하향",
  "credit_cycle_stage": "초기회복|확장|후기|수축",
  "capex_theme": "AI 인프라, 전력망 등 구체적 테마 텍스트",
  "favorable_sectors": ["유리한 섹터1", "유리한 섹터2"],
  "unfavorable_sectors": ["불리한 섹터1", "불리한 섹터2"],
  "key_risks": "현재 실제 발생 중인 주요 거시·지정학·금융 리스크 3-4문장",
  "summary": "현재 매크로 환경 종합 요약 (실제 데이터 수치 포함) 4-5문장"
}
```
"""


def generate_macro_assessment(user_prompt: str = "") -> dict:
    """AI가 웹 검색으로 최신 데이터를 조회 후 거시경제 환경을 분석하여 macro_assessment dict 반환.

    웹 검색 툴(web_search)을 사용하여 실시간 환율, 유가, 금리, 주요 뉴스를 조회 후 분석.

    Args:
        user_prompt: 사용자 추가 분석 요청 (기존 프롬프트에 append)

    Returns:
        {
            "scores": {environment, cash_signal, growth, real_rate, ...},
            "model": str,
            "generated_date": str,
        }
    """
    if not config.ANTHROPIC_API_KEY:
        raise ValueError("ANTHROPIC_API_KEY 환경변수가 설정되지 않았습니다.")

    client = anthropic.Anthropic(
        api_key=config.ANTHROPIC_API_KEY,
        timeout=httpx.Timeout(180.0, connect=10.0),
    )

    log.info("매크로 AI 분석 시작 (model=%s, web_search=True, user_prompt=%s)",
             config.ANALYSIS_MODEL, bool(user_prompt))

    prompt = MACRO_ASSESSMENT_USER_PROMPT
    if user_prompt:
        prompt += (
            f"\n\n## 사용자 추가 분석 요청\n{user_prompt}\n\n"
            "위 사용자 요청사항을 반드시 분석에 반영하되, "
            "기존 필수 검색 항목과 JSON 출력 형식은 그대로 유지하세요."
        )

    # web_search_20250305: server-side tool — Claude가 직접 검색 실행, tool_result 불필요
    message = _call_with_retry(
        client,
        model=config.ANALYSIS_MODEL,
        max_tokens=4096,
        system=MACRO_ASSESSMENT_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": prompt}],
        tools=[{"type": "web_search_20250305", "name": "web_search", "max_uses": 5}],
    )

    block_types = [getattr(b, "type", "?") for b in message.content]
    log.info("매크로 분석 응답 (stop_reason=%s, blocks=%s)", message.stop_reason, block_types)

    raw_text = ""
    for block in message.content:
        if hasattr(block, "type") and block.type == "text":
            raw_text += block.text
    raw_text = raw_text.strip()

    scores = _parse_json_response(raw_text)
    generated_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log.info("매크로 AI 분석 완료 (environment=%s)", scores.get("environment"))

    return {
        "scores": scores,
        "model": config.ANALYSIS_MODEL,
        "generated_date": generated_date,
    }


def format_macro_context(macro: dict) -> str:
    """매크로 체크리스트 데이터를 포트폴리오 분석 프롬프트 텍스트로 포맷팅."""
    lines = ["\n## 거시경제 환경 (매크로 체크리스트)\n"]

    lines.append("### 상위 프레임 (현금 비중 판단)")
    lines.append(f"- 성장 방향: {macro.get('growth', '미입력')}")
    lines.append(f"- 실질금리 방향: {macro.get('real_rate', '미입력')}")
    lines.append(f"- 금융여건: {macro.get('financial_conditions', '미입력')}")
    env = macro.get('environment', '미입력')
    lines.append(f"→ 현금 비중 판단: {env}")

    extra_lines = []
    if macro.get('usd_krw'):
        extra_lines.append(f"- 달러/원화: {macro['usd_krw']}")
    if macro.get('commodities'):
        extra_lines.append(f"- 원자재/에너지: {macro['commodities']}")
    if macro.get('credit_spread'):
        extra_lines.append(f"- 신용스프레드: {macro['credit_spread']}")
    if macro.get('capex_theme'):
        extra_lines.append(f"- 구조적 CAPEX 테마: {macro['capex_theme']}")
    if macro.get('china'):
        extra_lines.append(f"- 중국 경기: {macro['china']}")
    if macro.get('semiconductor'):
        extra_lines.append(f"- 반도체 가격: {macro['semiconductor']}")
    if macro.get('market_sentiment'):
        extra_lines.append(f"- 시장 심리: {macro['market_sentiment']}")
    if macro.get('liquidity_conditions'):
        extra_lines.append(f"- 유동성 여건: {macro['liquidity_conditions']}")
    if macro.get('market_breadth'):
        extra_lines.append(f"- 시장 건전성: {macro['market_breadth']}")
    if macro.get('earnings_revision'):
        extra_lines.append(f"- 이익 추정치: {macro['earnings_revision']}")
    if macro.get('credit_cycle_stage'):
        extra_lines.append(f"- 신용 사이클: {macro['credit_cycle_stage']}")
    if extra_lines:
        lines.append("\n### 추가 컨텍스트")
        lines.extend(extra_lines)

    favorable = macro.get('favorable_sectors', [])
    unfavorable = macro.get('unfavorable_sectors', [])
    if favorable or unfavorable:
        lines.append("\n### 매크로 기반 포트폴리오 방향")
        if favorable:
            lines.append(f"- 유리 섹터: {', '.join(favorable)}")
        if unfavorable:
            lines.append(f"- 불리 섹터: {', '.join(unfavorable)}")

    return "\n".join(lines) + "\n"


def generate_portfolio_report(portfolio_items: list[dict],
                              stock_data: dict[str, dict],
                              ai_reports: dict[str, dict],
                              watchlist_data: dict[str, dict] | None = None,
                              correlation_data: dict | None = None,
                              cash_balance: float = 0,
                              macro_context: dict | None = None) -> dict:
    """
    포트폴리오 전체 분석 보고서 생성 (Claude API).

    Args:
        portfolio_items: api_portfolio() 결과의 items 리스트
        stock_data: {종목코드: dashboard_result row dict}
        ai_reports: {종목코드: scores dict (parsed JSON)}
        watchlist_data: {종목코드: dashboard_result row dict} — 관심종목 데이터 (옵션)
        macro_context: 매크로 체크리스트 데이터 dict (옵션). 제공 시 현금 비중·섹터 배분에 반영.
            키: growth, real_rate, financial_conditions, environment, usd_krw,
                commodities, credit_spread, capex_theme, china, semiconductor,
                favorable_sectors (list), unfavorable_sectors (list)

    Returns:
        { "scores": {...}, "report_html": "...", "model": "...",
          "generated_date": "...", "mode": "claude" }
    """
    if not config.ANTHROPIC_API_KEY:
        raise ValueError("ANTHROPIC_API_KEY 환경변수가 설정되지 않았습니다.")

    # 종목별 섹션 빌드
    per_stock_parts = []
    for item in portfolio_items:
        code = item["종목코드"]
        stock = stock_data.get(code)
        ai = ai_reports.get(code)
        if stock:
            per_stock_parts.append(
                format_portfolio_stock(stock, item, ai)
            )
        else:
            # ETF/우선주 등 — 기본 정보만
            per_stock_parts.append(
                format_portfolio_stock({}, item, ai)
            )

    # 관심종목 섹션 빌드
    watchlist_section = ""
    if watchlist_data:
        wl_lines = ["\n## 관심종목 (워치리스트) 데이터\n"]
        wl_lines.append("포트폴리오에 없는 관심종목입니다. 편입 여부를 검토해주세요.\n")
        for code, stock in watchlist_data.items():
            name = stock.get("종목명", code)
            wl_lines.append(f"\n### [관심] {name} ({code})")
            wl_lines.append(f"- 섹터: {stock.get('섹터') or 'N/A'}")
            wl_lines.append(format_portfolio_quant_compact(stock, has_ai_report=False, is_watchlist=True))
        watchlist_section = "\n".join(wl_lines) + "\n"

    # 요약 통계
    total_eval = sum(i.get("평가금액", 0) or 0 for i in portfolio_items)
    total_buy = sum(i.get("매입금액", 0) or 0 for i in portfolio_items)
    total_return = ((total_eval / total_buy - 1) * 100) if total_buy else 0

    sector_map: dict[str, float] = {}
    for i in portfolio_items:
        s = i.get("섹터") or "기타"
        sector_map[s] = sector_map.get(s, 0) + (i.get("평가금액", 0) or 0)
    sector_dist = ", ".join(
        f"{k} {v / total_eval * 100:.1f}%" for k, v in
        sorted(sector_map.items(), key=lambda x: -x[1])
    ) if total_eval else "N/A"

    # 상관관계 섹션 빌드
    correlation_section = ""
    if correlation_data and correlation_data.get("matrix"):
        codes_list = correlation_data["codes"]
        names_list = correlation_data["names"]
        matrix = correlation_data["matrix"]
        # 상삼각 페어만 출력 (N×N → N*(N-1)/2 셀로 압축)
        corr_lines = ["\n## 종목 간 상관관계 (최근 250 거래일, 상삼각 페어)\n"]
        pairs = []
        for i in range(len(names_list)):
            for j in range(i + 1, len(names_list)):
                v = matrix[i][j]
                val_str = f"{v:.2f}" if v is not None else "N/A"
                pairs.append(f"{names_list[i]}-{names_list[j]}: {val_str}")
        # 4개씩 한 줄로 묶어 출력
        for k in range(0, len(pairs), 4):
            corr_lines.append(", ".join(pairs[k:k + 4]))
        corr_lines.append("※ ≥0.7: 고상관, ≤-0.3: 역상관")
        correlation_section = "\n".join(corr_lines) + "\n"

    total_assets = total_eval + cash_balance
    cash_weight_str = (
        f"{cash_balance / total_assets * 100:.1f}%" if total_assets > 0 else "0%"
    )

    # 매크로 컨텍스트 섹션 빌드
    if macro_context:
        macro_section = format_macro_context(macro_context)
    else:
        macro_section = (
            "\n## 거시경제 환경\n\n"
            "별도 AI 매크로 분석 결과가 제공되지 않았습니다. "
            "현재 한국 거시경제 환경(경기성장, 금리, 환율, 원자재, 신용스프레드, "
            "중국경기, 반도체사이클 등)을 AI가 자체 판단하여 macro_assessment를 작성해주세요.\n"
        )

    user_prompt = PORTFOLIO_USER_PROMPT_TEMPLATE.format(
        stock_count=len(portfolio_items),
        total_eval=f"{round(total_eval):,}",
        cash_balance=f"{round(cash_balance):,}",
        cash_weight=cash_weight_str,
        total_assets=f"{round(total_assets):,}",
        total_return=f"{total_return:.2f}",
        sector_distribution=sector_dist,
        macro_section=macro_section,
        per_stock_sections="\n".join(per_stock_parts),
        watchlist_section=watchlist_section,
        correlation_section=correlation_section,
    )

    client = anthropic.Anthropic(
        api_key=config.ANTHROPIC_API_KEY,
        timeout=httpx.Timeout(600.0, connect=30.0),
    )

    log.info("포트폴리오 AI 분석 시작 (model=%s, web_search=True)", config.PORTFOLIO_MODEL)
    # web_search_20250305: server-side tool — 최신 뉴스/업황/매크로 조회
    message = _call_with_retry(
        client, model=config.PORTFOLIO_MODEL, max_tokens=32768,
        system=PORTFOLIO_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_prompt}],
        tools=[{"type": "web_search_20250305", "name": "web_search", "max_uses": 2}],
    )
    block_types = [getattr(b, "type", "?") for b in message.content]
    log.info("포트폴리오 AI 분석 완료 (stop_reason=%s, blocks=%s, usage=%s)",
             message.stop_reason, block_types, message.usage)

    # content 배열에서 text 블록만 추출 (web_search_tool_result 등 다른 블록 무시)
    raw_text = ""
    for block in message.content:
        if hasattr(block, "type") and block.type == "text":
            raw_text += block.text
    raw_text = raw_text.strip()

    if not raw_text:
        log.error("포트폴리오 분석: text 블록 없음 (stop_reason=%s, blocks=%s)",
                  message.stop_reason, block_types)
        return {
            "scores": {},
            "report_html": "<p>오류: 모델 응답에 텍스트가 없습니다. 잠시 후 다시 시도해주세요.</p>",
            "error": f"빈 응답 (stop_reason={message.stop_reason})",
            "model": config.PORTFOLIO_MODEL,
            "generated_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "mode": "claude",
        }

    if message.stop_reason == "max_tokens":
        log.warning("포트폴리오 분석: max_tokens 도달로 응답이 잘렸을 수 있음 (len=%d)", len(raw_text))

    try:
        scores = _parse_json_response(raw_text)
    except json.JSONDecodeError as e:
        log.error("포트폴리오 분석 JSON 파싱 실패: %s", str(e)[:100])
        log.debug("시도한 JSON (첫 500자): %s", raw_text[:500])
        log.debug("시도한 JSON (끝 200자): %s", raw_text[-200:])
        return {
            "scores": {},
            "report_html": "<p>오류: JSON 파싱 실패</p>",
            "error": str(e),
            "model": config.PORTFOLIO_MODEL,
            "generated_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "mode": "claude",
        }

    generated_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    model_label = f"Claude ({config.PORTFOLIO_MODEL})"
    try:
        report_html = render_portfolio_html(scores, portfolio_items,
                                            generated_date, model_label,
                                            correlation_data=correlation_data)
    except Exception as e:
        log.exception("포트폴리오 HTML 렌더링 실패 — raw scores 반환")
        report_html = (
            f'<div class="alert alert-warning">'
            f'<strong>HTML 렌더링 중 오류 발생:</strong> {str(e)}<br>'
            f'분석 자체는 완료되었습니다. 아래 원본 JSON 데이터를 확인하세요.</div>'
            f'<pre style="max-height:400px;overflow:auto;">{json.dumps(scores, ensure_ascii=False, indent=2)}</pre>'
        )

    return {
        "scores": scores,
        "report_html": report_html,
        "model": config.PORTFOLIO_MODEL,
        "generated_date": generated_date,
        "mode": "claude",
    }


def render_portfolio_html(scores: dict, portfolio_items: list[dict],
                          generated_date: str, model_label: str,
                          correlation_data: dict | None = None) -> str:
    """포트폴리오 분석 결과를 HTML 보고서로 렌더링."""

    health = scores.get("portfolio_health", {})
    macro_assessment = scores.get("macro_assessment") or {}
    stock_actions = scores.get("stock_actions", [])
    sector_analysis = scores.get("sector_analysis", {})
    risks = scores.get("portfolio_risks", [])
    catalysts = scores.get("portfolio_catalysts", [])
    missing = scores.get("missing_themes", [])
    summary = scores.get("summary", "")

    grade = health.get("grade", "N/A")
    score = int(float(health.get("score", 0) or 0))
    grade_color = _grade_color(grade)

    # 액션 뱃지 컬러/라벨 매핑
    action_colors = {
        "BUY_MORE": "#1e8449",
        "HOLD": "#2c3e50",
        "TRIM": "#b9770e",
        "SELL": "#c0392b",
    }
    action_labels = {
        "BUY_MORE": "추가매수",
        "HOLD": "보유유지",
        "TRIM": "비중축소",
        "SELL": "매도",
    }

    # 종목별 액션 테이블
    action_rows = ""
    for sa in stock_actions:
        action = sa.get("action", "HOLD")
        color = action_colors.get(action, "#6c757d")
        label = action_labels.get(action, action)
        cur_w = float(sa.get("current_weight", 0) or 0)
        rec_w = float(sa.get("recommended_weight", 0) or 0)
        weight_diff = rec_w - cur_w
        diff_str = f"+{weight_diff:.1f}%" if weight_diff > 0 else f"{weight_diff:.1f}%"
        diff_cls = "val-pos" if weight_diff > 0 else ("val-neg" if weight_diff < 0 else "")
        # 구체적 매매 수량/가격 (새 필드, 하위 호환)
        target_shares = int(float(sa.get("target_shares", 0) or 0))
        price_low = float(sa.get("target_price_low", 0) or 0)
        price_high = float(sa.get("target_price_high", 0) or 0)
        est_amount = float(sa.get("estimated_amount", 0) or 0)
        target_shares_str = f"{target_shares:,}주" if target_shares else "-"
        price_range_str = f"{price_low:,.0f}~{price_high:,.0f}원" if price_low and price_high else ("-" if not price_low else f"{price_low:,.0f}원~")
        est_amount_str = f"{est_amount:,.0f}원" if est_amount else "-"
        action_rows += f"""
        <tr>
          <td><strong>{sa.get("name", "")}</strong>
            <span class="text-muted small">{sa.get("code", "")}</span></td>
          <td><span class="pf-action-badge" style="background:{color};">{label}</span></td>
          <td>{cur_w:.1f}%</td>
          <td>{rec_w:.1f}%</td>
          <td class="{diff_cls}">{diff_str}</td>
          <td class="text-end">{target_shares_str}</td>
          <td class="text-end small">{price_range_str}</td>
          <td class="text-end">{est_amount_str}</td>
          <td class="small">{sa.get("rationale", "")}</td>
        </tr>"""

    # 리스크 아이템
    severity_colors = {"high": "#c0392b", "medium": "#b9770e", "low": "#2c3e50"}
    risk_html = ""
    for r in risks:
        sev = r.get("severity", "medium")
        sev_color = severity_colors.get(sev, "#6c757d")
        affected = ", ".join(str(s) for s in r.get("affected_stocks", []))
        risk_html += f"""
        <div class="pf-risk-item">
          <span class="pf-severity-badge" style="background:{sev_color};">
            {sev.upper()}</span>
          <strong>{r.get("risk", "")}</strong>
          <span class="text-muted small ms-2">관련: {affected}</span>
        </div>"""

    # 촉매 아이템
    catalyst_html = ""
    for c in catalysts:
        imp = c.get("impact", "medium")
        imp_color = severity_colors.get(imp, "#6c757d")
        benefiting = ", ".join(str(s) for s in c.get("benefiting_stocks", []))
        catalyst_html += f"""
        <div class="pf-catalyst-item">
          <span class="pf-severity-badge" style="background:{imp_color};">
            {imp.upper()}</span>
          <strong>{c.get("catalyst", "")}</strong>
          <span class="text-muted small ms-2">수혜: {benefiting}</span>
        </div>"""

    # 보완 테마
    theme_html = ""
    for t in missing:
        theme_html += f"""
        <div class="pf-theme-item">
          <strong>{t.get("theme", "")}</strong>
          <div class="small text-muted">{t.get("reason", "")}</div>
        </div>"""

    # 관심종목 편입 권고 테이블
    wl_recommendations = scores.get("watchlist_recommendations", [])
    wl_action_colors = {
        "ADD": "#1e8449", "WATCH": "#1a5276", "SKIP": "#6c757d",
    }
    wl_action_labels = {
        "ADD": "편입 권고", "WATCH": "관찰 지속", "SKIP": "제외",
    }
    watchlist_rows = ""
    for wr in wl_recommendations:
        wl_act = wr.get("action", "WATCH")
        wl_color = wl_action_colors.get(wl_act, "#6c757d")
        wl_label = wl_action_labels.get(wl_act, wl_act)
        wl_rec_w = float(wr.get("recommended_weight", 0) or 0)
        wl_shares = int(float(wr.get("target_shares", 0) or 0))
        wl_pl = float(wr.get("target_price_low", 0) or 0)
        wl_ph = float(wr.get("target_price_high", 0) or 0)
        wl_amt = float(wr.get("estimated_amount", 0) or 0)
        wl_shares_str = f"{wl_shares:,}주" if wl_shares else "-"
        wl_price_str = f"{wl_pl:,.0f}~{wl_ph:,.0f}원" if wl_pl and wl_ph else "-"
        wl_amt_str = f"{wl_amt:,.0f}원" if wl_amt else "-"
        watchlist_rows += f"""
        <tr>
          <td><strong>{wr.get("name", "")}</strong>
            <span class="text-muted small">{wr.get("code", "")}</span></td>
          <td><span class="pf-action-badge" style="background:{wl_color};">{wl_label}</span></td>
          <td>{wl_rec_w:.1f}%</td>
          <td class="text-end">{wl_shares_str}</td>
          <td class="text-end small">{wl_price_str}</td>
          <td class="text-end">{wl_amt_str}</td>
          <td class="small">{wr.get("rationale", "")}</td>
          <td class="small text-muted">{wr.get("synergy", "")}</td>
        </tr>"""
    watchlist_card = ""
    if wl_recommendations:
        watchlist_card = f"""
  <div class="stage-card">
    <div class="stage-card-title">관심종목 편입 권고</div>
    <div class="table-responsive">
      <table class="table table-sm table-hover mb-0">
        <thead><tr>
          <th>종목</th><th>판정</th><th>권장비중</th><th>매수수량</th><th>목표가</th><th>예상금액</th><th>근거</th><th>시너지</th>
        </tr></thead>
        <tbody>{watchlist_rows}</tbody>
      </table>
    </div>
  </div>"""

    # 리밸런싱 실행 계획
    rebal = scores.get("rebalancing_plan", {})
    urgency_map = {
        "immediate": ("즉시 실행", "#c0392b"),
        "monthly": ("이번 달 내", "#b9770e"),
        "quarterly": ("이번 분기 내", "#2c3e50"),
        "none": ("불필요", "#6c757d"),
    }
    urgency_val = rebal.get("urgency", "none")
    urgency_label, urgency_color = urgency_map.get(urgency_val, ("알 수 없음", "#6c757d"))
    priority_items = "".join(
        f'<li>{a}</li>' for a in rebal.get("priority_actions", [])
    )
    rebal_card = f"""
  <div class="stage-card">
    <div class="stage-card-title">리밸런싱 실행 계획</div>
    <div class="stage-field">
      <strong>긴급도:</strong>
      <span class="pf-severity-badge ms-1" style="background:{urgency_color};">{urgency_label}</span>
    </div>
    {f'<div class="stage-field"><strong>우선 실행 항목:</strong><ul class="mb-1 mt-1">{priority_items}</ul></div>' if priority_items else ''}
    <div class="stage-analysis">{rebal.get("execution_note", "")}</div>
  </div>""" if rebal else ""

    # 배당 분석
    div_analysis = scores.get("dividend_analysis", {})
    div_yield = float(div_analysis.get("portfolio_yield", 0) or 0)
    div_annual = float(div_analysis.get("annual_dividend_estimate", 0) or 0)
    dividend_card = f"""
  <div class="stage-card">
    <div class="stage-card-title">배당 분석</div>
    <div class="stage-field"><strong>포트폴리오 배당수익률:</strong> {div_yield:.2f}%</div>
    <div class="stage-field"><strong>연간 예상 배당금:</strong> {div_annual:,.0f}원</div>
    <div class="stage-field"><strong>배당 추세:</strong> {div_analysis.get("dividend_growth_trend", "N/A")}</div>
    <div class="stage-analysis">{div_analysis.get("suggestion", "")}</div>
  </div>""" if div_analysis else ""

    # 상관관계 히트맵 테이블
    correlation_card = ""
    if correlation_data and correlation_data.get("matrix"):
        corr_names = correlation_data.get("names", [])
        corr_matrix = correlation_data.get("matrix", [])

        def _corr_cell_style(val, i, j):
            if i == j:
                return "background:#d0d0d0; color:#333;"
            if val is None:
                return "background:#f8f9fa;"
            try:
                v = float(val)
            except (TypeError, ValueError):
                return "background:#f8f9fa;"
            if v >= 0.7:
                r, g, b = 220, 50, 50
            elif v >= 0.3:
                r, g, b = 240, 180, 60
            elif v <= -0.3:
                r, g, b = 60, 120, 220
            else:
                r, g, b = 248, 249, 250
            return f"background:rgb({r},{g},{b}); color:{'#fff' if v >= 0.7 or v <= -0.3 else '#222'};"

        header_cells = "".join(
            f'<th class="text-center small">{n}</th>' for n in corr_names
        )
        corr_rows_html = ""
        for i, name in enumerate(corr_names):
            if i >= len(corr_matrix):
                break
            cells = ""
            for j, val in enumerate(corr_matrix[i]):
                style = _corr_cell_style(val, i, j)
                try:
                    display = "1.00" if i == j else (f"{float(val):.2f}" if val is not None else "N/A")
                except (TypeError, ValueError):
                    display = "N/A"
                cells += f'<td class="text-center small" style="{style}">{display}</td>'
            corr_rows_html += f'<tr><th class="small">{name}</th>{cells}</tr>'

        correlation_card = f"""
  <div class="stage-card">
    <div class="stage-card-title">종목 간 가격 상관관계 (최근 250 거래일)</div>
    <div class="table-responsive">
      <table class="table table-sm table-bordered mb-0" style="width:auto;">
        <thead><tr><th></th>{header_cells}</tr></thead>
        <tbody>{corr_rows_html}</tbody>
      </table>
    </div>
    <div class="text-muted small mt-2">
      <span style="background:rgb(220,50,50);color:#fff;padding:2px 6px;border-radius:3px;">0.7+</span> 고상관 &nbsp;
      <span style="background:rgb(240,180,60);padding:2px 6px;border-radius:3px;">0.3~0.7</span> 중간 &nbsp;
      <span style="background:#f8f9fa;border:1px solid #dee2e6;padding:2px 6px;border-radius:3px;">-0.3~0.3</span> 낮음 &nbsp;
      <span style="background:rgb(60,120,220);color:#fff;padding:2px 6px;border-radius:3px;">-0.3↓</span> 역상관
    </div>
  </div>"""

    # 적정 종목수 카드
    opt = scores.get("portfolio_optimization", {})
    optimization_card = ""
    if opt:
        cur_cnt = opt.get("current_count", len(portfolio_items))
        rec_min = opt.get("recommended_count_min", 0)
        rec_max = opt.get("recommended_count_max", 0)
        strategy_labels = {
            "balanced": ("균형형", "#2c3e50"),
            "concentrated": ("집중형", "#c0392b"),
            "diversified": ("분산형", "#1a5276"),
        }
        s_type = opt.get("strategy_type", "balanced")
        s_label, s_color = strategy_labels.get(s_type, ("균형형", "#2c3e50"))
        rec_range = f"{rec_min}~{rec_max}종목" if rec_min and rec_max else "N/A"
        optimization_card = f"""
  <div class="stage-card">
    <div class="stage-card-title">적정 종목수 &amp; 구성 최적화</div>
    <div class="stage-field">
      <strong>현재 종목수:</strong> {cur_cnt}종목 &nbsp;
      <strong>권장 범위:</strong> {rec_range} &nbsp;
      <span class="pf-action-badge ms-1" style="background:{s_color};">{s_label}</span>
    </div>
    <div class="stage-field">{opt.get("assessment", "")}</div>
    <div class="stage-analysis">{opt.get("adjustment_suggestion", "")}</div>
  </div>"""

    # 매크로 환경 평가 카드
    macro_card = ""
    if macro_assessment:
        env = macro_assessment.get("environment", "")
        env_colors = {"공격적": ("#d5f5e3", "#1e8449"), "중립": ("#fef9e7", "#b9770e"), "방어적": ("#fadbd8", "#c0392b")}
        env_bg, env_fg = env_colors.get(env, ("#e9ecef", "#6c757d"))
        cash = macro_assessment.get("cash_signal", "")
        cash_colors = {"축소": ("#d5f5e3", "#1e8449"), "유지": ("#fef9e7", "#b9770e"), "확대": ("#fadbd8", "#c0392b")}
        cash_bg, cash_fg = cash_colors.get(cash, ("#e9ecef", "#6c757d"))
        fav_sectors = macro_assessment.get("favorable_sectors", [])
        unfav_sectors = macro_assessment.get("unfavorable_sectors", [])
        fav_chips = " ".join(f'<span class="macro-sector-chip favorable">{s}</span>' for s in fav_sectors) if fav_sectors else '<span class="text-muted small">없음</span>'
        unfav_chips = " ".join(f'<span class="macro-sector-chip unfavorable">{s}</span>' for s in unfav_sectors) if unfav_sectors else '<span class="text-muted small">없음</span>'
        key_risks = macro_assessment.get("key_risks", "")
        alignment = macro_assessment.get("portfolio_alignment", "")
        macro_card = f"""\
  <div class="stage-card macro-assessment-card">
    <div class="stage-card-title">거시경제 환경 평가</div>
    <div style="display:flex;gap:12px;flex-wrap:wrap;margin-bottom:10px;">
      <span class="macro-env-badge" style="background:{env_bg};color:{env_fg};">환경: {env or 'N/A'}</span>
      <span class="macro-cash-badge" style="background:{cash_bg};color:{cash_fg};">현금비중 신호: {cash or 'N/A'}</span>
    </div>
    <div class="stage-field"><strong>유리 섹터:</strong> {fav_chips}</div>
    <div class="stage-field"><strong>불리 섹터:</strong> {unfav_chips}</div>
    {f'<div class="stage-field" style="margin-top:8px;"><strong>매크로 리스크:</strong> {key_risks}</div>' if key_risks else ''}
    {f'<div class="stage-analysis">{alignment}</div>' if alignment else ''}
  </div>"""

    # 역발상 점검 카드
    pb = scores.get("perspective_balance", {})
    perspective_card = ""
    if pb:
        tilt = pb.get("growth_vs_value_tilt", "")
        tilt_colors = {
            "성장 편향": ("#fadbd8", "#c0392b"),
            "균형": ("#d5f5e3", "#1e8449"),
            "가치 편향": ("#d6eaf8", "#1a5276"),
        }
        tilt_bg, tilt_fg = tilt_colors.get(tilt, ("#e9ecef", "#6c757d"))
        val_risk = pb.get("valuation_risk_level", "")
        val_risk_colors = {"low": "#1e8449", "medium": "#b9770e", "high": "#c0392b"}
        val_risk_color = val_risk_colors.get(val_risk, "#6c757d")
        contrarian = pb.get("contrarian_opportunities", [])
        contrarian_chips = " ".join(
            f'<span class="badge bg-info me-1">{s}</span>' for s in contrarian
        ) if contrarian else '<span class="text-muted small">없음</span>'
        consensus_risk = pb.get("consensus_risk", "")
        bear_case = pb.get("bear_case_summary", "")
        max_dd = pb.get("estimated_max_drawdown", "")
        perspective_card = f"""\
  <div class="stage-card" style="border-left:4px solid #c0392b;">
    <div class="stage-card-title">역발상 점검 &amp; 편향 균형 (Devil&#39;s Advocate)</div>
    <div style="display:flex;gap:12px;flex-wrap:wrap;margin-bottom:10px;">
      {f'<span class="macro-env-badge" style="background:{tilt_bg};color:{tilt_fg};">투자 성향: {tilt}</span>' if tilt else ''}
      {f'<span class="pf-action-badge" style="background:{val_risk_color};">밸류에이션 리스크: {val_risk.upper()}</span>' if val_risk else ''}
      {f'<span class="pf-action-badge" style="background:#6c757d;">추정 최대 낙폭: {max_dd}</span>' if max_dd else ''}
    </div>
    <div class="stage-field"><strong>역발상 기회 종목:</strong> {contrarian_chips}</div>
    {f'<div class="stage-field"><strong>컨센서스 동조 위험:</strong> {consensus_risk}</div>' if consensus_risk else ''}
    {f'<div class="stage-analysis" style="background:#fdf2f2;border-left:3px solid #c0392b;padding:10px;margin-top:8px;"><strong style="color:#c0392b;">약세 시나리오 (Bear Case):</strong><br>{bear_case}</div>' if bear_case else ''}
  </div>"""

    # 과비중/과소비중 섹터 뱃지
    overweight = sector_analysis.get("overweight_sectors", [])
    underweight = sector_analysis.get("underweight_sectors", [])
    ow_badges = " ".join(
        f'<span class="badge bg-danger me-1">{s}</span>' for s in overweight
    ) if overweight else '<span class="text-muted small">없음</span>'
    uw_badges = " ".join(
        f'<span class="badge bg-info me-1">{s}</span>' for s in underweight
    ) if underweight else '<span class="text-muted small">없음</span>'

    return f"""\
<div class="analysis-report portfolio-analysis">
  <div class="report-header">
    <div class="stock-identity">
      <h2 class="stock-name">포트폴리오 종합 분석</h2>
      <span class="stock-code">{len(portfolio_items)}종목 보유</span>
    </div>
    <div class="composite-section">
      <div class="composite-grade" style="background: {grade_color};">{grade}</div>
      <div class="composite-score-wrap">
        <div class="composite-label">포트폴리오 건강도</div>
        <div class="composite-num">{score}<span class="composite-max">/100</span></div>
      </div>
    </div>
  </div>

  <div class="summary-box">
    <h4>포트폴리오 종합 의견</h4>
    <p>{summary}</p>
  </div>

  <div class="pf-charts-row" style="display:flex;gap:16px;flex-wrap:wrap;margin-bottom:16px;">
    <div class="stage-card" style="flex:1;min-width:280px;">
      <div class="stage-card-title">종목별 수익률</div>
      <canvas id="pf-return-chart" style="max-height:220px;"></canvas>
    </div>
    <div class="stage-card" style="flex:1;min-width:260px;">
      <div class="stage-card-title">섹터 비중</div>
      <canvas id="pf-sector-chart" style="max-height:220px;"></canvas>
    </div>
  </div>

  <div class="stage-card">
    <div class="stage-card-title">포트폴리오 건강도 상세</div>
    <div class="stage-field"><strong>분산도:</strong> {health.get("diversification", "N/A")}</div>
    <div class="stage-field"><strong>밸류에이션:</strong> {health.get("valuation", "N/A")}</div>
    <div class="stage-field"><strong>성장성/품질:</strong> {health.get("growth_quality", "N/A")}</div>
    <div class="stage-analysis">{health.get("overall_assessment", "")}</div>
  </div>

  {macro_card}

  {perspective_card}

  {optimization_card}

  {rebal_card}

  <div class="stage-card">
    <div class="stage-card-title">종목별 액션 권고</div>
    <div class="table-responsive">
      <table class="table table-sm table-hover mb-0">
        <thead><tr>
          <th>종목</th><th>액션</th><th>현재비중</th><th>권장비중</th><th>변경</th>
          <th>매매수량</th><th>목표가</th><th>예상금액</th><th>근거</th>
        </tr></thead>
        <tbody>{action_rows}</tbody>
      </table>
    </div>
  </div>

  {watchlist_card}

  <div class="stage-card">
    <div class="stage-card-title">섹터 집중도 분석</div>
    <div class="stage-field"><strong>집중 리스크:</strong>
      {sector_analysis.get("concentration_risk", "N/A")}</div>
    <div class="stage-field"><strong>과비중 섹터:</strong> {ow_badges}</div>
    <div class="stage-field"><strong>과소비중 섹터:</strong> {uw_badges}</div>
    <div class="stage-analysis">{sector_analysis.get("rebalancing_suggestion", "")}</div>
  </div>

  {correlation_card}

  <div class="risk-catalyst-grid">
    <div class="risk-section">
      <h4>포트폴리오 리스크</h4>
      {risk_html if risk_html else '<p class="text-muted small">식별된 리스크 없음</p>'}
    </div>
    <div class="catalyst-section">
      <h4>포트폴리오 촉매</h4>
      {catalyst_html if catalyst_html else '<p class="text-muted small">식별된 촉매 없음</p>'}
    </div>
  </div>

  <div class="stage-card">
    <div class="stage-card-title">보완이 필요한 테마/섹터</div>
    {theme_html if theme_html else '<p class="text-muted small">현재 포트폴리오 구성이 적절합니다.</p>'}
  </div>

  {dividend_card}

  <div class="report-footer">
    <span>Generated by {model_label} &mdash; Portfolio Advisor</span>
    <span>{generated_date}</span>
  </div>
</div>"""


# ─────────────────────────────────────────
# 보고서 변경점 요약 (이전 vs 현재)
# ─────────────────────────────────────────

def _compare_master_scores(old_masters: dict, new_masters: dict) -> list[str]:
    """거장별 점수 변화를 비교하여 텍스트 리스트로 반환."""
    changes = []
    for key, info in MASTER_INFO.items():
        old_s = old_masters.get(key, {}).get("score", 0)
        new_s = new_masters.get(key, {}).get("score", 0)
        if old_s != new_s:
            arrow = "▲" if new_s > old_s else "▼"
            diff = new_s - old_s
            changes.append(f"{info['name']}: {old_s}→{new_s} ({arrow}{abs(diff):+.1f})")
    return changes


def generate_diff_summary(old_scores_json: str, new_scores: dict) -> str:
    """이전 보고서와 현재 보고서의 scores를 비교하여 HTML diff 요약을 생성."""
    try:
        old_scores = json.loads(old_scores_json) if isinstance(old_scores_json, str) else (old_scores_json or {})
    except (json.JSONDecodeError, TypeError):
        return ""

    if not old_scores or not new_scores:
        return ""

    sections = []

    # 1. 종합 점수 / 등급 변화
    old_composite = old_scores.get("composite_score", 0)
    new_composite = new_scores.get("composite_score", 0)
    old_grade = old_scores.get("investment_grade", "N/A")
    new_grade = new_scores.get("investment_grade", "N/A")

    if old_composite != new_composite or old_grade != new_grade:
        arrow = "▲" if new_composite > old_composite else "▼" if new_composite < old_composite else "→"
        grade_change = f' (등급: {old_grade}→{new_grade})' if old_grade != new_grade else ""
        sections.append(
            f'<div class="diff-item diff-grade">'
            f'<strong>종합점수:</strong> {old_composite}점 → {new_composite}점 {arrow}{grade_change}'
            f'</div>'
        )

    # 2. 거장별 점수 변화
    old_masters = old_scores.get("stage7_masters", {})
    new_masters = new_scores.get("stage7_masters", {})
    master_changes = _compare_master_scores(old_masters, new_masters)
    if master_changes:
        items = "".join(f"<li>{c}</li>" for c in master_changes)
        sections.append(
            f'<div class="diff-item"><strong>거장별 점수 변화:</strong>'
            f'<ul class="mb-0">{items}</ul></div>'
        )

    # 3. 액션 변화 (매수/매도 의견)
    old_action = old_scores.get("stage8_action", {}).get("recommendation", "")
    new_action = new_scores.get("stage8_action", {}).get("recommendation", "")
    if old_action and new_action and old_action != new_action:
        sections.append(
            f'<div class="diff-item diff-action">'
            f'<strong>투자의견 변경:</strong> {old_action} → {new_action}'
            f'</div>'
        )

    # 4. 목표가 변화
    old_target = old_scores.get("stage6_valuation", {}).get("target_price")
    new_target = new_scores.get("stage6_valuation", {}).get("target_price")
    if old_target and new_target and old_target != new_target:
        try:
            old_tp = int(old_target)
            new_tp = int(new_target)
            pct = (new_tp - old_tp) / old_tp * 100 if old_tp else 0
            arrow = "▲" if pct > 0 else "▼"
            sections.append(
                f'<div class="diff-item">'
                f'<strong>목표가:</strong> {old_tp:,}원 → {new_tp:,}원 ({arrow}{abs(pct):.1f}%)'
                f'</div>'
            )
        except (ValueError, TypeError):
            pass

    # 5. 리스크/촉매 변화 (risks가 dict 또는 str 모두 지원)
    def _risk_key(r):
        if isinstance(r, dict):
            return r.get("description", str(r))
        return str(r)
    old_risks = set(_risk_key(r) for r in old_scores.get("risks", []))
    new_risks = set(_risk_key(r) for r in new_scores.get("risks", []))
    added_risks = new_risks - old_risks
    removed_risks = old_risks - new_risks
    if added_risks:
        items = "".join(f"<li>+ {r}</li>" for r in list(added_risks)[:3])
        sections.append(f'<div class="diff-item"><strong>새로운 리스크:</strong><ul class="mb-0">{items}</ul></div>')
    if removed_risks:
        items = "".join(f"<li>- {r}</li>" for r in list(removed_risks)[:3])
        sections.append(f'<div class="diff-item"><strong>해소된 리스크:</strong><ul class="mb-0">{items}</ul></div>')

    if not sections:
        return '<div class="diff-summary"><p class="text-muted mb-0">이전 보고서 대비 주요 변경사항이 없습니다.</p></div>'

    content = "\n".join(sections)
    return f'<div class="diff-summary">{content}</div>'
