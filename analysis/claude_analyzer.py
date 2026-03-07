"""
AI 종목 정성 분석 보고서 생성기 (Grand Master Protocol v2).

6대 투자 거장(Warren Buffett, Aswath Damodaran, Philip Fisher,
Pat Dorsey, Peter Lynch, André Kostolany)의 핵심 철학을 기반으로
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
ANALYSIS_INPUT_VERSION = "stock-analysis-v3"

# ─────────────────────────────────────────
# 프롬프트 템플릿
# ─────────────────────────────────────────

SYSTEM_PROMPT = """\
당신은 한국 주식시장 Grand Master 애널리스트입니다.
6대 투자 거장의 철학을 통합한 9단계 심층 분석 프로토콜(Stage 0~8)을 수행합니다.

[환각 방지 - 최우선] 종목명만 보고 업종을 단정짓지 마세요. 불확실하면 hallucination_flag=true.

## 분석 단계 요약
- Stage 0: 핵심 사업 모델 (무엇을 팔아 돈을 버는가? 3줄, 핵심 제품명·매출비중 필수)
- Stage 1: 거시환경 & 밸류체인 (전방산업 CAGR, 경쟁우위)
- Stage 2: 수익성 해부 (P×Q×C 분석, 캐시카우 vs 성장동력 구분)
- Stage 3: 수명주기 & 해자 (도입/성장/성숙/쇠퇴, 4대 해자 데이터로 입증)
- Stage 4: 재무건전성 (매출총이익률 추이, FCF 품질, 부채비율, 컨센서스 괴리)
- Stage 4.5: Peer 비교 → 아래 별도 규칙 참조
- Stage 5: 전망 & 모멘텀 (CAPEX, 수주잔고, 신사업, 12개월 촉매)
- Stage 6: 밸류에이션 & 코스톨라니 (수명주기 맞춤 방법론, 달걀 1~6 위치)
- Stage 7: 거장 6인 평가 (각 1줄+분석, 통합 S~F 등급)
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

### 6대 거장 핵심 관점 (키워드)
- Buffett: 해자지속성·경영진·S-RIM 안전마진·장기보유
- Damodaran: 내러티브-숫자일관성·ROIC vs WACC·리스크대비보상
- Fisher: R&D혁신·이익률개선추세·장기성장·조직문화
- Dorsey: 4대해자(무형/전환비용/네트워크/원가) 강도·트렌드(확대/축소)
- Lynch: PEG·이해가능사업·10-bagger잠재력·이익↔주가연동
- Kostolany: 달걀위치·역발상·유동성수급·인내심

## Stage 4.5: Peer 비교
- USER_PROMPT에 "동종업계 Peer DB 데이터" 섹션이 있으면 그 값을 peer_comparison.peers에 그대로 사용 (웹 추정 금지)
- DB 데이터 없을 때만 웹 검색으로 보완. 지표별 대상 종목 상대 순위 명시. 저평가/적정/고평가 판정. 더 매력적 대안이 있으면 솔직히 언급.

## 웹 검색 (최대 5회)
1. "{종목명} 실적 공시 뉴스 {연도}" → Stage 1·5에 반영
2. "site:dart.fss.or.kr {종목명}" → DART 공시·위험요인 → Stage 3·4·risks에 반영
3. "{종목명} 증권사 리포트 목표주가 {연도}" → 컨센서스 → Stage 5·8에 반영
4. "{종목명} 주요주주 지분변동 배당 자사주 {연도}" → Stage 5에 반영
5. "{산업명} 시장 전망 동향 {연도}" → Stage 1에 반영
검색 결과는 분석 근거에 구체적으로 인용하세요.

## 근거 인용 규칙 (중요)
- 출처 우선순위: DART/전자공시/회사 IR/실적발표 > 증권사 리포트 > 경제지/통신사 > 일반 기사
- 가능하면 아래 화이트리스트 계열 출처를 우선 사용: DART, 전자공시, 회사 IR/실적발표, 한국경제, 매일경제, 서울경제, 연합뉴스, Reuters, Bloomberg, 주요 증권사 리포트
- 같은 사실을 여러 출처가 다루면 더 상위 출처를 먼저 recent_news에 배치
- recent_news 각 항목은 날짜(date), 출처(source), 핵심 사실 1개 이상을 반드시 포함
- 핵심 사실에는 가능한 한 수치 1개 이상을 포함 (예: 매출, 영업이익, 점유율, 목표주가, 수주금액, CAPEX)
- stage5_outlook.analysis는 3~5문장 모두를 근거 중심으로 작성하고, 최소 2문장 이상에 날짜 또는 출처명을 포함
- stage5_outlook.analysis와 summary의 핵심 주장(실적 개선, 목표주가, 수주, 점유율, 주주환원, 산업 전망)은 가능하면 화이트리스트 출처 근거를 우선 사용
- 일반 기사만 근거일 경우, DART/IR/증권사/주요 경제지로 교차확인되지 않은 핵심 수치는 과장하지 말고 보수적으로 서술
- "전망", "기대", "가능성"만 반복하지 말고, 확인된 사실과 추정/의견을 구분해서 쓰세요
- 출처 없는 업계 루머, 커뮤니티 글, 출처 불명 숫자는 사용 금지

## recent_news (3~5건 필수)
웹 검색에서 투자 판단에 중요한 뉴스·공시를 선별. 각 항목: title, date, summary(1~2문장), impact(긍정/부정/중립), source.

## 내부 일관성 (JSON 출력 전 자체 검증)
1. moat_rating=none → fair_value_range PBR/BPS 보수적 산출, 성장 프리미엄 금지
2. TTM_FCF 음수 또는 부채비율>200% → portfolio_weight 최대 2%
3. lifecycle=쇠퇴기 → Stage 6에서 성장주 PER/PSR 멀티플 금지
4. recent_news impact=부정 → risks에 반드시 반영
5. peer relative_valuation=고평가 → entry_price는 현재가 대비 할인가 필수
6. better_alternative≠null → summary에 대안 종목 언급 필수

## risks 필수 규칙
단순 "경쟁 심화·금리·환율" 기재 금지 — 수치/사건 연결 필수. 재무 약점 1개 이상, severity=high 1개 이상, DART·뉴스 근거 1개 이상.

"""

USER_PROMPT_TEMPLATE = """\
종목: {code} {name} ({market})

## 정량 데이터
{quant_data}
{qualitative_section}
[환각 방지] 위 종목의 실제 핵심 사업을 웹 검색으로 먼저 확인하세요. 금융 데이터만 보고 업종을 추정하지 마세요.
데이터 활용: PEG·매출CAGR·ROE→Lynch | 괴리율·SRIM적정가→Buffett | F스코어→Stage4 | RSI·MA이격도→코스톨라니 | TTM·CAGR→Stage2

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
    "analysis": "<3-5문장>"
  }},
  "stage2_business_model": {{
    "p_times_q_analysis": "<P×Q×C 분석>",
    "cash_cow_drivers": "<캐시카우 사업부>",
    "growth_drivers": "<성장 동력>",
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
    "debt_assessment": "<부채 구조>",
    "consensus_deviation": "<컨센서스 대비>",
    "analysis": "<3-5문장>"
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
    "analysis": "<3-5문장>"
  }},
  "stage5_outlook": {{
    "capex_signals": "<CAPEX 현황>",
    "order_backlog": "<수주잔고>",
    "new_business": "<신사업>",
    "catalysts_12m": ["<촉매1>", "<촉매2>", "<촉매3>"],
    "analysis": "<3-5문장. 최소 2문장 이상은 날짜/출처/수치 포함>"
  }},
  "stage6_valuation": {{
    "lifecycle_matched_method": "<밸류에이션 방법론>",
    "fair_value_range": "<적정 주가 범위>",
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
    "entry_price": "<진입 가격대(원)>",
    "entry_basis": "<산출 근거(SRIM/지지선/MA60 등)>",
    "target_price": "<12개월 목표주가(원)>",
    "target_basis": "<산출 근거(PER/SRIM/컨센서스 등)>",
    "stop_loss": "<손절 기준>",
    "portfolio_weight": "<권장 비중(예: 3-5%)>",
    "holding_period": "<단기3개월|중기6-12개월|장기2-3년>",
    "exit_conditions": ["<매도 조건1>", "<매도 조건2>"],
    "analysis": "<2-3문장>"
  }},
  "summary": "<5-7문장 종합 투자 의견>",
  "recent_news": [
    {{
      "title": "<제목>",
      "date": "<YYYY-MM-DD 또는 추정시기>",
      "summary": "<1-2문장, 가능한 한 핵심 수치 1개 이상 포함>",
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
}


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
    "buffett": 20,
    "damodaran": 15,
    "fisher": 15,
    "dorsey": 15,
    "lynch": 15,
    "kostolany": 10,
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

    total = 0.0
    for key, weight in MASTER_SCORE_WEIGHTS.items():
        try:
            score = float((masters.get(key) or {}).get("score"))
        except (TypeError, ValueError):
            return None, None
        total += score * weight

    composite = total / sum(MASTER_SCORE_WEIGHTS.values()) * 10
    confidence = ((scores.get("business_identity") or {}).get("confidence") or "").lower()
    confidence_adj = {"high": 5, "medium": 0, "low": -10}.get(confidence, 0)
    composite += confidence_adj

    try:
        if float(stock.get("괴리율(%)", 0) or 0) <= -30:
            buffett = masters.get("buffett") or {}
            if "score" in buffett:
                buffett["score"] = min(float(buffett.get("score") or 0), 4)
                masters["buffett"] = buffett
                total = 0.0
                for key, weight in MASTER_SCORE_WEIGHTS.items():
                    total += float((masters.get(key) or {}).get("score") or 0) * weight
                composite = total / sum(MASTER_SCORE_WEIGHTS.values()) * 10 + confidence_adj
    except (TypeError, ValueError):
        pass

    if float(stock.get("F스코어", 0) or 0) <= 3:
        composite = min(composite, 45)

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
        timeout=httpx.Timeout(300.0, connect=30.0),
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

    user_prompt = USER_PROMPT_TEMPLATE.format(
        code=code, name=name, market=market,
        quant_data=quant_text,
        qualitative_section=peer_db_section,
    )

    # --- 메시지 content 구성 ---
    user_content: list = [{"type": "text", "text": user_prompt}]

    # --- Prompt caching: 시스템 프롬프트 캐시 적용 ---
    system_with_cache = [{"type": "text", "text": SYSTEM_PROMPT, "cache_control": {"type": "ephemeral"}}]

    web_search_max_uses = config.WEB_SEARCH_MAX_USES
    log.info("종목 AI 분석 시작 (%s %s, model=%s, web_search max_uses=%d)",
             code, name, config.ANALYSIS_MODEL, web_search_max_uses)

    message = _call_with_retry(
        client,
        use_beta=False,
        model=config.ANALYSIS_MODEL,
        max_tokens=config.ANALYSIS_MAX_TOKENS,
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
    log.info("종목 AI 분석 완료 (%s %s, stop_reason=%s, blocks=%s)",
             code, name, message.stop_reason, block_types)

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
- **TRIM/SELL은 산업 성장성 훼손, 해자 소실, 구조적 경쟁력 약화가 확인된 경우에만 권고**하세요. 단기 고평가(PER 등)는 TRIM 근거가 되지 않습니다.
- 현금(예수금)이 있는 경우, BUY_MORE 권고 시 예수금 범위 내에서 실행 가능한 매수량/금액을 우선 제시하세요.
- 수급(코스톨라니 달걀 위치, 시장심리)은 매수 타이밍 보조 지표로만 활용하고, 장기 성장성 판단을 뒤집어서는 안 됩니다.

## 분석 프레임워크

### 1. 포트폴리오 건강도 평가
- 전체 포트폴리오의 품질 점수 (0-100)
- **산업 성장성, 해자 품질, 밸류에이션, 분산도** 관점에서 종합 평가

### 2. 종목별 액션 권고
- 각 종목에 대해 BUY_MORE / HOLD / TRIM / SELL 중 하나를 권고
- **판단 우선순위**: ① 산업 장기 성장성(전방산업 CAGR) → ② 해자 등급(wide/narrow/none) → ③ 개별 AI 분석(6대 거장) → ④ 현재 비중 → ⑤ 수급/시장심리(달걀모형)
- 현재 비중 vs 권장 비중 제시
- **구체적 매매 수량 산출** (보유수량, 평균매입가, 현재가, 총평가금액 기반):
  - BUY_MORE: 추가 매수할 주수(target_shares), 매수 목표가 범위(target_price_low ~ target_price_high), 예상 매수금액(estimated_amount). 예수금이 있으면 예수금 내 실행 가능한 금액 우선
  - TRIM: 매도할 주수(target_shares), 매도 목표가 범위, 예상 매도금액
  - SELL: 전량 매도 주수(=보유수량), 매도 목표가 범위, 예상 매도금액
  - HOLD: target_shares=0, 가격 범위는 현재가 ±5% 내외 감시 범위
  - target_shares 산출식: abs(총평가금액 × (권장비중 - 현재비중) / 100) ÷ 목표가 중간값, 정수 반올림
  - estimated_amount = target_shares × 목표가 중간값

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
- 포트폴리오 가중평균 배당수익률 추정 (각 종목의 배당수익률 × 비중 합산)
- 연간 예상 배당금 합계 (보유 수량 × 주당 배당금 기반, 데이터 부족 시 업종 평균 참고)
- 배당 성향 추세: 증가/유지/감소/혼재/무배당 다수 중 판정
- 배당 인컴 관점에서 포트폴리오 평가 및 개선 제안

### 7. 보완 제안
- 포트폴리오에 부족한 섹터/테마/스타일 식별
- **장기 성장 산업 중 미편입 섹터**를 우선으로 추가 편입 후보 테마 제안 (종목 추천 아님, 테마/섹터 수준)

### 8. 관심종목 편입 권고 (관심종목 데이터가 있는 경우)
- 관심종목 중 포트폴리오에 추가하면 좋을 종목 식별
- 각 관심종목에 대해 ADD(편입 권고) / WATCH(관찰 지속) / SKIP(제외 권고) 판정
- ADD 종목: 권장 비중, 매수 주수, 목표가 범위, 예상 금액 제시
- 기존 포트폴리오와의 섹터 분산 효과, 산업 성장성 보완, 상관관계 고려

### 9. 리밸런싱 실행 계획
- 가장 시급한 액션 1-3개 우선순위 제시
- 실행 순서 및 권장 시기 (즉시/이번 달/이번 분기/불필요)
- 예수금이 있는 경우: 현금 활용 우선순위 포함

## 분석 지침
- 포트폴리오 비중이 높은 종목에 더 주의를 기울이세요
- 종목 간 상관관계와 중복 리스크를 식별하세요
- 데이터가 없는 종목(ETF, 우선주 등)은 비중 분석에만 포함하세요 (비중=None인 종목은 현재가 없음)
- 6대 투자 거장(Buffett, Damodaran, Fisher, Dorsey, Lynch, Kostolany)의 관점이 기존 분석에 포함되어 있으면 이를 종합적으로 활용하세요
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

    # 정량 데이터 (reuse format_quant_data)
    if stock:
        lines.append(format_quant_data(stock))

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
        # 수급/시장심리
        val = ai_report.get("stage6_valuation", {})
        egg = val.get("kostolany_egg_position")
        psych = val.get("market_psychology")
        if egg or psych:
            lines.append(f"- 달걀모형 위치: {egg}/6, 시장심리: {psych or 'N/A'}")
        # 12개월 촉매
        outlook = ai_report.get("stage5_outlook", {})
        cats_12m = outlook.get("catalysts_12m", [])
        if cats_12m:
            lines.append(f"- 12개월 촉매: {', '.join(str(c) for c in cats_12m[:3])}")
        # 거장 점수 요약
        masters = ai_report.get("stage7_masters", {})
        if masters:
            master_scores = []
            for key, info in MASTER_INFO.items():
                m = masters.get(key, {})
                s = m.get("score", 0)
                master_scores.append(f"{info['name']}: {s}/10")
            lines.append(f"- 거장 점수: {', '.join(master_scores)}")

    return "\n".join(lines)


def generate_portfolio_report(portfolio_items: list[dict],
                              stock_data: dict[str, dict],
                              ai_reports: dict[str, dict],
                              watchlist_data: dict[str, dict] | None = None,
                              correlation_data: dict | None = None,
                              cash_balance: float = 0) -> dict:
    """
    포트폴리오 전체 분석 보고서 생성 (Claude API).

    Args:
        portfolio_items: api_portfolio() 결과의 items 리스트
        stock_data: {종목코드: dashboard_result row dict}
        ai_reports: {종목코드: scores dict (parsed JSON)}
        watchlist_data: {종목코드: dashboard_result row dict} — 관심종목 데이터 (옵션)

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
            wl_lines.append(format_quant_data(stock))
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
        corr_lines = ["\n## 종목 간 가격 상관관계 행렬 (최근 250 거래일 일별 수익률 기준)\n"]
        header = "종목\t" + "\t".join(names_list)
        corr_lines.append(header)
        for i, name in enumerate(names_list):
            row_vals = []
            for j, v in enumerate(matrix[i]):
                if v is None:
                    row_vals.append("N/A")
                elif i == j:
                    row_vals.append("1.00")
                else:
                    row_vals.append(f"{v:.2f}")
            corr_lines.append(f"{name}\t" + "\t".join(row_vals))
        corr_lines.append("\n※ 0.7 이상: 고상관(빨강), 0.3~0.7: 중간, -0.3~0.3: 낮음, -0.3 미만: 역상관")
        correlation_section = "\n".join(corr_lines) + "\n"

    total_assets = total_eval + cash_balance
    cash_weight_str = (
        f"{cash_balance / total_assets * 100:.1f}%" if total_assets > 0 else "0%"
    )
    user_prompt = PORTFOLIO_USER_PROMPT_TEMPLATE.format(
        stock_count=len(portfolio_items),
        total_eval=f"{round(total_eval):,}",
        cash_balance=f"{round(cash_balance):,}",
        cash_weight=cash_weight_str,
        total_assets=f"{round(total_assets):,}",
        total_return=f"{total_return:.2f}",
        sector_distribution=sector_dist,
        per_stock_sections="\n".join(per_stock_parts),
        watchlist_section=watchlist_section,
        correlation_section=correlation_section,
    )

    client = anthropic.Anthropic(
        api_key=config.ANTHROPIC_API_KEY,
        timeout=httpx.Timeout(600.0, connect=30.0),
    )

    log.info("포트폴리오 AI 분석 시작 (model=%s)", config.PORTFOLIO_MODEL)
    message = _call_with_retry(
        client, model=config.PORTFOLIO_MODEL, max_tokens=32768,
        system=PORTFOLIO_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_prompt}],
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
