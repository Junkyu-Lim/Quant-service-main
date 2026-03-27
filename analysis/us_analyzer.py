"""
US Stock AI Analysis Report Generator.

Same 9-stage Grand Master Protocol as the Korean analyzer,
but prompts are in English. All output text is in Korean.
"""

import json
import logging
import math
import hashlib
import time
from datetime import datetime

import anthropic
import httpx

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import db as _db
import config

# Reuse rendering/parsing helpers from the KR analyzer
from analysis.claude_analyzer import (
    render_html,
    _parse_json_response,
    _postprocess_scores,
    _call_with_retry,
    _fmt_val,
    build_stock_analysis_input_hash,
)

log = logging.getLogger("US_Analyzer")
US_ANALYSIS_INPUT_VERSION = "us-stock-analysis-v1"

# ─────────────────────────────────────────
# System Prompt (English instructions, Korean output)
# ─────────────────────────────────────────

SYSTEM_PROMPT = """\
You are a Grand Master analyst for US equity markets (NYSE/NASDAQ).
You apply the investment philosophies of 9 legendary investors through a 9-stage deep analysis protocol (Stage 0–8).
**IMPORTANT: All analysis output text must be written in Korean (한국어). JSON keys remain in English as specified.**

[Anti-hallucination — top priority] Do not assume a company's business based on its name alone. Always verify via web search first.

## Analysis Stages
- Stage 0: Core business model (what does it sell? 3 lines, key products/services & revenue mix required)
- Stage 1: Macro environment & value chain (upstream industry CAGR, competitive positioning)
- Stage 2: Business model dissection (P×Q×C analysis, cash cow vs growth drivers)
- Stage 3: Lifecycle & moat (intro/growth/mature/decline, 4 moat types with data evidence)
- Stage 4: Financial health (gross margin trend, FCF quality, leverage, consensus deviation)
- Stage 4.5: Peer comparison → see separate rules below
- Stage 5: Outlook & momentum (CAPEX, pipeline, new initiatives, 12-month catalysts)
- Stage 6: Valuation & Kostolany egg (lifecycle-matched methodology, egg position 1–6)
- Stage 7: 9 Masters evaluation (1 line + analysis each, composite S–F grade)
- Stage 8: Trading action plan (entry/target/stop-loss/weight/holding period/exit conditions)

### Kostolany Egg Position (apply quantitative data directly)
1=extreme fear: RSI<30, within 10% of 52w low, MA60 gap<-15% | 2=pessimism→turning: RSI 30–45 | 3=optimism start: RSI 45–60 | 4=extreme optimism: RSI>70, within 5% of 52w high | 5=optimism→declining: RSI 55–70 | 6=pessimism start: RSI 40–55, 15–30% off high

### Timing Signal Interpretation (only if data present)
- "수급 다이버전스 경고" → adjust Stage 6 egg to 5+ and reflect in Kostolany Stage 7
- "과열+매도 동시 경고" → Stage 8 entry_price must be set ≥10% below current price
- "피크 실적 리스크" → add to risks: "피크 실적 리스크: 영업이익 성장 감속 중" severity=high
- "상승 초기 진입 기회" → Stage 6 egg position 2–3, entry near current price, active weight recommended
- "VCP 돌파 대기" → Stage 8 short-to-mid holding period, mention quick entry on catalyst

### Master Scoring Criteria (1–10, absolute evaluation vs US-listed universe)
10=top 2%·9=excellent·7–8=good·5–6=average·3–4=below average (must cite weakness)·1–2=unsuitable
[Required] Cite at least 1 key weakness per master. Never list only positives.

### 9 Masters — Core Perspectives
- Buffett: moat durability, management quality, S-RIM margin of safety, long-term hold
- Damodaran: narrative-number consistency, ROIC vs WACC, risk-adjusted return
- Fisher: R&D innovation, margin improvement trend, long-term growth, organizational culture
- Dorsey: 4 moat types (intangible/switching costs/network/cost advantage), trend (widening/narrowing)
- Lynch: PEG, understandable business, 10-bagger potential, earnings↔price linkage
- Kostolany: egg position, contrarian, liquidity/flows, patience
- Munger: inversion (top 3 failure scenarios), mental models, ROIC sustainability, Lollapalooza effect
- Marks: second-level thinking (does the market already know?), cycle position, price vs value gap, consensus deviation, permanent capital loss probability
- Klarman: catalyst identification (value re-rating trigger), conservative margin of safety, value trap warning, downside scenario

## Stage 4.5: Peer Comparison
- If "Peer DB Data" section is present in USER_PROMPT, use those values directly (no web estimation)
- Only use web search to supplement when DB data is absent. State relative rank per metric. Assess undervalued/fair/overvalued. Honestly mention better alternatives if they exist.

## Web Search (max 2 uses)
1. [Required] "{ticker} {company} earnings revenue operating income {year}" → recent results/news → apply to Stage 5 & recent_news
2. [Required] "{ticker} analyst price target consensus" → consensus → apply to Stage 5 & Stage 8
Cite search results specifically in analysis.

## Source Citation Rules
- Priority: SEC filings/earnings releases/company IR > analyst reports > financial news > general articles
- Preferred sources: SEC EDGAR, company IR, earnings calls, Reuters, Bloomberg, WSJ, major broker reports
- Each recent_news item must include date, source, and at least 1 key fact (preferably with a number)
- stage5_outlook.analysis: 2–4 sentences, at least 2 with date or source reference
- Distinguish confirmed facts from estimates/opinions
- Do not use unverified community posts or unattributed numbers

## recent_news (3–4 items preferred)
Select investment-relevant news/filings from web search. Each item: title, date, summary (1 sentence with ≥1 number), impact, source.

## Second-Level Thinking (Howard Marks — apply to all judgments)
For every positive judgment, ask: "Does the market already know this and reflect it in the current price?"
If your view aligns with consensus, explain specifically why it is NOT yet priced in.
If positives are already priced in: Marks score cap 5, Stage 8 entry_price must be below current price.

## Inversion Thinking (Charlie Munger — required in Stage 7)
Write "Top 3 failure scenarios" for this investment. Each scenario must be specific (include numbers/events).
If average of all 9 masters ≥8, Munger bear_case must flag overconfidence/overvaluation risk.

## Catalyst Identification (Seth Klarman — linked to Stage 5 & 7)
For undervalued stocks, identify at least one 12–18 month re-rating catalyst.
Specify expected timing and probability (high/medium/low) for each catalyst.
If no catalyst found: Klarman score cap 4, include "무촉매 저평가" warning in summary.

## Value Trap Detection (Stage 4 value_trap_risk criteria)
- Revenue up / margin down → divergent, value_trap_risk ≥ medium
- PER<15 + PBR<2 + sustainability_quality ≤ 2 → "cheap for reason", raise value_trap_risk
- Cash conversion rate <50% → question earnings quality, mention in Stage 4
- sustainability_quality ≥ 4 + positive divergence → "genuine undervaluation", value_trap_risk=low

## Internal Consistency (self-check before JSON output)
1. moat_rating=none → use conservative BPS-based fair value, no growth premium
2. TTM_FCF negative or debt ratio >200% → portfolio_weight max 2%
3. lifecycle=쇠퇴기 → no growth PER/PSR multiples in Stage 6
4. recent_news impact=부정 → must reflect in risks
5. peer relative_valuation=고평가 → entry_price must be below current price
6. better_alternative≠null → mention alternative in summary
7. revenue up / margin down → value_trap_risk ≥ medium, Buffett score cap 6
8. revenue down / margin down → value_trap_risk=high, Buffett/Dorsey score cap 4
9. value_trap_warning=1 → summary must include "가치함정 리스크", portfolio_weight cap 3%
10. All 9 masters average ≥8 → Munger bear_case must flag overconfidence risk
11. Klarman catalysts=0 + divergence>0 → summary must include "무촉매 저평가 경고", portfolio_weight cap 3%

## risks — Required Rules
Never just write "competition/interest rates/FX" — must link to specific numbers/events. At least 1 financial weakness, 1 severity=high, 1 with data source evidence.

## Output Compression Rules
- No text outside JSON
- Do not copy-paste the same numbers/sources across recent_news, stage analysis, and summary
- Write only key evidence; omit preambles, filler phrases, and repetitive expressions

"""

USER_PROMPT_TEMPLATE = """\
Stock: {ticker} — {name} ({exchange})

## Quantitative Data
{quant_data}
{qualitative_section}
[Anti-hallucination] Always verify the company's actual core business via web search before analysis. Do not infer industry from the ticker/name alone.
Data usage hints: PEG·Revenue_CAGR·ROE→Lynch | PER/PBR deviation→Buffett | F-Score→Stage4 | RSI·MA gap→Kostolany | TTM·CAGR→Stage2

## Length Guidelines
- recent_news: 3 items preferred
- Stage 1–6: 2–4 sentences each
- Stage 7 each master: 2–3 sentences (Munger bear_case 3 items separately)
- summary: 4–5 sentences
- Stage 8: 1–2 sentences

You MUST respond only with the following JSON format. All text values must be written in Korean (한국어).

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
    "fcf_quality": "<FCF 품질>",
    "value_trap_risk": "<low|medium|high>",
    "debt_assessment": "<부채 구조>",
    "consensus_deviation": "<컨센서스 대비>",
    "analysis": "<2-4문장>"
  }},
  "peer_comparison": {{
    "peers": [
      {{
        "name": "<경쟁사명>",
        "code": "<ticker 또는 null>",
        "market_cap": "<시총(B USD)>",
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
    "order_backlog": "<수주잔고 또는 파이프라인>",
    "new_business": "<신사업>",
    "catalysts_12m": ["<촉매1>", "<촉매2>", "<촉매3>"],
    "analysis": "<2-4문장. 최소 2문장 이상은 날짜/출처/수치 포함>"
  }},
  "stage6_valuation": {{
    "lifecycle_matched_method": "<밸류에이션 방법론>",
    "fair_value_range": "<적정 주가 범위 (USD)>",
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
    "marks": {{"score": <1-10>, "one_liner": "<한 줄 평>", "consensus_gap": "<시장 기대 vs 실제 괴리>", "analysis": "<2-3문장>"}},
    "klarman": {{"score": <1-10>, "one_liner": "<한 줄 평>", "catalysts": [{{"event": "<촉매>", "timing": "<예상 시기>", "probability": "<high|medium|low>"}}], "analysis": "<2-3문장>"}}
  }},
  "stage8_action": {{
    "entry_price": "<진입 가격대 (USD)>",
    "entry_basis": "<산출 근거>",
    "target_price": "<12개월 목표주가 (USD)>",
    "target_basis": "<산출 근거>",
    "stop_loss": "<손절 기준>",
    "portfolio_weight": "<권장 비중 (예: 3-5%)>",
    "holding_period": "<단기3개월|중기6-12개월|장기2-3년>",
    "exit_conditions": ["<매도 조건1>", "<매도 조건2>"],
    "analysis": "<1-2문장>"
  }},
  "summary": "<4-5문장 종합 투자 의견 (한국어)>",
  "recent_news": [
    {{
      "title": "<제목>",
      "date": "<YYYY-MM-DD 또는 추정시기>",
      "summary": "<1문장, 핵심 수치 포함>",
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
# US quant data sections
# ─────────────────────────────────────────

US_QUANT_SECTIONS = {
    "Basic Info": [
        ("종가", "usd"), ("시가총액", "usd_cap"), ("섹터", "str"), ("exchange", "str"), ("industry", "str"),
    ],
    "Valuation": [
        ("PER", "f2"), ("PBR", "f2"), ("PEG", "f2"), ("PSR", "f2"),
        ("ROE(%)", "f2"), ("이익수익률(%)", "f2"),
        ("적정주가_SRIM", "usd"), ("괴리율(%)", "f2"),
    ],
    "Quality / Financials": [
        ("영업이익률(%)", "f2"), ("현금전환율(%)", "f1"), ("FCF수익률(%)", "f2"),
        ("F스코어", "int"), ("부채비율(%)", "f1"), ("유동비율(%)", "f1"),
        ("ROIC(%)", "f2"), ("이자보상배율", "f2"), ("부채상환능력", "f2"),
    ],
    "Growth / Momentum": [
        ("매출_CAGR", "f1"), ("영업이익_CAGR", "f1"), ("순이익_CAGR", "f1"),
        ("Q_매출_YoY(%)", "f1"), ("Q_영업이익_YoY(%)", "f1"), ("Q_순이익_YoY(%)", "f1"),
        ("TTM_영업이익_YoY(%)", "f1"), ("TTM_순이익_YoY(%)", "f1"),
        ("RS_등급", "f1"),
    ],
    "Dividend / Technical": [
        ("배당수익률(%)", "f2"), ("배당성향(%)", "f2"), ("배당_연속증가", "int"),
        ("52주_최고대비(%)", "f1"), ("52주_최저대비(%)", "f1"),
        ("MA60_이격도(%)", "f1"), ("RSI_14", "f1"),
        ("과열도", "f1"), ("상승조짐", "f1"),
    ],
    "TTM Summary (USD)": [
        ("TTM_매출", "usd_cap"), ("TTM_영업이익", "usd_cap"), ("TTM_순이익", "usd_cap"),
        ("TTM_영업CF", "usd_cap"), ("TTM_FCF", "usd_cap"),
        ("자본", "usd_cap"), ("부채", "usd_cap"), ("자산총계", "usd_cap"),
    ],
}


def _fmt_us_val(v, fmt_type: str) -> str:
    """US 전용 포맷터 (USD 단위 포함)."""
    if v is None:
        return "N/A"
    if fmt_type == "str":
        return str(v) if v else "N/A"
    if fmt_type == "flag":
        return "Yes" if v == 1 else ("No" if v == 0 else "N/A")
    if fmt_type == "int":
        try:
            return f"{int(v):,}"
        except (TypeError, ValueError):
            return "N/A"
    if fmt_type in ("f1", "f2"):
        try:
            decimals = 1 if fmt_type == "f1" else 2
            return f"{float(v):.{decimals}f}"
        except (TypeError, ValueError):
            return "N/A"
    if fmt_type == "usd":
        try:
            fv = float(v)
            if abs(fv) >= 1000:
                return f"${fv:,.0f}"
            return f"${fv:.2f}"
        except (TypeError, ValueError):
            return "N/A"
    if fmt_type == "usd_cap":
        # USD raw value → formatted with B/M suffix
        try:
            fv = float(v)
            if abs(fv) >= 1e9:
                return f"${fv / 1e9:.2f}B"
            if abs(fv) >= 1e6:
                return f"${fv / 1e6:.1f}M"
            return f"${fv:,.0f}"
        except (TypeError, ValueError):
            return "N/A"
    return str(v)


def _us_strategy_tags(stock: dict) -> str:
    tags = []
    score_map = {
        "주도주_점수": "Leaders",
        "우량가치_점수": "Quality Value",
        "고성장_점수": "Growth Mom",
        "현금배당_점수": "Cash Div",
        "턴어라운드_점수": "Turnaround",
    }
    for col, label in score_map.items():
        v = stock.get(col)
        if v is not None:
            try:
                if float(v) >= 60:
                    tags.append(label)
            except (TypeError, ValueError):
                pass
    return ", ".join(tags) if tags else "None"


def format_us_quant_data(stock: dict) -> str:
    """US 종목 데이터를 분석용 텍스트로 포맷팅 (USD 단위)."""
    lines = [
        "### Analysis Notes",
        f"- Strategy Tags: {_us_strategy_tags(stock)}",
    ]
    for section, metrics in US_QUANT_SECTIONS.items():
        lines.append(f"\n### {section}")
        for col, fmt_type in metrics:
            val = stock.get(col)
            lines.append(f"- {col}: {_fmt_us_val(val, fmt_type)}")

    # 타이밍 신호 (과열/상승조짐 기반)
    timing_parts = []
    overheat = stock.get("과열도")
    upward = stock.get("상승조짐")
    try:
        if overheat is not None and float(overheat) >= 75:
            timing_parts.append(f"과열+매도 동시 경고 (과열도={float(overheat):.0f})")
        elif overheat is not None and float(overheat) >= 60:
            timing_parts.append(f"경계 (과열도={float(overheat):.0f})")
        if upward is not None and float(upward) >= 55:
            timing_parts.append(f"상승 초기 진입 기회 (상승조짐={float(upward):.0f})")
    except (TypeError, ValueError):
        pass
    vcp = stock.get("VCP_신호")
    try:
        if vcp is not None and int(float(vcp)) == 1:
            timing_parts.append("VCP 돌파 대기")
    except (TypeError, ValueError):
        pass
    if timing_parts:
        lines.append(f"\n### Timing Signals\n- " + "\n- ".join(timing_parts))

    return "\n".join(lines)


# ─────────────────────────────────────────
# US Peer 비교: DB 조회
# ─────────────────────────────────────────

def _fetch_us_industry_candidates(ticker: str, industry: str, limit: int = 15) -> list[dict]:
    """us_dashboard_result에서 같은 industry 종목을 시총 순으로 조회."""
    if not industry:
        return []
    try:
        with _db.get_conn() as conn:
            rows = conn.execute(
                """SELECT 종목코드, 종목명, PER, PBR, "ROE(%)", "영업이익률(%)", 시가총액,
                          COALESCE("Q_매출_YoY(%)", "TTM_매출_YoY(%)", "매출_CAGR"), industry
                   FROM us_dashboard_result
                   WHERE industry = ? AND 종목코드 != ?
                   ORDER BY 시가총액 DESC
                   LIMIT ?""",
                [industry, ticker, limit],
            ).fetchall()
        return [
            {"종목코드": r[0], "종목명": r[1], "PER": r[2], "PBR": r[3],
             "ROE(%)": r[4], "영업이익률(%)": r[5], "시가총액": r[6],
             "매출_CAGR": r[7], "industry": r[8]}
            for r in rows
        ]
    except Exception as e:
        log.warning("US industry candidates fetch failed: %s", e)
        return []


def _fetch_us_peer_data(tickers: list[str]) -> list[dict]:
    """us_dashboard_result에서 특정 ticker 목록의 지표를 조회."""
    if not tickers:
        return []
    try:
        placeholders = ", ".join("?" * len(tickers))
        with _db.get_conn() as conn:
            rows = conn.execute(
                f"""SELECT 종목코드, 종목명, PER, PBR, "ROE(%)", "영업이익률(%)", 시가총액,
                           COALESCE("Q_매출_YoY(%)", "TTM_매출_YoY(%)", "매출_CAGR")
                    FROM us_dashboard_result
                    WHERE 종목코드 IN ({placeholders})""",
                tickers,
            ).fetchall()
        result = []
        for r in rows:
            mktcap_b = round(r[6] / 1e9, 2) if r[6] else None
            result.append({
                "종목코드": r[0],
                "종목명": r[1],
                "PER": round(r[2], 2) if r[2] else None,
                "PBR": round(r[3], 2) if r[3] else None,
                "ROE(%)": round(r[4], 1) if r[4] else None,
                "영업이익률(%)": round(r[5], 1) if r[5] else None,
                "시가총액(B USD)": mktcap_b,
                "매출성장률(%)": round(r[7], 1) if r[7] else None,
            })
        return result
    except Exception as e:
        log.warning("US peer data fetch failed: %s", e)
        return []


def _identify_us_peers(stock: dict, ticker: str, candidates: list[dict]) -> list[str]:
    """후보군에서 정량 유사도로 경쟁사 ticker를 선택."""
    if not candidates:
        return []

    def _sf(v):
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    target_cap = _sf(stock.get("시가총액"))
    target_per = _sf(stock.get("PER"))
    target_pbr = _sf(stock.get("PBR"))
    target_roe = _sf(stock.get("ROE(%)"))
    target_opm = _sf(stock.get("영업이익률(%)"))
    target_growth = _sf(stock.get("Q_매출_YoY(%)") or stock.get("TTM_매출_YoY(%)") or stock.get("매출_CAGR"))

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
            cand_val = _sf(cand.get(key))
            if target is None or cand_val is None:
                score += weight * 1.5
            else:
                denom = max(abs(target), 1.0)
                score += abs(target - cand_val) / denom * weight
        ranked.append((score, str(cand["종목코드"])))

    ranked.sort(key=lambda x: x[0])
    return [t for _, t in ranked[:5]]


def build_us_analysis_input_hash(stock: dict, data_version: str = "") -> str:
    """US 분석 입력 해시."""
    payload = {
        "version": US_ANALYSIS_INPUT_VERSION,
        "data_version": data_version,
        "stock": {k: stock.get(k) for k in sorted(stock.keys())},
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


# ─────────────────────────────────────────
# Main: generate_us_report
# ─────────────────────────────────────────

def generate_us_report(stock: dict) -> dict:
    """
    US 종목 분석 보고서를 생성합니다 (Claude API, English prompts → Korean output).

    Args:
        stock: us_dashboard_result의 한 종목 데이터 (dict)

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

    ticker = str(stock.get("종목코드", "")).upper()
    name = stock.get("종목명", "Unknown")
    exchange = stock.get("exchange", stock.get("시장구분", ""))
    industry = stock.get("industry", stock.get("섹터", "")) or ""

    quant_text = format_us_quant_data(stock)

    client = anthropic.Anthropic(
        api_key=config.ANTHROPIC_API_KEY,
        timeout=httpx.Timeout(config.ANALYSIS_TIMEOUT_SEC, connect=30.0),
    )

    # Peer DB 섹션 구성
    peer_db_section = ""
    candidates = _fetch_us_industry_candidates(ticker, industry, limit=15)
    if candidates:
        log.info("US Peer DB selection start (%s %s, industry=%s, candidates=%d)",
                 ticker, name, industry, len(candidates))
        peer_tickers = _identify_us_peers(stock, ticker, candidates)
        if peer_tickers:
            peer_data = _fetch_us_peer_data(peer_tickers)
            if peer_data:
                lines = ["## Peer DB Data (from our system — do not estimate from web)"]
                lines.append("Use the values below directly for peer_comparison.peers.")
                lines.append("")
                for p in peer_data:
                    lines.append(
                        f"- {p['종목코드']} {p['종목명']}: "
                        f"MarketCap={p['시가총액(B USD)']}B USD, PER={p['PER']}, PBR={p['PBR']}, "
                        f"ROE={p['ROE(%)']}%, OPM={p['영업이익률(%)']}%, RevGrowth={p['매출성장률(%)']}%"
                    )
                peer_db_section = "\n".join(lines) + "\n"
                log.info("US Peer DB ready (%d stocks)", len(peer_data))

    user_prompt = USER_PROMPT_TEMPLATE.format(
        ticker=ticker,
        name=name,
        exchange=exchange,
        quant_data=quant_text,
        qualitative_section=("\n" + peer_db_section) if peer_db_section else "",
    )

    user_content: list = [{"type": "text", "text": user_prompt}]
    system_with_cache = [{"type": "text", "text": SYSTEM_PROMPT, "cache_control": {"type": "ephemeral"}}]

    web_search_max_uses = max(1, min(2, int(config.WEB_SEARCH_MAX_USES)))
    log.info(
        "US 종목 AI 분석 시작 (%s %s, model=%s, timeout=%.0fs, prompt_chars=%d, web_search max_uses=%d)",
        ticker, name, config.ANALYSIS_MODEL, config.ANALYSIS_TIMEOUT_SEC,
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

    if hasattr(message, "usage") and message.usage:
        u = message.usage
        cache_read = getattr(u, "cache_read_input_tokens", 0) or 0
        cache_write = getattr(u, "cache_creation_input_tokens", 0) or 0
        log.info("토큰 사용 (%s %s): input=%d output=%d cache_read=%d cache_write=%d",
                 ticker, name, u.input_tokens, u.output_tokens, cache_read, cache_write)

    block_types = [getattr(b, "type", "?") for b in message.content]
    elapsed = time.perf_counter() - started_at
    log.info("US AI 분석 완료 (%s %s, stop_reason=%s, blocks=%s, elapsed=%.1fs)",
             ticker, name, message.stop_reason, block_types, elapsed)

    raw_text = ""
    for block in message.content:
        if hasattr(block, "type") and block.type == "text":
            raw_text += block.text
    raw_text = raw_text.strip()

    if not raw_text:
        log.error("US 분석: text 블록 없음 (%s %s)", ticker, name)
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
        log.warning("US 분석 max_tokens 도달 (%s %s, len=%d)", ticker, name, len(raw_text))

    try:
        scores = _parse_json_response(raw_text)
    except json.JSONDecodeError as e:
        log.error("US Claude JSON 파싱 실패 (%s %s): %s", ticker, name, str(e)[:100])
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
    # render_html은 KR/US 공통 사용 가능 (JSON 구조 동일)
    report_html = render_html(
        code=ticker,
        name=name,
        market=exchange,
        stock=stock,
        scores=scores,
        generated_date=generated_date,
        model_label=model_label,
        truncated=truncated_by_tokens,
    )

    return {
        "scores": scores,
        "report_html": report_html,
        "model": model_label,
        "generated_date": generated_date,
        "mode": "claude",
    }
