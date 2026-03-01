"""
AI 종목 정성 분석 보고서 생성기 (Grand Master Protocol v2).

6대 투자 거장(Warren Buffett, Aswath Damodaran, Philip Fisher,
Pat Dorsey, Peter Lynch, André Kostolany)의 핵심 철학을 기반으로
9단계(Stage 0~8) 심층 분석 프로토콜을 수행합니다.
"""

import json
import logging
from datetime import datetime

import anthropic

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


# ─────────────────────────────────────────
# 정량 데이터 포맷팅
# ─────────────────────────────────────────

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

    quant_text = format_quant_data(stock)

    user_prompt = USER_PROMPT_TEMPLATE.format(
        code=code, name=name, market=market,
        quant_data=quant_text,
        qualitative_section="",
    )

    client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY, timeout=300.0)
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


# ─────────────────────────────────────────
# 포트폴리오 AI 종합 분석
# ─────────────────────────────────────────

PORTFOLIO_SYSTEM_PROMPT = """\
당신은 한국 주식시장 포트폴리오 전략 어드바이저입니다.
투자자의 보유 포트폴리오 전체를 분석하여 포트폴리오 수준의 전략적 조언을 제공합니다.

## 분석 프레임워크

### 1. 포트폴리오 건강도 평가
- 전체 포트폴리오의 품질 점수 (0-100)
- 분산도, 밸류에이션, 성장성, 안정성 관점에서 종합 평가

### 2. 종목별 액션 권고
- 각 종목에 대해 BUY_MORE / HOLD / TRIM / SELL 중 하나를 권고
- 현재 비중 vs 권장 비중 제시
- 정량 데이터와 기존 AI 분석을 종합하여 근거 제시

### 3. 섹터 집중도 분석
- 섹터별 비중 분포의 적절성 평가
- 과집중/과소 섹터 식별
- 섹터 리밸런싱 권고

### 4. 포트폴리오 리스크 & 촉매
- 포트폴리오 전체 관점에서의 주요 리스크 (상관관계, 동일 리스크 노출 등)
- 포트폴리오 전체 관점에서의 상승 촉매

### 5. 보완 제안
- 포트폴리오에 부족한 섹터/테마/스타일 식별
- 추가 편입 후보 테마 제안 (종목 추천 아님, 테마/섹터 수준)

## 분석 지침
- 포트폴리오 비중이 높은 종목에 더 주의를 기울이세요
- 종목 간 상관관계와 중복 리스크를 식별하세요
- 데이터가 없는 종목(ETF, 우선주 등)은 비중 분석에만 포함하세요
- 6대 투자 거장(Buffett, Damodaran, Fisher, Dorsey, Lynch, Kostolany)의 관점이 기존 분석에 포함되어 있으면 이를 종합적으로 활용하세요
- 한국어로 분석하세요
"""

PORTFOLIO_USER_PROMPT_TEMPLATE = """\
아래 포트폴리오의 전체 데이터를 분석하여, 포트폴리오 수준의 전략적 분석 보고서를 작성해주세요.

## 포트폴리오 요약
- 총 종목수: {stock_count}
- 총평가금액: {total_eval}원
- 총수익률: {total_return}%
- 섹터 분포: {sector_distribution}

## 종목별 데이터

{per_stock_sections}

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
      "affected_stocks": ["종목코드"]
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
            lines.append(f"- 리스크: {', '.join(str(r) for r in risks[:3])}")
        catalysts = ai_report.get("catalysts", [])
        if catalysts:
            lines.append(f"- 촉매: {', '.join(str(c) for c in catalysts[:3])}")
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
                              ai_reports: dict[str, dict]) -> dict:
    """
    포트폴리오 전체 분석 보고서 생성 (Claude API).

    Args:
        portfolio_items: api_portfolio() 결과의 items 리스트
        stock_data: {종목코드: dashboard_result row dict}
        ai_reports: {종목코드: scores dict (parsed JSON)}

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

    user_prompt = PORTFOLIO_USER_PROMPT_TEMPLATE.format(
        stock_count=len(portfolio_items),
        total_eval=f"{round(total_eval):,}",
        total_return=f"{total_return:.2f}",
        sector_distribution=sector_dist,
        per_stock_sections="\n".join(per_stock_parts),
    )

    client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY, timeout=600.0)
    message = client.messages.create(
        model=config.ANALYSIS_MODEL,
        max_tokens=16384,
        system=PORTFOLIO_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_prompt}],
    )

    raw_text = message.content[0].text.strip()

    try:
        scores = _parse_json_response(raw_text)
    except json.JSONDecodeError as e:
        log.error("포트폴리오 분석 JSON 파싱 실패: %s", str(e)[:100])
        log.debug("시도한 JSON: %s", raw_text[:200])
        return {
            "scores": {},
            "report_html": "<p>오류: JSON 파싱 실패</p>",
            "error": str(e),
            "model": config.ANALYSIS_MODEL,
            "generated_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "mode": "claude",
        }

    generated_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    model_label = f"Claude ({config.ANALYSIS_MODEL})"
    report_html = render_portfolio_html(scores, portfolio_items,
                                        generated_date, model_label)

    return {
        "scores": scores,
        "report_html": report_html,
        "model": config.ANALYSIS_MODEL,
        "generated_date": generated_date,
        "mode": "claude",
    }


def render_portfolio_html(scores: dict, portfolio_items: list[dict],
                          generated_date: str, model_label: str) -> str:
    """포트폴리오 분석 결과를 HTML 보고서로 렌더링."""

    health = scores.get("portfolio_health", {})
    stock_actions = scores.get("stock_actions", [])
    sector_analysis = scores.get("sector_analysis", {})
    risks = scores.get("portfolio_risks", [])
    catalysts = scores.get("portfolio_catalysts", [])
    missing = scores.get("missing_themes", [])
    summary = scores.get("summary", "")

    grade = health.get("grade", "N/A")
    score = health.get("score", 0)
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
        cur_w = sa.get("current_weight", 0) or 0
        rec_w = sa.get("recommended_weight", 0) or 0
        weight_diff = rec_w - cur_w
        diff_str = f"+{weight_diff:.1f}%" if weight_diff > 0 else f"{weight_diff:.1f}%"
        diff_cls = "val-pos" if weight_diff > 0 else ("val-neg" if weight_diff < 0 else "")
        action_rows += f"""
        <tr>
          <td><strong>{sa.get("name", "")}</strong>
            <span class="text-muted small">{sa.get("code", "")}</span></td>
          <td><span class="pf-action-badge" style="background:{color};">{label}</span></td>
          <td>{cur_w:.1f}%</td>
          <td>{rec_w:.1f}%</td>
          <td class="{diff_cls}">{diff_str}</td>
          <td class="small">{sa.get("rationale", "")}</td>
        </tr>"""

    # 리스크 아이템
    severity_colors = {"high": "#c0392b", "medium": "#b9770e", "low": "#2c3e50"}
    risk_html = ""
    for r in risks:
        sev = r.get("severity", "medium")
        sev_color = severity_colors.get(sev, "#6c757d")
        affected = ", ".join(r.get("affected_stocks", []))
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
        benefiting = ", ".join(c.get("benefiting_stocks", []))
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

  <div class="stage-card">
    <div class="stage-card-title">포트폴리오 건강도 상세</div>
    <div class="stage-field"><strong>분산도:</strong> {health.get("diversification", "N/A")}</div>
    <div class="stage-field"><strong>밸류에이션:</strong> {health.get("valuation", "N/A")}</div>
    <div class="stage-field"><strong>성장성/품질:</strong> {health.get("growth_quality", "N/A")}</div>
    <div class="stage-analysis">{health.get("overall_assessment", "")}</div>
  </div>

  <div class="stage-card">
    <div class="stage-card-title">종목별 액션 권고</div>
    <div class="table-responsive">
      <table class="table table-sm table-hover mb-0">
        <thead><tr>
          <th>종목</th><th>액션</th><th>현재비중</th><th>권장비중</th><th>변경</th><th>근거</th>
        </tr></thead>
        <tbody>{action_rows}</tbody>
      </table>
    </div>
  </div>

  <div class="stage-card">
    <div class="stage-card-title">섹터 집중도 분석</div>
    <div class="stage-field"><strong>집중 리스크:</strong>
      {sector_analysis.get("concentration_risk", "N/A")}</div>
    <div class="stage-field"><strong>과비중 섹터:</strong> {ow_badges}</div>
    <div class="stage-field"><strong>과소비중 섹터:</strong> {uw_badges}</div>
    <div class="stage-analysis">{sector_analysis.get("rebalancing_suggestion", "")}</div>
  </div>

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

    # 5. 리스크/촉매 변화
    old_risks = set(old_scores.get("risks", []))
    new_risks = set(new_scores.get("risks", []))
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
