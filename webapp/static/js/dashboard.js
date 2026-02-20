/**
 * Quant Dashboard – 완전한 클라이언트 로직
 * 기능: 종목 클릭 세부 모달, 툴팁, Advanced Filter, AI 분석, 파이프라인, 비교, 관심종목, 배치 변경
 */
(function () {
  "use strict";

  // ─── 상태 ───────────────────────────────────────────────────────────────
  let currentScreen = "all";
  let currentPage   = 1;
  let pageSize      = 50;
  let sortCol       = "종합점수";
  let sortOrder     = "desc";
  const columnFilters = {};       // { col: { min, max } }
  let batchChanges  = null;
  const compareSet  = new Set();  // 비교 선택된 종목코드
  let advCat        = "all";
  let currentDetailCode = null;
  let currentDetailData = null;
  let financialChart    = null;
  let pipelineTimer     = null;
  let pipelineStartTime = null;
  let tabCounts         = {};

  // ─── 탭 기본 정렬 ─────────────────────────────────────────────────────
  const TAB_DEFAULT_SORT = {
    all:            "종합점수",
    leaders:        "주도주_점수",
    quality_value:  "우량가치_점수",
    growth_mom:     "고성장_점수",
    cash_div:       "현금배당_점수",
    turnaround:     "턴어라운드_점수",
    multi_strategy: "전략수",
    watchlist:      "종합점수",
  };

  // ─── 관심종목 ──────────────────────────────────────────────────────────
  const WATCHLIST_KEY = "quant_watchlist";
  function getWatchlist() {
    try { return new Set(JSON.parse(localStorage.getItem(WATCHLIST_KEY) || "[]")); }
    catch { return new Set(); }
  }
  function saveWatchlist(set) {
    localStorage.setItem(WATCHLIST_KEY, JSON.stringify([...set]));
  }
  function updateWatchlistCount() {
    const el = document.getElementById("cnt-watchlist");
    if (el) el.textContent = getWatchlist().size;
  }
  function updateAllStarButtons() {
    const wl = getWatchlist();
    document.querySelectorAll(".watch-btn").forEach(btn => {
      const watched = wl.has(btn.dataset.code);
      btn.textContent = watched ? "★" : "☆";
      btn.classList.toggle("watched", watched);
    });
    const btnWD = document.getElementById("btn-watch-detail");
    if (btnWD && btnWD.dataset.code) {
      const watched = wl.has(btnWD.dataset.code);
      btnWD.textContent = watched ? "★ 관심해제" : "☆ 관심종목";
      btnWD.classList.toggle("btn-warning", watched);
      btnWD.classList.toggle("btn-outline-warning", !watched);
    }
  }
  function toggleWatch(code) {
    const wl = getWatchlist();
    if (wl.has(code)) wl.delete(code); else wl.add(code);
    saveWatchlist(wl);
    updateAllStarButtons();
    updateWatchlistCount();
    if (currentScreen === "watchlist") loadStocks();
  }

  // ─── 전략 설명 ────────────────────────────────────────────────────────
  const STRATEGY_DESCRIPTIONS = {
    all:            { title: "📊 전체 종목",            criteria: "전체 시장 종목 조회" },
    leaders:        { title: "🔥 시장 주도주 (Leaders)",  criteria: "시총 2,000억↑ · 거래대금 10억↑ · 실적성장" },
    quality_value:  { title: "💎 우량가치 (Quality & Value)", criteria: "ROE 10%↑ · PEG < 1.5 · PER 1~40 · F-Score 4↑" },
    growth_mom:     { title: "🚀 고성장 모멘텀 (Growth)", criteria: "이익성장 15%↑ · 분기 실적 호조 · 정배열 추세" },
    cash_div:       { title: "💰 현금배당 (Cash & Div)",  criteria: "FCF수익률 3%↑ · 배당수익률 1%↑ · 부채비율 < 150%" },
    turnaround:     { title: "🔄 턴어라운드 (Turnaround)", criteria: "흑자전환 OR 이익률 급개선 · TTM 순이익 흑자" },
    multi_strategy: { title: "🏆 Multi-Pick (3관왕 이상)", criteria: "5개 전략 중 3개 이상 동시 선정 종목" },
    watchlist:      { title: "⭐ 관심 종목",             criteria: "사용자가 직접 추가한 종목" },
  };

  // ─── 탭별 기본 컬럼 정의 ─────────────────────────────────────────────
  const COLUMNS = {
    all: [
      { key: "종목코드", label: "Code" }, { key: "종목명", label: "Name" }, { key: "시장구분", label: "Mkt" },
      { key: "종가", label: "Price", fmt: "int" }, { key: "PER", label: "PER", fmt: "f2" },
      { key: "ROE(%)", label: "ROE", fmt: "f2" }, { key: "PBR", label: "PBR", fmt: "f2" },
      { key: "수급강도", label: "Supply", fmt: "f1" }, { key: "거래대금_20일평균", label: "Vol Avg", fmt: "int" },
      { key: "종합점수", label: "Score", fmt: "f1" }
    ],
    leaders: [
      { key: "종목코드", label: "Code" }, { key: "종목명", label: "Name" }, { key: "종가", label: "Price", fmt: "int" },
      { key: "수급강도", label: "Supply", fmt: "f1" }, { key: "거래대금_20일평균", label: "Vol Avg", fmt: "int" },
      { key: "Q_영업이익_YoY(%)", label: "Q OP YoY", fmt: "f1" }, { key: "RSI_14", label: "RSI", fmt: "f1" },
      { key: "주도주_점수", label: "L-Score", fmt: "f1" }
    ],
    quality_value: [
      { key: "종목코드", label: "Code" }, { key: "종목명", label: "Name" }, { key: "종가", label: "Price", fmt: "int" },
      { key: "ROE(%)", label: "ROE", fmt: "f2" }, { key: "PEG", label: "PEG", fmt: "f2" },
      { key: "PER", label: "PER", fmt: "f2" }, { key: "F스코어", label: "F-Score", fmt: "int" },
      { key: "우량가치_점수", label: "QV-Score", fmt: "f1" }
    ],
    growth_mom: [
      { key: "종목코드", label: "Code" }, { key: "종목명", label: "Name" }, { key: "종가", label: "Price", fmt: "int" },
      { key: "영업이익_CAGR", label: "OP CAGR", fmt: "f1" }, { key: "Q_영업이익_YoY(%)", label: "Q OP YoY", fmt: "f1" },
      { key: "MA20_이격도(%)", label: "MA20%", fmt: "f1" }, { key: "52주_최고대비(%)", label: "52W High", fmt: "f1" },
      { key: "고성장_점수", label: "G-Score", fmt: "f1" }
    ],
    cash_div: [
      { key: "종목코드", label: "Code" }, { key: "종목명", label: "Name" }, { key: "종가", label: "Price", fmt: "int" },
      { key: "FCF수익률(%)", label: "FCF%", fmt: "f2" }, { key: "배당수익률(%)", label: "Div%", fmt: "f2" },
      { key: "부채비율(%)", label: "Debt%", fmt: "f1" }, { key: "DPS_CAGR", label: "DPS CAGR", fmt: "f1" },
      { key: "현금배당_점수", label: "CD-Score", fmt: "f1" }
    ],
    turnaround: [
      { key: "종목코드", label: "Code" }, { key: "종목명", label: "Name" }, { key: "종가", label: "Price", fmt: "int" },
      { key: "흑자전환", label: "Turn", fmt: "flag" }, { key: "이익률_급개선", label: "OPM Jump", fmt: "flag" },
      { key: "이익률_변동폭", label: "OPM Delta", fmt: "f1" }, { key: "RSI_14", label: "RSI", fmt: "f1" },
      { key: "턴어라운드_점수", label: "T-Score", fmt: "f1" }
    ],
    multi_strategy: [
      { key: "종목코드", label: "Code" }, { key: "종목명", label: "Name" }, { key: "종가", label: "Price", fmt: "int" },
      { key: "전략수", label: "Count", fmt: "int" }, { key: "종합점수", label: "Total Score", fmt: "f1" }
    ],
    watchlist: [
      { key: "종목코드", label: "Code" }, { key: "종목명", label: "Name" }, { key: "종가", label: "Price", fmt: "int" },
      { key: "PER", label: "PER", fmt: "f2" }, { key: "ROE(%)", label: "ROE", fmt: "f2" },
      { key: "종합점수", label: "Score", fmt: "f1" }
    ]
  };

  // ─── 지표 툴팁 ────────────────────────────────────────────────────────
  const METRIC_TOOLTIPS = {
    "PER": "주가수익비율. 낮을수록 저평가. 업종 평균 대비 비교 권장.",
    "PBR": "주가순자산비율. 1 미만이면 장부가 이하.",
    "PSR": "주가매출비율. 적자기업 평가에 유용.",
    "PEG": "PEG = PER ÷ 이익성장률. 1 미만이면 성장 대비 저평가.",
    "ROE(%)": "자기자본이익률. 10% 이상이 일반적 기준.",
    "EPS": "주당순이익 (원).",
    "BPS": "주당순자산 (원). PBR 계산의 기준값.",
    "F스코어": "Piotroski F-Score (0-9). 7↑ 우량, 4↓ 주의.",
    "부채비율(%)": "부채/자본. 200% 미만이 일반적으로 안전.",
    "영업이익률(%)": "영업이익/매출. 높을수록 사업 효율적.",
    "이익수익률(%)": "EPS/주가×100. 채권 수익률과 비교 가능.",
    "FCF수익률(%)": "잉여현금흐름/시가총액×100. 3%↑ 양호.",
    "배당수익률(%)": "연간배당금/주가×100.",
    "이익품질_양호": "영업CF > 순이익 → O. 이익 품질 높음.",
    "현금전환율(%)": "영업CF/순이익×100. 100%↑ 이상적.",
    "CAPEX비율(%)": "CAPEX/영업CF. 낮을수록 FCF 여유 많음.",
    "부채상환능력": "이자보상배율 (영업이익/이자비용). 3↑ 안전.",
    "52주_최고대비(%)": "현재가의 52주 최고가 대비 위치. 0%이면 신고가.",
    "52주_최저대비(%)": "현재가가 52주 최저가 대비 상승률.",
    "MA20_이격도(%)": "현재가 vs 20일 이동평균 이격. ±5% 범위 정상.",
    "MA60_이격도(%)": "현재가 vs 60일 이동평균 이격.",
    "RSI_14": "상대강도지수(14일). 30↓ 과매도, 70↑ 과매수.",
    "거래대금_20일평균": "최근 20일 평균 거래대금 (원). 유동성 지표.",
    "거래대금_증감(%)": "최근 vs 이전 거래대금 증감률.",
    "변동성_60일(%)": "60일 연환산 변동성. 낮을수록 안정적.",
    "수급강도": "외인+기관 순매수 강도. 양수면 매수우위.",
    "외인순매수_20d": "최근 20일 외국인 순매수 주식 수.",
    "기관순매수_20d": "최근 20일 기관 순매수 주식 수.",
    "매출_CAGR": "매출액 연평균 성장률 (%).",
    "영업이익_CAGR": "영업이익 연평균 성장률 (%).",
    "순이익_CAGR": "순이익 연평균 성장률 (%).",
    "영업CF_CAGR": "영업현금흐름 연평균 성장률 (%).",
    "FCF_CAGR": "잉여현금흐름 연평균 성장률 (%).",
    "DPS_최근": "최근 주당배당금 (원).",
    "DPS_CAGR": "주당배당금 연평균 성장률 (%).",
    "배당_연속증가": "배당금 연속 증가 연수.",
    "배당_수익동반증가": "배당과 이익이 동반 증가하면 O.",
    "매출_연속성장": "매출액 연속 성장 연수.",
    "영업이익_연속성장": "영업이익 연속 성장 연수.",
    "순이익_연속성장": "순이익 연속 성장 연수.",
    "영업CF_연속성장": "영업현금흐름 연속 성장 연수.",
    "이익률_개선": "전년 대비 영업이익률 개선 시 O.",
    "이익률_급개선": "영업이익률 2%p↑ 개선 시 O.",
    "흑자전환": "전년 적자 → 금년 흑자 전환 시 O.",
    "Q_매출_YoY(%)": "분기 매출액 전년동기 대비 증감률.",
    "Q_영업이익_YoY(%)": "분기 영업이익 전년동기 대비 증감률.",
    "Q_순이익_YoY(%)": "분기 순이익 전년동기 대비 증감률.",
    "TTM_매출_YoY(%)": "TTM 매출액 전년 대비 증감률.",
    "TTM_영업이익_YoY(%)": "TTM 영업이익 전년 대비 증감률.",
    "TTM_순이익_YoY(%)": "TTM 순이익 전년 대비 증감률.",
    "적정주가_SRIM": "S-RIM 기반 적정주가 (원).",
    "괴리율(%)": "현재가 vs 적정주가 괴리율. 양수면 현재가가 저평가.",
    "종합점수": "5개 전략 가중합산 종합점수 (0-100).",
    "주도주_점수": "시장 주도주 전략 점수.",
    "우량가치_점수": "우량가치 전략 점수.",
    "고성장_점수": "고성장 모멘텀 전략 점수.",
    "현금배당_점수": "현금배당 전략 점수.",
    "턴어라운드_점수": "턴어라운드 전략 점수.",
    "TTM_매출": "최근 12개월 매출액 (원).",
    "TTM_영업이익": "최근 12개월 영업이익 (원).",
    "TTM_순이익": "최근 12개월 순이익 (원).",
    "TTM_영업CF": "최근 12개월 영업현금흐름 (원).",
    "TTM_CAPEX": "최근 12개월 자본적지출 (원).",
    "TTM_FCF": "최근 12개월 잉여현금흐름 (원).",
    "자본": "자기자본 총계 (원).",
    "부채": "부채 총계 (원).",
    "자산총계": "자산 총계 (원).",
    "전략수": "Multi-Pick 선정 전략 수 (3↑이면 Multi-Pick).",
  };

  // ─── 세부 모달의 지표 그룹 ────────────────────────────────────────────
  const METRIC_GROUPS = [
    {
      title: "가격 & 밸류에이션",
      metrics: [
        { key: "종가",           label: "현재가",      fmt: "int" },
        { key: "시가총액",       label: "시가총액",    fmt: "eok" },
        { key: "PER",            label: "PER",          fmt: "f2" },
        { key: "PBR",            label: "PBR",          fmt: "f2" },
        { key: "PSR",            label: "PSR",          fmt: "f2" },
        { key: "PEG",            label: "PEG",          fmt: "f2" },
        { key: "EPS",            label: "EPS",          fmt: "int" },
        { key: "BPS",            label: "BPS",          fmt: "int" },
        { key: "이익수익률(%)",  label: "이익수익률%",  fmt: "f2" },
        { key: "적정주가_SRIM",  label: "S-RIM 적정가", fmt: "int" },
        { key: "괴리율(%)",      label: "괴리율%",      fmt: "f2" },
      ]
    },
    {
      title: "수익성",
      metrics: [
        { key: "ROE(%)",         label: "ROE%",        fmt: "f2" },
        { key: "영업이익률(%)",  label: "영업이익률%",  fmt: "f2" },
        { key: "FCF수익률(%)",   label: "FCF수익률%",   fmt: "f2" },
        { key: "이익품질_양호",  label: "이익품질",     fmt: "flag" },
        { key: "현금전환율(%)",  label: "현금전환율%",  fmt: "f1" },
        { key: "CAPEX비율(%)",   label: "CAPEX비율%",   fmt: "f1" },
      ]
    },
    {
      title: "성장성",
      metrics: [
        { key: "매출_CAGR",           label: "매출CAGR%",    fmt: "f1" },
        { key: "영업이익_CAGR",       label: "OP CAGR%",     fmt: "f1" },
        { key: "순이익_CAGR",         label: "NP CAGR%",     fmt: "f1" },
        { key: "FCF_CAGR",            label: "FCF CAGR%",    fmt: "f1" },
        { key: "Q_매출_YoY(%)",       label: "Q 매출YoY%",   fmt: "f1" },
        { key: "Q_영업이익_YoY(%)",   label: "Q OP YoY%",    fmt: "f1" },
        { key: "Q_순이익_YoY(%)",     label: "Q NP YoY%",    fmt: "f1" },
        { key: "TTM_매출_YoY(%)",     label: "TTM 매출YoY%", fmt: "f1" },
        { key: "TTM_영업이익_YoY(%)", label: "TTM OP YoY%",  fmt: "f1" },
      ]
    },
    {
      title: "재무건전성",
      metrics: [
        { key: "F스코어",      label: "F-Score",    fmt: "int" },
        { key: "부채비율(%)",  label: "부채비율%",  fmt: "f1" },
        { key: "부채상환능력", label: "이자보상배율", fmt: "f2" },
      ]
    },
    {
      title: "배당",
      metrics: [
        { key: "배당수익률(%)",    label: "배당수익률%",  fmt: "f2" },
        { key: "DPS_최근",         label: "DPS",          fmt: "int" },
        { key: "DPS_CAGR",         label: "DPS CAGR%",    fmt: "f1" },
        { key: "배당_연속증가",    label: "배당연속증가",  fmt: "int" },
        { key: "배당_수익동반증가", label: "수익동반증가", fmt: "flag" },
      ]
    },
    {
      title: "기술적 지표",
      metrics: [
        { key: "52주_최고대비(%)", label: "52주 최고대비%", fmt: "f1" },
        { key: "52주_최저대비(%)", label: "52주 최저대비%", fmt: "f1" },
        { key: "MA20_이격도(%)",   label: "MA20%",          fmt: "f1" },
        { key: "MA60_이격도(%)",   label: "MA60%",          fmt: "f1" },
        { key: "RSI_14",           label: "RSI(14)",         fmt: "f1" },
        { key: "거래대금_20일평균", label: "거래대금(20일평균)", fmt: "int" },
        { key: "거래대금_증감(%)", label: "거래대금 증감%",  fmt: "f1" },
        { key: "변동성_60일(%)",   label: "변동성(60일)%",  fmt: "f1" },
        { key: "수급강도",         label: "수급강도",        fmt: "f1" },
        { key: "외인순매수_20d",   label: "외인순매수(20d)", fmt: "int" },
        { key: "기관순매수_20d",   label: "기관순매수(20d)", fmt: "int" },
      ]
    },
    {
      title: "TTM 실적",
      metrics: [
        { key: "TTM_매출",    label: "TTM 매출",    fmt: "int" },
        { key: "TTM_영업이익", label: "TTM 영업이익", fmt: "int" },
        { key: "TTM_순이익",  label: "TTM 순이익",  fmt: "int" },
        { key: "TTM_영업CF",  label: "TTM 영업CF",  fmt: "int" },
        { key: "TTM_CAPEX",   label: "TTM CAPEX",   fmt: "int" },
        { key: "TTM_FCF",     label: "TTM FCF",     fmt: "int" },
        { key: "자본",        label: "자본",        fmt: "int" },
        { key: "부채",        label: "부채",        fmt: "int" },
        { key: "자산총계",    label: "자산총계",    fmt: "int" },
      ]
    },
    {
      title: "전략 점수",
      metrics: [
        { key: "종합점수",      label: "종합점수",      fmt: "f1" },
        { key: "주도주_점수",   label: "주도주 점수",   fmt: "f1" },
        { key: "우량가치_점수", label: "우량가치 점수", fmt: "f1" },
        { key: "고성장_점수",   label: "고성장 점수",   fmt: "f1" },
        { key: "현금배당_점수", label: "현금배당 점수", fmt: "f1" },
        { key: "턴어라운드_점수", label: "턴어라운드 점수", fmt: "f1" },
        { key: "전략수",        label: "전략수",        fmt: "int" },
      ]
    },
    {
      title: "연속 성장",
      metrics: [
        { key: "매출_연속성장",    label: "매출 연속성장",  fmt: "int" },
        { key: "영업이익_연속성장", label: "OP 연속성장",   fmt: "int" },
        { key: "순이익_연속성장",  label: "NP 연속성장",   fmt: "int" },
        { key: "영업CF_연속성장",  label: "CF 연속성장",   fmt: "int" },
        { key: "흑자전환",         label: "흑자전환",       fmt: "flag" },
        { key: "이익률_개선",      label: "이익률 개선",    fmt: "flag" },
        { key: "이익률_급개선",    label: "이익률 급개선",  fmt: "flag" },
        { key: "이익률_변동폭",    label: "이익률 변동폭",  fmt: "f1" },
      ]
    },
  ];

  // ─── Advanced Filter 카테고리 ──────────────────────────────────────────
  const FILTER_CATEGORIES = [
    { key: "all", label: "전체", fields: [] },
    {
      key: "valuation", label: "밸류에이션",
      fields: [
        { col: "PER",       label: "PER" },
        { col: "PBR",       label: "PBR" },
        { col: "PSR",       label: "PSR" },
        { col: "PEG",       label: "PEG" },
        { col: "괴리율(%)", label: "괴리율%" },
      ]
    },
    {
      key: "profitability", label: "수익성",
      fields: [
        { col: "ROE(%)",        label: "ROE%" },
        { col: "영업이익률(%)", label: "영업이익률%" },
        { col: "FCF수익률(%)",  label: "FCF수익률%" },
        { col: "이익수익률(%)", label: "이익수익률%" },
      ]
    },
    {
      key: "growth", label: "성장성",
      fields: [
        { col: "매출_CAGR",           label: "매출CAGR%" },
        { col: "영업이익_CAGR",       label: "OP CAGR%" },
        { col: "순이익_CAGR",         label: "NP CAGR%" },
        { col: "Q_영업이익_YoY(%)",   label: "Q OP YoY%" },
        { col: "TTM_매출_YoY(%)",     label: "TTM 매출YoY%" },
      ]
    },
    {
      key: "stability", label: "안정성",
      fields: [
        { col: "F스코어",      label: "F-Score" },
        { col: "부채비율(%)",  label: "부채비율%" },
        { col: "부채상환능력", label: "이자보상배율" },
      ]
    },
    {
      key: "technical", label: "기술적",
      fields: [
        { col: "RSI_14",           label: "RSI(14)" },
        { col: "수급강도",         label: "수급강도" },
        { col: "MA20_이격도(%)",   label: "MA20%" },
        { col: "52주_최저대비(%)", label: "52주 최저대비%" },
        { col: "변동성_60일(%)",   label: "변동성(60일)%" },
      ]
    },
    {
      key: "dividend", label: "배당",
      fields: [
        { col: "배당수익률(%)", label: "배당수익률%" },
        { col: "DPS_CAGR",     label: "DPS CAGR%" },
        { col: "배당_연속증가", label: "배당연속증가" },
      ]
    },
    {
      key: "market", label: "시가총액 / 점수",
      fields: [
        { col: "시가총액", label: "시가총액(억)", unit: 1e8 },
        { col: "종합점수", label: "종합점수" },
      ]
    },
  ];

  // 전체 필드 목록 (카테고리 탭 = "all"일 때 사용)
  const ALL_FILTER_FIELDS = FILTER_CATEGORIES
    .filter(c => c.key !== "all")
    .flatMap(c => c.fields.map(f => ({ ...f, catKey: c.key })));

  // 컬럼별 단위 배수 (시가총액: 억 단위 입력 → 원으로 변환)
  const UNIT_FACTORS = {};
  ALL_FILTER_FIELDS.forEach(f => { if (f.unit) UNIT_FACTORS[f.col] = f.unit; });

  // ─── 포맷팅 ──────────────────────────────────────────────────────────
  function fmt(v, type) {
    if (v === null || v === undefined) return "-";
    if (type === "int")  return Number(v).toLocaleString("ko-KR", { maximumFractionDigits: 0 });
    if (type === "f1")   return Number(v).toFixed(1);
    if (type === "f2")   return Number(v).toFixed(2);
    if (type === "flag") return Number(v) === 1 ? "O" : "-";
    if (type === "eok")  return Math.round(Number(v) / 1e8).toLocaleString("ko-KR") + "억";
    return String(v);
  }
  function valClass(v) {
    if (v == null) return "";
    return Number(v) > 0 ? "val-pos" : Number(v) < 0 ? "val-neg" : "";
  }

  // ─── 시장 요약 카드 ───────────────────────────────────────────────────
  async function loadMarketSummary() {
    try {
      const res  = await fetch("/api/markets/summary");
      const data = await res.json();
      const el   = document.getElementById("market-summary");
      if (!el) return;
      el.innerHTML = data.map(m => `
        <div class="col-md-6 col-lg-5">
          <div class="card summary-card ${m.market === "KOSDAQ" ? "kosdaq" : ""} p-2 mb-2">
            <div class="card-title">${m.market}
              <span class="badge bg-secondary ms-1">${m.stock_count.toLocaleString()}종목</span>
            </div>
            <div class="d-flex gap-3 small">
              <span><span class="text-muted">PER </span><strong>${m.avg_per != null ? Number(m.avg_per).toFixed(1) : "-"}</strong></span>
              <span><span class="text-muted">PBR </span><strong>${m.avg_pbr != null ? Number(m.avg_pbr).toFixed(2) : "-"}</strong></span>
              <span><span class="text-muted">ROE </span><strong>${m.avg_roe != null ? Number(m.avg_roe).toFixed(1) : "-"}%</strong></span>
            </div>
          </div>
        </div>`).join("");
    } catch (e) { console.error("Market summary:", e); }
  }

  // ─── 탭 종목 수 / 변동 배지 ──────────────────────────────────────────
  async function loadTabCounts() {
    try {
      const res = await fetch("/api/stocks/tab_counts");
      tabCounts = await res.json();
      renderTabBadges();
    } catch (e) { console.error("loadTabCounts:", e); }
  }

  function renderTabBadges() {
    document.querySelectorAll("#screen-tabs .nav-link[data-screen]").forEach(link => {
      const screen = link.dataset.screen;
      if (screen === "watchlist") return;

      // 기존 배지 제거
      link.querySelectorAll(".tab-count-badge, .tab-diff").forEach(el => el.remove());

      // 종목 수 배지
      if (tabCounts[screen] !== undefined) {
        const badge = document.createElement("span");
        badge.className = "badge bg-secondary ms-1 tab-count-badge";
        badge.style.cssText = "font-size:0.68rem;vertical-align:middle;";
        badge.textContent = Number(tabCounts[screen]).toLocaleString("ko-KR");
        link.appendChild(badge);
      }

      // +/- 변동 표시
      const s = batchChanges?.strategies?.[screen];
      if (s && (s.added_count > 0 || s.removed_count > 0)) {
        const diff = document.createElement("span");
        diff.className = "tab-diff ms-1";
        diff.style.cssText = "font-size:0.72rem;white-space:nowrap;";
        diff.innerHTML = [
          s.added_count   > 0 ? `<span class="text-success fw-bold">+${s.added_count}</span>`  : "",
          s.removed_count > 0 ? `<span class="text-danger fw-bold">-${s.removed_count}</span>` : "",
        ].join(" ");
        link.appendChild(diff);
      }
    });
  }

  // ─── 배치 변경 배너 ───────────────────────────────────────────────────
  async function loadBatchChanges() {
    try {
      const res  = await fetch("/api/batch/changes");
      batchChanges = await res.json();
      renderChangeBanner();
      renderTabBadges();
    } catch (e) {}
  }
  function renderChangeBanner() {
    const container = document.getElementById("change-banner-container");
    if (!container) return;
    if (!batchChanges || !batchChanges.has_changes) { container.style.display = "none"; return; }
    const s = batchChanges.strategies[currentScreen];
    if (!s || (s.added_count === 0 && s.removed_count === 0)) { container.style.display = "none"; return; }
    container.style.display = "";
    const addedHtml   = s.added.map(x   => `<span class="change-stock-chip added">${x.name}</span>`).join("");
    const removedHtml = s.removed.map(x => `<span class="change-stock-chip removed">${x.name}</span>`).join("");
    container.innerHTML = `
      <div class="change-banner">
        <span class="badge-change">배치 업데이트</span>
        ${s.added_count   > 0 ? `<span class="ms-2">신규 진입: ${addedHtml}</span>`  : ""}
        ${s.removed_count > 0 ? `<span class="ms-2">이탈: ${removedHtml}</span>` : ""}
      </div>`;
  }

  // ─── 종목 목록 로드 ───────────────────────────────────────────────────
  async function loadStocks() {
    // 로딩 스피너
    const loadingCols = (COLUMNS[currentScreen] || COLUMNS.all).length + 2;
    tbody.innerHTML = `<tr><td colspan="${loadingCols}" class="text-center py-4 text-muted">
      <span class="spinner-border spinner-border-sm me-2" role="status"></span>데이터 로딩 중...
    </td></tr>`;

    const market = document.getElementById("f-market").value;
    const q      = document.getElementById("f-search").value.trim();
    let apiScreen  = currentScreen;
    let codesParam = "";

    if (currentScreen === "watchlist") {
      const wl = [...getWatchlist()];
      if (wl.length === 0) { renderTable([]); return; }
      apiScreen  = "all";
      codesParam = wl.join(",");
    }

    pageSize = parseInt(document.getElementById("f-pagesize")?.value || "50", 10);
    const params = new URLSearchParams({
      screen: apiScreen, page: currentPage, size: pageSize,
      sort: sortCol, order: sortOrder,
    });
    if (market)     params.set("market", market);
    if (q)          params.set("q", q);
    if (codesParam) params.set("codes", codesParam);

    // Advanced Filter 파라미터
    Object.entries(columnFilters).forEach(([col, { min, max }]) => {
      const factor = UNIT_FACTORS[col] || 1;
      if (min !== "") params.set(`min_${col}`, parseFloat(min) * factor);
      if (max !== "") params.set(`max_${col}`, parseFloat(max) * factor);
    });

    try {
      const res  = await fetch(`/api/stocks?${params}`);
      const data = await res.json();
      renderTable(data.items);
      const totalPages = Math.ceil(data.total / data.size) || 1;
      pageInfo.textContent = `Page ${data.page} / ${totalPages} (${data.total.toLocaleString()}종목)`;
      btnPrev.disabled = data.page <= 1;
      btnNext.disabled = data.page >= totalPages;

      // 현재 탭 카운트 실시간 갱신 (서버 재시작 전에도 보임)
      if (currentScreen !== "watchlist") {
        tabCounts[currentScreen] = data.total;
        renderTabBadges();
      }
    } catch (e) { console.error("loadStocks:", e); }
  }

  // ─── 테이블 렌더링 ────────────────────────────────────────────────────
  function renderTable(items) {
    const cols = COLUMNS[currentScreen] || COLUMNS.all;
    if (!items.length) {
      tbody.innerHTML = `<tr><td colspan="${cols.length + 3}" class="text-center py-4 text-muted">데이터 없음</td></tr>`;
      return;
    }
    const wl       = getWatchlist();
    const newCodes = (batchChanges && batchChanges.strategies && batchChanges.strategies[currentScreen])
      ? new Set(batchChanges.strategies[currentScreen].added.map(x => x.code))
      : new Set();

    tbody.innerHTML = items.map(s => {
      const code    = s["종목코드"];
      const watched = wl.has(code);
      const isNew   = newCodes.has(code);
      const compareCb = `<td class="text-center p-1"><input type="checkbox" class="compare-cb" data-code="${code}" ${compareSet.has(code) ? "checked" : ""}></td>`;
      const star      = `<td class="text-center p-1"><button class="watch-btn${watched ? " watched" : ""}" data-code="${code}">${watched ? "★" : "☆"}</button></td>`;
      const cells = cols.map(c => {
        let cls = "";
        if (["거래대금_증감(%)", "수급강도", "MA20_이격도(%)", "MA60_이격도(%)",
             "Q_매출_YoY(%)", "Q_영업이익_YoY(%)", "Q_순이익_YoY(%)",
             "TTM_매출_YoY(%)", "TTM_영업이익_YoY(%)", "TTM_순이익_YoY(%)",
             "매출_CAGR", "영업이익_CAGR", "순이익_CAGR", "괴리율(%)"].includes(c.key)) {
          cls = valClass(s[c.key]);
        }
        const newBadge = (isNew && c.key === "종목명")
          ? ' <span class="badge badge-new bg-success">NEW</span>' : "";
        return `<td class="${cls}">${fmt(s[c.key], c.fmt)}${newBadge}</td>`;
      }).join("");
      return `<tr data-code="${code}" class="${isNew ? "row-new" : ""}">${compareCb}${star}${cells}</tr>`;
    }).join("");

    tbody.querySelectorAll(".watch-btn").forEach(btn =>
      btn.addEventListener("click", e => { e.stopPropagation(); toggleWatch(btn.dataset.code); })
    );
    tbody.querySelectorAll(".compare-cb").forEach(cb =>
      cb.addEventListener("change", e => { e.stopPropagation(); toggleCompare(cb.dataset.code, cb.checked); })
    );
    tbody.querySelectorAll("tr[data-code]").forEach(tr =>
      tr.addEventListener("click", e => {
        if (e.target.classList.contains("watch-btn") || e.target.classList.contains("compare-cb")) return;
        openDetail(tr.dataset.code);
      })
    );
    initTooltips();
  }

  // ─── 헤더 구성 ────────────────────────────────────────────────────────
  function buildHeader() {
    const cols = COLUMNS[currentScreen] || COLUMNS.all;
    headerRow.innerHTML =
      `<th width="20"></th><th width="30">★</th>` +
      cols.map(c => {
        const arrow = sortCol === c.key ? (sortOrder === "desc" ? " ↓" : " ↑") : "";
        const tip = METRIC_TOOLTIPS[c.key]
          ? ` data-bs-toggle="tooltip" data-bs-placement="bottom" title="${METRIC_TOOLTIPS[c.key]}"` : "";
        return `<th data-col="${c.key}" style="cursor:pointer"${tip}>${c.label}<span class="sort-arrow">${arrow}</span></th>`;
      }).join("");

    headerRow.querySelectorAll("th[data-col]").forEach(th => {
      th.addEventListener("click", () => {
        const col = th.dataset.col;
        if (sortCol === col) sortOrder = sortOrder === "asc" ? "desc" : "asc";
        else { sortCol = col; sortOrder = "desc"; }
        currentPage = 1;
        buildHeader();
        loadStocks();
      });
    });
    initTooltips();
  }

  // ─── 세부 모달 ────────────────────────────────────────────────────────
  async function openDetail(code) {
    currentDetailCode = code;
    try {
      const res = await fetch(`/api/stocks/${code}`);
      currentDetailData = await res.json();
      renderDetailModal(currentDetailData);
      new bootstrap.Modal(document.getElementById("detail-modal")).show();
    } catch (e) { console.error("openDetail:", e); }
  }

  function renderDetailModal(stock) {
    const code   = stock["종목코드"] || currentDetailCode;
    const name   = stock["종목명"]   || "Unknown";
    const market = stock["시장구분"] || "";
    const price  = stock["종가"];

    document.getElementById("detail-title").innerHTML =
      `<strong>${name}</strong> <span class="text-muted fs-6">${code}</span>
       <span class="badge ${market === "KOSPI" ? "bg-primary" : "bg-danger"} ms-2">${market}</span>
       ${price != null ? `<span class="ms-2 fw-bold">${fmt(price, "int")}원</span>` : ""}`;

    const btnWD = document.getElementById("btn-watch-detail");
    if (btnWD) { btnWD.dataset.code = code; updateAllStarButtons(); }

    // 지표 그룹별 렌더링
    const metricsEl = document.getElementById("detail-metrics");
    metricsEl.innerHTML = METRIC_GROUPS.map(group => {
      const pills = group.metrics.map(m => {
        const v = stock[m.key];
        if (v === null || v === undefined) return "";
        const tip = METRIC_TOOLTIPS[m.key]
          ? `data-bs-toggle="tooltip" data-bs-placement="top" title="${METRIC_TOOLTIPS[m.key]}"` : "";
        let cls = "";
        if (["매출_CAGR","영업이익_CAGR","순이익_CAGR","FCF_CAGR",
             "Q_매출_YoY(%)","Q_영업이익_YoY(%)","Q_순이익_YoY(%)",
             "TTM_매출_YoY(%)","TTM_영업이익_YoY(%)","TTM_순이익_YoY(%)",
             "수급강도","괴리율(%)","MA20_이격도(%)","MA60_이격도(%)"].includes(m.key)) {
          cls = valClass(v);
        }
        return `<div class="metric-pill" ${tip}>
          <div class="lbl">${m.label}</div>
          <div class="val ${cls}">${fmt(v, m.fmt)}</div>
        </div>`;
      }).filter(Boolean).join("");
      if (!pills) return "";
      return `<div class="w-100 mt-2 mb-1"><small class="fw-bold text-muted text-uppercase">${group.title}</small></div>${pills}`;
    }).join("");

    // 분석 버튼에 code 설정
    document.getElementById("btn-analysis-gemini").dataset.code = code;
    document.getElementById("btn-analysis-claude").dataset.code  = code;

    // 재무 차트 로드
    loadFinancialChart(code);
    initTooltips();
  }

  async function loadFinancialChart(code) {
    const area = document.getElementById("financial-chart-area");
    try {
      const res  = await fetch(`/api/stocks/${code}/financials`);
      const data = await res.json();
      if (!data.years || data.years.length === 0) { area.style.display = "none"; return; }
      area.style.display = "";
      if (financialChart) { financialChart.destroy(); financialChart = null; }
      const ctx = document.getElementById("financial-chart").getContext("2d");
      financialChart = new Chart(ctx, {
        type: "bar",
        data: {
          labels: data.years,
          datasets: data.series.map((s, i) => ({
            label: s.name,
            data: s.data.map(v => v != null ? Math.round(v / 1e8) : null),
            backgroundColor: ["rgba(13,110,253,0.7)", "rgba(25,135,84,0.7)", "rgba(220,53,69,0.7)"][i],
          }))
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: {
            title: { display: true, text: "연간 실적 추이 (억원)" },
            legend: { position: "bottom" },
          },
          scales: { y: { beginAtZero: false } }
        }
      });
    } catch (e) { area.style.display = "none"; }
  }

  // ─── AI 분석 ─────────────────────────────────────────────────────────
  async function requestAnalysis(code, mode) {
    const name = (currentDetailData && currentDetailData["종목명"]) || code;
    const reportModal = new bootstrap.Modal(document.getElementById("report-modal"));
    reportModal.show();

    document.getElementById("report-title").textContent   = `AI 분석 보고서 — ${name}`;
    document.getElementById("report-loading").style.display = "";
    document.getElementById("report-content").innerHTML   = "";
    document.getElementById("report-meta").textContent    = "";
    document.getElementById("report-loading-text").textContent =
      mode === "gemini" ? "Gemini로 분석 중 (Google Search 포함)..." : "Claude로 심층 분석 중...";

    try {
      // 캐시 확인
      let data;
      const getRes = await fetch(`/api/stocks/${code}/analysis`);
      if (getRes.ok) {
        const cached = await getRes.json();
        if (cached.mode === mode) {
          data = cached;
        }
      }
      // 캐시 없거나 모드 다르면 생성
      if (!data) {
        const postRes = await fetch(`/api/stocks/${code}/analysis`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ mode }),
        });
        data = await postRes.json();
      }

      document.getElementById("report-loading").style.display = "none";
      if (data.error) {
        document.getElementById("report-content").innerHTML =
          `<div class="alert alert-danger"><strong>오류:</strong> ${data.error}</div>`;
      } else {
        document.getElementById("report-content").innerHTML = data.report_html || "";
        document.getElementById("report-meta").textContent  =
          `${data.model || ""} · ${data.generated_date || ""}`;
      }
      document.getElementById("btn-regenerate").dataset.code = code;
      document.getElementById("btn-regenerate").dataset.mode = mode;
    } catch (e) {
      document.getElementById("report-loading").style.display = "none";
      document.getElementById("report-content").innerHTML =
        `<div class="alert alert-danger">오류: ${e.message}</div>`;
    }
  }

  // ─── 파이프라인 ───────────────────────────────────────────────────────
  async function triggerPipeline() {
    try {
      const res  = await fetch("/api/batch/trigger", { method: "POST" });
      const data = await res.json();
      if (data.status === "already_running") { alert("파이프라인이 이미 실행 중입니다."); return; }
      showPipelineModal();
    } catch (e) { alert("파이프라인 시작 실패: " + e.message); }
  }

  function showPipelineModal() {
    pipelineStartTime = Date.now();
    document.getElementById("pipeline-bar").style.width   = "0%";
    document.getElementById("pipeline-stage").textContent = "Starting...";
    document.getElementById("pipeline-pct").textContent   = "0%";
    document.getElementById("pipeline-elapsed").textContent = "0s";
    document.getElementById("pipeline-error").style.display  = "none";
    document.getElementById("pipeline-footer").style.display = "none";
    new bootstrap.Modal(document.getElementById("pipeline-progress-modal"), { keyboard: false }).show();
    if (pipelineTimer) clearTimeout(pipelineTimer);
    pollPipelineStatus();
  }

  async function pollPipelineStatus() {
    try {
      const res  = await fetch("/api/batch/status");
      const data = await res.json();
      const secs = Math.round((Date.now() - pipelineStartTime) / 1000);

      document.getElementById("pipeline-bar").style.width     = (data.progress || 0) + "%";
      document.getElementById("pipeline-stage").textContent   = data.stage || "Processing...";
      document.getElementById("pipeline-pct").textContent     = (data.progress || 0) + "%";
      document.getElementById("pipeline-elapsed").textContent = `${Math.floor(secs / 60)}m ${secs % 60}s`;

      if (data.error) {
        document.getElementById("pipeline-error").textContent   = "오류: " + data.error;
        document.getElementById("pipeline-error").style.display = "";
        document.getElementById("pipeline-footer").style.display = "";
      } else if (!data.running) {
        document.getElementById("pipeline-bar").style.width     = "100%";
        document.getElementById("pipeline-stage").textContent   = "완료!";
        document.getElementById("pipeline-pct").textContent     = "100%";
        document.getElementById("pipeline-footer").style.display = "";
        loadMarketSummary();
        loadBatchChanges();
        loadTabCounts();
        loadStocks();
      } else {
        pipelineTimer = setTimeout(pollPipelineStatus, 2000);
      }
    } catch (e) {
      pipelineTimer = setTimeout(pollPipelineStatus, 3000);
    }
  }

  // ─── Advanced Filter ─────────────────────────────────────────────────
  let advFilterOpen = false;

  function updateAdvButton() {
    const btn   = document.getElementById("btn-adv-toggle");
    if (!btn) return;
    const total = Object.keys(columnFilters).length;
    const arrow = advFilterOpen ? "▲" : "▼";
    btn.classList.toggle("active", advFilterOpen);
    if (total > 0) {
      btn.innerHTML = `Advanced <span class="badge bg-primary ms-1" style="font-size:0.7rem;vertical-align:middle;">${total}</span> ${arrow}`;
    } else {
      btn.textContent = `Advanced ${arrow}`;
    }
  }

  function toggleAdvFilter() {
    advFilterOpen = !advFilterOpen;
    const panel = document.getElementById("adv-filter-panel");
    panel.style.display = advFilterOpen ? "" : "none";
    if (advFilterOpen) renderFilterPanel();
    else updateAdvButton();
  }

  function getCurrentCatFields() {
    if (advCat === "all") return ALL_FILTER_FIELDS;
    const cat = FILTER_CATEGORIES.find(c => c.key === advCat);
    return cat ? cat.fields.map(f => ({ ...f, catKey: advCat })) : [];
  }

  function getActiveCount(catKey) {
    if (catKey === "all") return Object.keys(columnFilters).length;
    const cat = FILTER_CATEGORIES.find(c => c.key === catKey);
    if (!cat || !cat.fields) return 0;
    return cat.fields.filter(f =>
      columnFilters[f.col] && (columnFilters[f.col].min !== "" || columnFilters[f.col].max !== "")
    ).length;
  }

  function renderFilterPanel() {
    // 카테고리 탭
    const tabsEl = document.getElementById("adv-cat-tabs");
    tabsEl.innerHTML = FILTER_CATEGORIES.map(cat => {
      const cnt = getActiveCount(cat.key);
      return `<button class="adv-cat-tab${advCat === cat.key ? " active" : ""}" data-cat="${cat.key}">
        ${cat.label}${cnt > 0 ? `<span class="badge bg-primary adv-cat-badge">${cnt}</span>` : ""}
      </button>`;
    }).join("");
    tabsEl.querySelectorAll(".adv-cat-tab").forEach(btn =>
      btn.addEventListener("click", () => { advCat = btn.dataset.cat; renderFilterPanel(); })
    );

    // 활성 필터 수 배지 + 버튼 업데이트
    const total = Object.keys(columnFilters).length;
    const badge = document.getElementById("adv-active-count");
    if (badge) { badge.textContent = total; badge.style.display = total > 0 ? "" : "none"; }
    updateAdvButton();

    // 검색어 필터
    const searchQ = (document.getElementById("adv-search")?.value || "").toLowerCase();
    let fields = getCurrentCatFields();
    if (searchQ) fields = fields.filter(f =>
      f.label.toLowerCase().includes(searchQ) || f.col.toLowerCase().includes(searchQ)
    );

    const gridEl = document.getElementById("adv-filter-grid");
    if (!fields.length) { gridEl.innerHTML = `<div class="adv-filter-empty">지표 없음</div>`; return; }

    gridEl.innerHTML = fields.map(f => {
      const vals     = columnFilters[f.col] || { min: "", max: "" };
      const isActive = vals.min !== "" || vals.max !== "";
      const unitNote = f.unit ? ` (억)` : "";
      return `<div class="adv-filter-row${isActive ? " adv-filter-row--active" : ""}">
        <span class="adv-filter-label" title="${f.col}">${f.label}${unitNote}</span>
        <input type="number" step="any" class="adv-filter-input" data-col="${f.col}" data-type="min"
          value="${vals.min}" placeholder="최솟값">
        <span class="adv-filter-sep">~</span>
        <input type="number" step="any" class="adv-filter-input" data-col="${f.col}" data-type="max"
          value="${vals.max}" placeholder="최댓값">
      </div>`;
    }).join("");

    gridEl.querySelectorAll(".adv-filter-input").forEach(input =>
      input.addEventListener("change", () => {
        const col = input.dataset.col;
        if (!columnFilters[col]) columnFilters[col] = { min: "", max: "" };
        columnFilters[col][input.dataset.type] = input.value;
        if (columnFilters[col].min === "" && columnFilters[col].max === "") delete columnFilters[col];
        renderFilterPanel();
        currentPage = 1;
        loadStocks();
      })
    );
  }

  function resetAdvFilter() {
    Object.keys(columnFilters).forEach(k => delete columnFilters[k]);
    if (advFilterOpen) renderFilterPanel();
    else updateAdvButton();
    currentPage = 1;
    loadStocks();
  }

  // ─── 비교 기능 ────────────────────────────────────────────────────────
  function toggleCompare(code, checked) {
    if (checked) compareSet.add(code); else compareSet.delete(code);
    updateCompareBar();
  }

  function updateCompareBar() {
    const bar = document.getElementById("compare-bar");
    const cnt = document.getElementById("compare-count");
    if (!bar) return;
    if (compareSet.size > 0) {
      bar.style.display = "";
      cnt.textContent   = `${compareSet.size}개 선택됨`;
    } else {
      bar.style.display = "none";
    }
    document.querySelectorAll(".compare-cb").forEach(cb => {
      cb.checked = compareSet.has(cb.dataset.code);
    });
  }

  async function openCompareModal() {
    if (compareSet.size < 2) { alert("2개 이상 종목을 선택해주세요."); return; }
    const codes = [...compareSet].join(",");
    try {
      const res  = await fetch(`/api/stocks/compare?codes=${codes}`);
      const data = await res.json();
      renderCompareModal(data);
      new bootstrap.Modal(document.getElementById("compare-modal")).show();
    } catch (e) { alert("비교 데이터 로드 실패: " + e.message); }
  }

  const COMPARE_METRICS_BY_CAT = {
    valuation: [
      { key: "종가",           label: "현재가",       fmt: "int" },
      { key: "시가총액",       label: "시가총액",     fmt: "eok" },
      { key: "PER",            label: "PER",           fmt: "f2" },
      { key: "PBR",            label: "PBR",           fmt: "f2" },
      { key: "PEG",            label: "PEG",           fmt: "f2" },
      { key: "괴리율(%)",      label: "괴리율%",       fmt: "f2" },
      { key: "적정주가_SRIM",  label: "S-RIM 적정가",  fmt: "int" },
    ],
    profitability: [
      { key: "ROE(%)",         label: "ROE%",          fmt: "f2" },
      { key: "영업이익률(%)",  label: "영업이익률%",   fmt: "f2" },
      { key: "FCF수익률(%)",   label: "FCF수익률%",    fmt: "f2" },
      { key: "이익수익률(%)",  label: "이익수익률%",   fmt: "f2" },
      { key: "현금전환율(%)",  label: "현금전환율%",   fmt: "f1" },
      { key: "F스코어",        label: "F-Score",        fmt: "int" },
    ],
    growth: [
      { key: "매출_CAGR",           label: "매출CAGR%",    fmt: "f1" },
      { key: "영업이익_CAGR",       label: "OP CAGR%",     fmt: "f1" },
      { key: "순이익_CAGR",         label: "NP CAGR%",     fmt: "f1" },
      { key: "Q_영업이익_YoY(%)",   label: "Q OP YoY%",    fmt: "f1" },
      { key: "TTM_영업이익_YoY(%)", label: "TTM OP YoY%",  fmt: "f1" },
    ],
    stability: [
      { key: "F스코어",      label: "F-Score",     fmt: "int" },
      { key: "부채비율(%)",  label: "부채비율%",   fmt: "f1" },
      { key: "부채상환능력", label: "이자보상배율", fmt: "f2" },
      { key: "TTM_FCF",      label: "TTM FCF",      fmt: "int" },
    ],
    technical: [
      { key: "RSI_14",           label: "RSI(14)",       fmt: "f1" },
      { key: "수급강도",         label: "수급강도",       fmt: "f1" },
      { key: "MA20_이격도(%)",   label: "MA20%",          fmt: "f1" },
      { key: "52주_최저대비(%)", label: "52주 최저대비%", fmt: "f1" },
      { key: "변동성_60일(%)",   label: "변동성(60일)%",  fmt: "f1" },
    ],
    dividend: [
      { key: "배당수익률(%)", label: "배당수익률%", fmt: "f2" },
      { key: "DPS_최근",     label: "DPS",          fmt: "int" },
      { key: "DPS_CAGR",     label: "DPS CAGR%",    fmt: "f1" },
      { key: "배당_연속증가", label: "배당연속증가", fmt: "int" },
    ],
  };

  function renderCompareModal(data) {
    const stocks  = data.stocks     || [];
    const metaMeta = data.metrics_meta || {};
    const cats = [
      { key: "all",           label: "전체" },
      { key: "valuation",     label: "밸류에이션" },
      { key: "profitability", label: "수익성" },
      { key: "growth",        label: "성장성" },
      { key: "stability",     label: "안정성" },
      { key: "technical",     label: "기술적" },
      { key: "dividend",      label: "배당" },
    ];
    const tabsEl = document.getElementById("compare-category-tabs");
    if (tabsEl) {
      tabsEl.innerHTML = cats.map((c, i) =>
        `<li class="nav-item"><a class="nav-link${i === 0 ? " active" : ""}" href="#" data-cat="${c.key}">${c.label}</a></li>`
      ).join("");
      tabsEl.querySelectorAll(".nav-link").forEach(a =>
        a.addEventListener("click", e => {
          e.preventDefault();
          tabsEl.querySelectorAll(".nav-link").forEach(x => x.classList.remove("active"));
          a.classList.add("active");
          renderCompareTable(stocks, metaMeta, a.dataset.cat);
        })
      );
    }
    renderCompareTable(stocks, metaMeta, "all");
  }

  function renderCompareTable(stocks, metaMeta, catKey) {
    const allMetrics = Object.values(COMPARE_METRICS_BY_CAT).flat();
    const deduped = [];
    const seen = new Set();
    allMetrics.forEach(m => { if (!seen.has(m.key)) { seen.add(m.key); deduped.push(m); } });

    const metrics = catKey === "all" ? deduped : (COMPARE_METRICS_BY_CAT[catKey] || deduped);
    const container = document.getElementById("compare-table-container");
    if (!container) return;

    const headCols = `<th>지표</th>${stocks.map(s =>
      `<th>${s["종목명"]}<br><small class="text-muted">${s["종목코드"]}</small></th>`
    ).join("")}`;

    const rows = metrics.map(m => {
      const vals = stocks.map(s => s[m.key]);
      const nums = vals.map(v => v != null ? Number(v) : null).filter(v => v !== null);
      const best = metaMeta[m.key]?.best;
      const maxV = nums.length ? Math.max(...nums) : null;
      const minV = nums.length ? Math.min(...nums) : null;

      const cells = vals.map(v => {
        if (v == null) return `<td>-</td>`;
        const n = Number(v);
        let cls = "";
        if (best === "high" && maxV !== null && n === maxV) cls = "best-val";
        else if (best === "low" && minV !== null && n === minV) cls = "best-val";
        else if (best === "high" && minV !== null && n === minV && nums.length > 1) cls = "worst-val";
        else if (best === "low" && maxV !== null && n === maxV && nums.length > 1) cls = "worst-val";
        return `<td class="${cls}">${fmt(v, m.fmt)}</td>`;
      }).join("");
      return `<tr><td><strong>${m.label}</strong></td>${cells}</tr>`;
    }).join("");

    container.innerHTML = `
      <div class="table-responsive">
        <table class="table table-sm compare-table">
          <thead><tr>${headCols}</tr></thead>
          <tbody>${rows}</tbody>
        </table>
      </div>`;
  }

  // ─── 툴팁 초기화 ─────────────────────────────────────────────────────
  function initTooltips() {
    document.querySelectorAll('[data-bs-toggle="tooltip"]').forEach(el => {
      if (!el._bsTooltip) {
        el._bsTooltip = new bootstrap.Tooltip(el, {
          trigger: "hover",
          placement: el.dataset.bsPlacement || "top",
        });
      }
    });
  }

  // ─── DOM 참조 ─────────────────────────────────────────────────────────
  const tbody    = document.getElementById("stock-tbody");
  const headerRow = document.getElementById("table-header");
  const pageInfo = document.getElementById("page-info");
  const btnPrev  = document.getElementById("btn-prev");
  const btnNext  = document.getElementById("btn-next");

  // ─── 이벤트 리스너 ────────────────────────────────────────────────────

  // 페이지네이션
  btnPrev.addEventListener("click", () => { if (currentPage > 1) { currentPage--; loadStocks(); } });
  btnNext.addEventListener("click", () => { currentPage++; loadStocks(); });

  // 상단 버튼
  document.getElementById("btn-refresh").addEventListener("click", () => {
    loadMarketSummary(); loadBatchChanges(); loadStocks();
  });
  document.getElementById("btn-trigger").addEventListener("click", triggerPipeline);

  // 검색 폼
  document.getElementById("filter-form").addEventListener("submit", e => {
    e.preventDefault(); currentPage = 1; loadStocks();
  });
  document.getElementById("btn-clear").addEventListener("click", () => {
    document.getElementById("f-market").value = "";
    document.getElementById("f-search").value = "";
    currentPage = 1; loadStocks();
  });

  document.getElementById("f-pagesize")?.addEventListener("change", () => {
    currentPage = 1; loadStocks();
  });

  // Advanced Filter
  document.getElementById("btn-adv-toggle")?.addEventListener("click", toggleAdvFilter);
  document.getElementById("btn-adv-reset")?.addEventListener("click", resetAdvFilter);
  document.getElementById("adv-search")?.addEventListener("input", () => renderFilterPanel());

  // 비교 기능
  document.getElementById("btn-compare")?.addEventListener("click", openCompareModal);
  document.getElementById("btn-compare-clear")?.addEventListener("click", () => {
    compareSet.clear();
    updateCompareBar();
    document.querySelectorAll(".compare-cb").forEach(cb => { cb.checked = false; });
  });

  // 관심종목 버튼 (세부 모달)
  document.getElementById("btn-watch-detail").addEventListener("click", function () {
    if (this.dataset.code) toggleWatch(this.dataset.code);
  });

  // AI 분석 버튼
  document.getElementById("btn-analysis-gemini").addEventListener("click", function () {
    requestAnalysis(this.dataset.code || currentDetailCode, "gemini");
  });
  document.getElementById("btn-analysis-claude").addEventListener("click", function () {
    requestAnalysis(this.dataset.code || currentDetailCode, "claude");
  });

  // 보고서 재생성 / PDF
  document.getElementById("btn-regenerate").addEventListener("click", function () {
    if (this.dataset.code && this.dataset.mode) {
      requestAnalysis(this.dataset.code, this.dataset.mode);
    }
  });
  document.getElementById("btn-pdf").addEventListener("click", () => window.print());

  // 전략 탭
  document.querySelectorAll("#screen-tabs .nav-link").forEach(l =>
    l.addEventListener("click", e => {
      e.preventDefault();
      document.querySelectorAll("#screen-tabs .nav-link").forEach(x => x.classList.remove("active"));
      l.classList.add("active");
      currentScreen = l.dataset.screen;
      sortCol       = TAB_DEFAULT_SORT[currentScreen];
      sortOrder     = "desc";
      currentPage   = 1;
      buildHeader();
      loadStocks();
      renderChangeBanner();
      const desc = STRATEGY_DESCRIPTIONS[currentScreen];
      if (desc) {
        document.getElementById("strategy-desc").innerHTML =
          `<strong>${desc.title}</strong>&nbsp;&nbsp;<small class="text-muted">${desc.criteria}</small>`;
      }
    })
  );

  // ─── 초기화 ──────────────────────────────────────────────────────────
  buildHeader();
  loadMarketSummary();
  loadBatchChanges();
  loadTabCounts();
  loadStocks();
  updateWatchlistCount();

  const initDesc = STRATEGY_DESCRIPTIONS.all;
  document.getElementById("strategy-desc").innerHTML =
    `<strong>${initDesc.title}</strong>&nbsp;&nbsp;<small class="text-muted">${initDesc.criteria}</small>`;

})();
