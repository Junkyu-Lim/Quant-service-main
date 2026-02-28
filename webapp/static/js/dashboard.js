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
  let reportMap         = {};  // { "005930": {model, date}, ... }

  // ─── 탭 기본 정렬 ─────────────────────────────────────────────────────
  const TAB_DEFAULT_SORT = {
    all:              "종합점수",
    leaders:          "종합점수",
    quality_value:    "종합점수",
    growth_mom:       "종합점수",
    cash_div:         "종합점수",
    turnaround:       "종합점수",
    multi_strategy:   "종합점수",
    forward_covered:  "Fwd_모멘텀_점수",
    watchlist:        "종합점수",
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

  // ─── Watchlist CSV 내보내기 ───────────────────────────────────────────
  function exportTableCSV() {
    const rows = document.querySelectorAll("#stock-tbody tr[data-code]");
    if (!rows.length) { alert("내보낼 데이터가 없습니다."); return; }
    const headers = [...document.querySelectorAll("#table-header th")]
      .map(th => th.textContent.trim())
      .filter((_, i) => i >= 2); // 체크박스, 별표 열 제외
    const lines = [headers.join(",")];
    rows.forEach(tr => {
      const cells = [...tr.querySelectorAll("td")].slice(2); // 체크박스, 별표 제외
      lines.push(cells.map(td => `"${td.textContent.trim().replace(/"/g, '""')}"`).join(","));
    });
    const bom = "\uFEFF";
    const blob = new Blob([bom + lines.join("\n")], { type: "text/csv;charset=utf-8;" });
    const url  = URL.createObjectURL(blob);
    const a    = document.createElement("a");
    a.href = url; a.download = `quant_${currentScreen}_${new Date().toISOString().slice(0,10)}.csv`;
    a.click(); URL.revokeObjectURL(url);
  }

  // ─── 전략 설명 ────────────────────────────────────────────────────────
  const STRATEGY_DESCRIPTIONS = {
    all:            { title: "📊 전체 종목",            criteria: "전체 시장 종목 조회" },
    leaders:        { title: "🔥 시장 주도주 (Leaders)",  criteria: "시총 1,000억↑ · 순이익 흑자 · RS 80↑ · 수급(+) · 거래 5억↑" },
    quality_value:  { title: "💎 우량가치 (Quality & Value)", criteria: "일반(ROIC 10%·PEG<1.2) + 금융(ROE 8%·PBR<1.5) 듀얼 트랙" },
    growth_mom:     { title: "🚀 고성장 모멘텀 (Growth)", criteria: "매출/이익 동반성장(10%↑) · RS 50↑ · 흑자도산 방지" },
    cash_div:       { title: "💰 현금배당 (Cash & Div)",  criteria: "FCF수익률 3%↑ · 배당수익률 1%↑ · 배당성향 < 80% · 현금전환율 70%↑ · 부채비율 < 120%" },
    turnaround:     { title: "🔄 턴어라운드 (Turnaround)", criteria: "흑자전환 OR 이익률 급개선 · TTM 순이익 흑자" },
    multi_strategy:  { title: "🏆 Multi-Pick (3관왕 이상)", criteria: "5개 전략 중 3개 이상 동시 선정 종목" },
    forward_covered: { title: "🔭 Forward Est. (컨센서스 추정치)", criteria: "애널리스트 커버 ~535종목 내 Forward 모멘텀 점수 순위" },
    watchlist:       { title: "⭐ 관심 종목",             criteria: "사용자가 직접 추가한 종목" },
  };

  // ─── 전략별 상세 가이드 콘텐츠 ──────────────────────────────────────────
  const STRATEGY_GUIDES = {
    all: `
      <h6>📌 기본 활용법</h6>
      <ul>
        <li><strong>검색 & 필터:</strong> 상단 검색창에 종목명/코드를 입력하거나, 'Advanced ▼' 버튼을 눌러 상세 조건(PER, PBR, 시총 등)으로 필터링하세요.</li>
        <li><strong>관심종목:</strong> 종목명 옆의 <span class="text-warning">☆</span> 별표를 눌러 관심종목에 추가하면 '관심종목' 탭에서 모아볼 수 있습니다.</li>
        <li><strong>상세 분석:</strong> 종목 행을 클릭하면 재무 차트, F-Score 상세, AI 분석 리포트 등 세부 정보를 볼 수 있습니다.</li>
      </ul>
      <h6>💡 팁</h6>
      <p>전체 목록에서도 컬럼 헤더를 클릭하여 <strong>PER 낮은 순</strong>, <strong>ROE 높은 순</strong> 등으로 정렬해보세요.</p>
    `,
    leaders: `
      <h6>🔥 시장 주도주 (Leaders) 공략법</h6>
      <p>시장의 관심(수급)과 실적 성장이 동시에 받쳐주는 주도주를 찾습니다.</p>
      <ul>
        <li><strong>핵심 지표:</strong> <span class="badge bg-light text-dark border">수급강도</span>, <span class="badge bg-light text-dark border">거래대금</span>, <span class="badge bg-light text-dark border">영업이익 성장률</span></li>
        <li><strong>매매 포인트:</strong>
          <ul>
            <li><strong>수급강도 양수(+) 유지:</strong> 외국인/기관이 꾸준히 사고 있다는 신호입니다.</li>
            <li><strong>RSI 70 이하:</strong> 과열권(70 이상)보다는, 상승 추세 중 일시적 조정(눌림목) 구간을 노리세요.</li>
            <li><strong>이격도 체크:</strong> MA20 이격도가 너무 높지 않은(105% 이하) 종목이 안전합니다.</li>
          </ul>
        </li>
      </ul>
    `,
    quality_value: `
      <h6>💎 우량가치주 (Quality & Value) 발굴</h6>
      <p>싸면서도 돈을 잘 벌고 재무가 튼튼한 '육각형 미인' 종목을 찾습니다.</p>
      <ul>
        <li><strong>핵심 지표:</strong> <span class="badge bg-light text-dark border">PEG</span>, <span class="badge bg-light text-dark border">ROE</span>, <span class="badge bg-light text-dark border">F-Score</span></li>
        <li><strong>Value Trap 피하기:</strong>
          <ul>
            <li>PER/PBR이 낮다고 무조건 좋은 게 아닙니다. <strong>ROE가 10% 이상</strong> 유지되는지 꼭 확인하세요.</li>
            <li><strong>PEG 0.5 ~ 1.0:</strong> 이익 성장률 대비 저평가된 구간입니다. 0.5 이하면 강력 매수 후보입니다.</li>
            <li><strong>F-Score 7점 이상:</strong> 재무 건전성이 매우 뛰어난 기업입니다.</li>
          </ul>
        </li>
      </ul>
    `,
    growth_mom: `
      <h6>🚀 고성장 모멘텀주 (Growth) 투자</h6>
      <p>매출과 이익이 폭발적으로 성장하며 주가 추세가 살아있는 종목입니다.</p>
      <ul>
        <li><strong>핵심 지표:</strong> <span class="badge bg-light text-dark border">영업이익 CAGR</span>, <span class="badge bg-light text-dark border">분기 매출 YoY</span>, <span class="badge bg-light text-dark border">MA60 이격도</span></li>
        <li><strong>체크리스트:</strong>
          <ul>
            <li><strong>실적 가속화:</strong> 연간 성장률(CAGR)보다 최근 분기 성장률(YoY)이 더 높은 종목이 좋습니다.</li>
            <li><strong>정배열 초입:</strong> MA60 이격도가 100% 근처에서 상승 머리를 들고 있는 종목을 주목하세요.</li>
            <li><strong>부채비율 관리:</strong> 성장을 위해 빚을 너무 많이 쓰진 않았는지(부채비율 200% 이하 권장) 확인하세요.</li>
          </ul>
        </li>
      </ul>
    `,
    cash_div: `
      <h6>💰 현금배당주 (Cash & Dividend) 선별</h6>
      <p>배당만 많이 주는 게 아니라, <strong>실제 현금 창출력</strong>과 <strong>배당 지속 가능성</strong>이 검증된 기업입니다.</p>
      <ul>
        <li><strong>핵심 지표:</strong> <span class="badge bg-light text-dark border">FCF수익률</span> <span class="badge bg-light text-dark border">배당성향%</span> <span class="badge bg-light text-dark border">ROIC</span> <span class="badge bg-light text-dark border">현금전환율</span></li>
        <li><strong>진짜 배당주 구별법:</strong>
          <ul>
            <li><strong>배당성향 < 50%:</strong> EPS의 절반 이하를 배당하면 이익 재투자와 배당 삭감 위험 모두 방어됩니다. 80% 초과는 <span class="badge bg-warning text-dark">경고</span> 신호.</li>
            <li><strong>ROIC ≥ 10%:</strong> 자본 대비 수익률이 높아야 5~10년 뒤에도 배당이 유지됩니다. 해자(Moat) 확인 지표.</li>
            <li><strong>현금전환율 ≥ 70%:</strong> 순이익의 70% 이상이 실제 현금으로 들어와야 진짜 배당 재원이 됩니다.</li>
            <li><strong>배당경고 = 0:</strong> 배당성향 > 80%, 배당수익률 > 10% + RS 하위권, 현금전환율 < 70% 중 하나라도 해당하면 경고 플래그가 켜집니다.</li>
            <li><strong>동반성장 = ✓:</strong> 이익 성장과 배당 성장이 함께 이루어진 종목만이 진정한 복리 배당주입니다.</li>
          </ul>
        </li>
      </ul>
    `,
    turnaround: `
      <h6>🔄 턴어라운드 (Turnaround) 포착</h6>
      <p>최악의 상황을 지나 실적이 급격히 개선되는 종목을 바닥권에서 잡습니다.</p>
      <ul>
        <li><strong>핵심 지표:</strong> <span class="badge bg-light text-dark border">흑자전환</span>, <span class="badge bg-light text-dark border">이익률 변동폭</span>, <span class="badge bg-light text-dark border">괴리율</span></li>
        <li><strong>주의사항:</strong>
          <ul>
            <li><strong>본업 개선 확인:</strong> 일회성 자산 매각으로 인한 흑자전환은 제외해야 합니다. (영업이익 흑자전환 중요)</li>
            <li><strong>높은 괴리율:</strong> 실적은 좋아졌는데 주가는 아직 반응하지 않아 괴리율이 높은(저평가된) 종목을 찾으세요.</li>
            <li><strong>수급 유입:</strong> 기관이나 외국인의 매수세가 들어오기 시작했다면 신뢰도가 높아집니다.</li>
          </ul>
        </li>
      </ul>
    `,
    multi_strategy: `
      <h6>🏆 Multi-Pick (다관왕) 활용</h6>
      <p>5가지 전략 중 <strong>3개 이상의 기준을 동시에 만족</strong>하는 '슈퍼 종목'입니다.</p>
      <ul>
        <li><strong>의미:</strong> 성장성, 안정성, 가치, 배당 등 여러 측면에서 결점이 적다는 뜻입니다.</li>
        <li><strong>활용법:</strong>
          <ul>
            <li>어떤 전략들이 중복되었는지 확인해보세요. (예: 고성장 + 우량가치 + 시장주도주 = 주가 상승 탄력 최고조)</li>
            <li>종합점수가 최상위권일 확률이 높으므로, 포트폴리오의 핵심 종목으로 고려해볼 만합니다.</li>
          </ul>
        </li>
      </ul>
    `,
    forward_covered: `
      <h6>🔭 Forward 컨센서스 추정치 활용법</h6>
      <p>애널리스트 컨센서스 추정치가 있는 <strong>~535개 커버리지 종목</strong>에 한해 내년도 실적 전망 기준으로 순위를 매깁니다.</p>
      <ul>
        <li><strong>⚠️ 커버리지 편향 주의:</strong> 이 탭은 대형·중형주 위주의 애널리스트 커버 종목만 표시됩니다. 소형주·성장 초기 기업은 포함되지 않으며, 기존 5개 전략 탭과 직접 비교하지 마세요.</li>
        <li><strong>Fwd_모멘텀_점수 구성:</strong>
          <ul>
            <li><span class="badge bg-light text-dark border">Fwd_OP성장률</span> × 35% — 영업이익 성장 모멘텀</li>
            <li><span class="badge bg-light text-dark border">Fwd_ROE%</span> × 25% — 내년 자본수익률</li>
            <li><span class="badge bg-light text-dark border">Fwd_PER (역순)</span> × 20% — 성장 대비 저평가</li>
            <li><span class="badge bg-light text-dark border">Fwd_OPM%</span> × 10% — 수익성</li>
            <li><span class="badge bg-light text-dark border">Fwd_2yr_OP성장</span> × 10% — 2년 성장 지속성</li>
          </ul>
        </li>
        <li><strong>추정치 신뢰도:</strong> 애널리스트 수가 많을수록, 최근 발표일에 가까울수록 신뢰도가 높습니다. 단일 애널리스트 추정치는 변동성이 클 수 있습니다.</li>
        <li><strong>활용법:</strong> 기존 탭에서 발굴한 종목의 Forward 지표를 확인하여 실적 개선 기대감이 주가에 이미 반영되었는지(Fwd_PER 과도한 할증) 교차 검증하세요.</li>
      </ul>
    `,
    watchlist: `
      <h6>⭐ 관심종목 관리</h6>
      <p>직접 선별한 종목들의 현황을 한눈에 모니터링합니다.</p>
      <ul>
        <li>다른 탭에서 <span class="text-warning">☆</span> 버튼을 눌러 추가한 종목들이 여기에 표시됩니다.</li>
        <li>정기적으로 리스트를 점검하여 투자 매력이 떨어진 종목은 제외하고, 새로운 유망 종목으로 교체하세요.</li>
        <li>'비교하기' 기능을 사용하여 관심 종목들 간의 지표 우열을 가려보세요.</li>
      </ul>
    `
  };

  // ─── 탭별 기본 컬럼 정의 ─────────────────────────────────────────────
  const COLUMNS = {
    // 1. 전체 종목 - 균형잡힌 기본 정보 (14개)
    all: [
      { key: "종목코드", label: "코드" }, { key: "종목명", label: "종목명" }, { key: "시장구분", label: "시장" },
      { key: "종가", label: "현재가", fmt: "int" }, { key: "시가총액", label: "시총", fmt: "eok" },
      { key: "PER", label: "PER", fmt: "f2" }, { key: "PBR", label: "PBR", fmt: "f2" },
      { key: "PEG", label: "PEG", fmt: "f2" }, { key: "ROE(%)", label: "ROE", fmt: "f2" },
      { key: "영업이익_CAGR", label: "OP성장", fmt: "f1" }, { key: "부채비율(%)", label: "부채%", fmt: "f1" },
      { key: "수급강도", label: "수급", fmt: "f1" }, { key: "거래대금_20일평균", label: "거래(평)", fmt: "eok" },
      { key: "종합점수", label: "점수", fmt: "f1" }
    ],
    // 2. 시장 주도주 - 수급+모멘텀+실적 (12개)
    leaders: [
      { key: "종목코드", label: "코드" }, { key: "종목명", label: "종목명" }, { key: "시가총액", label: "시총", fmt: "eok" },
      { key: "종가", label: "현재가", fmt: "int" }, { key: "시장구분", label: "시장" },
      { key: "수급강도", label: "수급", fmt: "f1" }, { key: "RS_등급", label: "RS등급", fmt: "f1" },
      { key: "스마트머니_승률", label: "SM승률", fmt: "f1" }, { key: "거래대금_증감(%)", label: "거래증감", fmt: "f1" },
      { key: "Q_영업이익_YoY(%)", label: "Q OP YoY", fmt: "f1" }, { key: "52주_최고대비(%)", label: "고가대비", fmt: "f1" },
      { key: "주도주_점수", label: "주도점수", fmt: "f1" }
    ],
    // 3. 우량가치 - ROE, F-Score, PEG, ROIC (12개)
    quality_value: [
      { key: "종목코드", label: "코드" }, { key: "종목명", label: "종목명" }, { key: "종가", label: "현재가", fmt: "int" },
      { key: "시가총액", label: "시총", fmt: "eok" }, { key: "PER", label: "PER", fmt: "f2" },
      { key: "PEG", label: "PEG", fmt: "f2" }, { key: "PBR", label: "PBR", fmt: "f2" },
      { key: "ROE(%)", label: "ROE", fmt: "f2" }, { key: "ROIC(%)", label: "ROIC", fmt: "f1" },
      { key: "F스코어", label: "F-Score", fmt: "int" }, { key: "부채비율(%)", label: "부채%", fmt: "f1" },
      { key: "영업이익률(%)", label: "OPM%", fmt: "f1" }, { key: "우량가치_점수", label: "우량점수", fmt: "f1" }
    ],
    // 4. 고성장 모멘텀 - CAGR, YoY, 추세, 가속도 (13개)
    growth_mom: [
      { key: "종목코드", label: "코드" }, { key: "종목명", label: "종목명" }, { key: "종가", label: "현재가", fmt: "int" },
      { key: "매출_CAGR", label: "매출CAGR", fmt: "f1" }, { key: "영업이익_CAGR", label: "OP CAGR", fmt: "f1" },
      { key: "순이익_CAGR", label: "NP CAGR", fmt: "f1" }, { key: "Q_영업이익_YoY(%)", label: "Q OP YoY", fmt: "f1" },
      { key: "실적가속_연속", label: "실적가속", fmt: "flag" }, { key: "영업이익_가속도", label: "OP가속도", fmt: "f1" },
      { key: "MA20_이격도(%)", label: "MA20이격", fmt: "f1" }, { key: "MA60_이격도(%)", label: "MA60이격", fmt: "f1" },
      { key: "52주_최고대비(%)", label: "고가대비", fmt: "f1" }, { key: "FCF_CAGR", label: "FCF CAGR", fmt: "f1" },
      { key: "고성장_점수", label: "성장점수", fmt: "f1" }
    ],
    // 5. 현금배당 - FCF, 배당, 현금흐름 (15개)
    cash_div: [
      { key: "종목코드", label: "코드" }, { key: "종목명", label: "종목명" }, { key: "종가", label: "현재가", fmt: "int" },
      { key: "FCF수익률(%)", label: "FCF%", fmt: "f2" }, { key: "배당수익률(%)", label: "배당%", fmt: "f2" },
      { key: "배당성향(%)", label: "배당성향%", fmt: "f1" }, { key: "배당_경고신호", label: "경고", fmt: "flag" },
      { key: "ROIC(%)", label: "ROIC%", fmt: "f1" }, { key: "DPS_CAGR", label: "DPS CAGR", fmt: "f1" },
      { key: "배당_연속증가", label: "배당연속", fmt: "int" }, { key: "현금전환율(%)", label: "현금전환%", fmt: "f1" },
      { key: "부채비율(%)", label: "부채%", fmt: "f1" }, { key: "이익품질_양호", label: "이익품질", fmt: "flag" },
      { key: "배당_수익동반증가", label: "동반성장", fmt: "flag" }, { key: "현금배당_점수", label: "배당점수", fmt: "f1" }
    ],
    // 6. 턴어라운드 - 전환신호, 이익률, 수급 (12개)
    turnaround: [
      { key: "종목코드", label: "코드" }, { key: "종목명", label: "종목명" }, { key: "종가", label: "현재가", fmt: "int" },
      { key: "흑자전환", label: "흑자전환", fmt: "flag" }, { key: "이익률_급개선", label: "OPM급등", fmt: "flag" },
      { key: "이익률_변동폭", label: "OPM변동", fmt: "f1" }, { key: "GPM_변화(pp)", label: "GPM변화", fmt: "f1" },
      { key: "스마트머니_승률", label: "SM승률", fmt: "f1" }, { key: "VCP_신호", label: "VCP", fmt: "flag" },
      { key: "ROIC_개선", label: "ROIC↑", fmt: "flag" }, { key: "TTM_순이익", label: "TTM NI", fmt: "int" },
      { key: "실적가속_연속", label: "실적가속", fmt: "flag" }, { key: "RSI_14", label: "RSI", fmt: "f1" },
      { key: "턴어라운드_점수", label: "턴점수", fmt: "f1" }
    ],
    // 7. Multi-Strategy (3관왕) - 5개 전략 점수 (10개)
    multi_strategy: [
      { key: "종목코드", label: "코드" }, { key: "종목명", label: "종목명" }, { key: "종가", label: "현재가", fmt: "int" },
      { key: "전략수", label: "전략수", fmt: "int" }, { key: "종합점수", label: "종합점수", fmt: "f1" },
      { key: "성장성_점수", label: "성장점수", fmt: "f1" }, { key: "안정성_점수", label: "안정점수", fmt: "f1" },
      { key: "가격_점수", label: "가격점수", fmt: "f1" }, { key: "주도주_점수", label: "주도점수", fmt: "f1" },
      { key: "우량가치_점수", label: "우량점수", fmt: "f1" }
    ],
    // 8. Forward 추정치 - 커버리지 종목 내 모멘텀 (12개)
    forward_covered: [
      { key: "종목코드", label: "코드" }, { key: "종목명", label: "종목명" },
      { key: "종가", label: "현재가", fmt: "int" }, { key: "시가총액", label: "시총", fmt: "eok" },
      { key: "Fwd_PER", label: "Fwd PER", fmt: "f2" }, { key: "Fwd_PBR", label: "Fwd PBR", fmt: "f2" },
      { key: "Fwd_ROE(%)", label: "Fwd ROE%", fmt: "f1" }, { key: "Fwd_OPM(%)", label: "Fwd OPM%", fmt: "f1" },
      { key: "Fwd_영업이익_성장률(%)", label: "Fwd OP성장%", fmt: "f1" },
      { key: "Fwd_매출_성장률(%)", label: "Fwd 매출성장%", fmt: "f1" },
      { key: "Fwd_2yr_영업이익_성장(%)", label: "2yr OP성장%", fmt: "f1" },
      { key: "Fwd_모멘텀_점수", label: "Fwd점수", fmt: "f1" }
    ],
    // 9. 관심종목 - 종합 모니터링 (10개)
    watchlist: [
      { key: "종목코드", label: "코드" }, { key: "종목명", label: "종목명" }, { key: "종가", label: "현재가", fmt: "int" },
      { key: "시가총액", label: "시총", fmt: "eok" }, { key: "PER", label: "PER", fmt: "f2" },
      { key: "PBR", label: "PBR", fmt: "f2" }, { key: "ROE(%)", label: "ROE", fmt: "f2" },
      { key: "부채비율(%)", label: "부채%", fmt: "f1" }, { key: "배당수익률(%)", label: "배당%", fmt: "f2" },
      { key: "종합점수", label: "점수", fmt: "f1" }
    ]
  };

    // ─── 지표 평가 로직 (Good: Green, Bad: Red) ─────────────────────────────
    const METRIC_CRITERIA = {
      // [Key]: [Good Condition, Bad Condition] (Functions returning boolean)
      // Valuation
      "PER":           [v => v > 0 && v <= 15, v => v < 0 || v >= 50],
      "PBR":           [v => v > 0 && v <= 1.2, v => v >= 3.0],
      "PEG":           [v => v > 0 && v <= 0.5, v => v >= 1.5],
      "PSR":           [v => v > 0 && v <= 1.0, v => v >= 5.0],
      "괴리율(%)":     [v => v >= 20, v => v < 0],
      "이익수익률(%)": [v => v >= 10, v => v <= 3],

      // Profitability
      "ROE(%)":        [v => v >= 15, v => v <= 5],
      "영업이익률(%)": [v => v >= 10, v => v <= 0],
      "순이익률(%)":   [v => v >= 10, v => v <= 0],
      "FCF수익률(%)":  [v => v >= 5, v => v <= 0],
      "현금전환율(%)": [v => v >= 100, v => v <= 50],
      "CAPEX비율(%)":  [v => v <= 30, v => v >= 80], // Low is good

      // Stability
      "부채비율(%)":   [v => v <= 100, v => v >= 200], // Low is good
      "부채상환능력":  [v => v >= 0.5, v => v <= 0.1],
      "이자보상배율":  [v => v >= 5, v => v < 1],
      "F스코어":       [v => v >= 7, v => v <= 4],

      // Growth (CAGR/YoY)
      "매출_CAGR":          [v => v >= 15, v => v <= 0],
      "영업이익_CAGR":      [v => v >= 15, v => v <= 0],
      "순이익_CAGR":        [v => v >= 15, v => v <= 0],
      "FCF_CAGR":           [v => v >= 15, v => v <= 0],
      "Q_매출_YoY(%)":      [v => v >= 15, v => v <= 0],
      "Q_영업이익_YoY(%)":  [v => v >= 15, v => v <= 0],
      "Q_순이익_YoY(%)":    [v => v >= 15, v => v <= 0],
      "TTM_매출_YoY(%)":    [v => v >= 15, v => v <= 0],
      "TTM_영업이익_YoY(%)":[v => v >= 15, v => v <= 0],
      "TTM_순이익_YoY(%)":  [v => v >= 15, v => v <= 0],

      // Dividend
      "배당수익률(%)": [v => v >= 3, v => v == 0],
      "DPS_CAGR":     [v => v >= 10, v => v <= 0],
      "배당_연속증가": [v => v >= 5, v => v == 0],
      "배당성향(%)":   [v => v > 0 && v <= 50, v => v >= 80],  // Good: ≤50%, Bad: ≥80%
      "배당_경고신호": [v => v === 0, v => v === 1],             // Good: 경고없음, Bad: 경고

      // Technical
      "RSI_14":           [v => v <= 30, v => v >= 70], // Oversold(Buy)=Good, Overbought=Bad
      "수급강도":         [v => v >= 1.0, v => v < 0],
      "외인순매수_20d":   [v => v > 0, v => v < 0],
      "기관순매수_20d":   [v => v > 0, v => v < 0],
      "스마트머니_승률":   [v => v >= 0.6, v => v <= 0.3],
      "양매수_비율":       [v => v >= 0.2, v => v == 0],
      "거래대금_증감(%)": [v => v >= 50, v => v <= -20],
      "52주_최고대비(%)": [v => v >= -5, v => v <= -30], // Near High = Momentum Good
      "MA20_이격도(%)":   [v => v >= 0, v => v <= -5],   // Trend support
      "MA60_이격도(%)":   [v => v >= 0, v => v <= -10],
      "RS_60d":           [v => v > 0, v => v < 0],
      "RS_120d":          [v => v > 0, v => v < 0],
      "RS_250d":          [v => v > 0, v => v < 0],
      "Composite_RS":     [v => v >= 70, v => v <= 30],
      "RS_등급":          [v => v >= 70, v => v <= 30],

      // Forward 추정치 (커버리지 종목만 유효)
      "Fwd_PER":                    [v => v > 0 && v <= 12, v => v < 0 || v >= 30],
      "Fwd_PBR":                    [v => v > 0 && v <= 1.2, v => v >= 3.0],
      "Fwd_ROE(%)":                 [v => v >= 15, v => v <= 5],
      "Fwd_OPM(%)":                 [v => v >= 10, v => v <= 0],
      "Fwd_영업이익_성장률(%)":     [v => v >= 15, v => v <= 0],
      "Fwd_매출_성장률(%)":         [v => v >= 10, v => v <= 0],
      "Fwd_순이익_성장률(%)":       [v => v >= 15, v => v <= 0],
      "Fwd_2yr_영업이익_성장(%)":   [v => v >= 10, v => v <= 0],
      "Fwd_모멘텀_점수":            [v => v >= 80, v => v <= 40],

      // Scores
      "종합점수":      [v => v >= 80, v => v <= 50],
      "성장성_점수":   [v => v >= 80, v => v <= 50],
      "안정성_점수":   [v => v >= 80, v => v <= 50],
      "가격_점수":     [v => v >= 80, v => v <= 50],
      "주도주_점수":   [v => v >= 80, v => v <= 50],
      "우량가치_점수": [v => v >= 80, v => v <= 50],
      "고성장_점수":   [v => v >= 80, v => v <= 50],
      "현금배당_점수": [v => v >= 80, v => v <= 50],
      "턴어라운드_점수":[v => v >= 80, v => v <= 50],

      // Binary Flags (1=Good, 0=Bad/Neutral)
      "F1_수익성": [v => v==1, v => v==0],
      "F2_영업CF": [v => v==1, v => v==0],
      "F3_ROA개선": [v => v==1, v => v==0],
      "F4_이익품질": [v => v==1, v => v==0],
      "F5_레버리지": [v => v==1, v => v==0],
      "F6_유동성": [v => v==1, v => v==0],
      "F7_희석없음": [v => v==1, v => v==0],
      "F8_매출총이익률": [v => v==1, v => v==0],
      "F9_자산회전율": [v => v==1, v => v==0],
      "이익품질_양호": [v => v==1, v => v==0],
      "배당_수익동반증가": [v => v==1, v => v==0],
      "흑자전환": [v => v==1, v => v==0],
      "이익률_개선": [v => v==1, v => v==0],
      "이익률_급개선": [v => v==1, v => v==0],
      "실적가속_연속": [v => v==1, v => v==0],
      "ROIC_개선": [v => v==1, v => v==0],
      "퀄리티_턴어라운드": [v => v==1, v => v==0],
      "VCP_신호": [v => v==1, v => v==0],
    };

    function getMetricColor(key, val) {
        if (val === null || val === undefined) return "";
        const v = Number(val);
        const criteria = METRIC_CRITERIA[key];
        if (!criteria) return ""; // No specific criteria -> Neutral
        if (criteria[0](v)) return "text-success fw-bold"; // Good (Green + Bold)
        if (criteria[1](v)) return "text-danger";  // Bad (Red)
        return "text-muted"; // Neutral (Gray)
    }

    // ─── 지표 툴팁 ────────────────────────────────────────────────────────
    const METRIC_TOOLTIPS = {
      // 0. 기본 정보
      "종가": "현재 주가(원). 거래 종료 시점의 마지막 매매 가격.",
      "시가총액": "총 시가(원). 현재 주가 × 상장주식수. 기업 규모의 지표.",
      "자본": "자기자본(원). 자산에서 부채를 뺀 주주 자산.",
      "부채": "총 부채(원). 유동부채 + 장기부채 합계.",
      "자산총계": "총자산(원). 유동자산 + 비유동자산 합계.",

      // 1. 밸류에이션
      "PER": "주가수익비율. 낮을수록 저평가. (Good: ≤ 15, Bad: ≥ 50)",
      "PBR": "주가순자산비율. 1 미만은 저평가. (Good: ≤ 1.2, Bad: ≥ 3.0)",
      "PSR": "주가매출비율. (Good: ≤ 1.0, Bad: ≥ 5.0)",
      "PEG": "PER ÷ 이익성장률. 성장성 감안 저평가. (Good: ≤ 0.5, Bad: ≥ 1.5)",
      "ROE(%)": "자기자본이익률. 워렌 버핏 중시 지표. (Good: ≥ 15%, Bad: ≤ 5%)",
      "EPS": "주당순이익(원). 1주가 벌어들인 순이익. 우상향 권장.",
      "BPS": "주당순자산(원). 기업 청산 가치.",
      "적정주가_SRIM": "S-RIM 모형으로 산출한 적정 주가.",
      "괴리율(%)": "적정주가 대비 현재가 차이. (Good: ≥ 20%, Bad: < 0%)",
      "이익수익률(%)": "PER의 역수. (Good: ≥ 10%, Bad: ≤ 3%)",

      // 2. 재무 건전성 & 현금흐름
      "F스코어": "재무 건전성 종합 점수(9점 만점). (Good: ≥ 7점, Bad: ≤ 4점)",
      "F1_수익성": "당기순이익 > 0 (흑자).",
      "F2_영업CF": "영업활동현금흐름 > 0.",
      "F3_ROA개선": "전년 대비 ROA 증가.",
      "F4_이익품질": "영업CF > 순이익.",
      "F5_레버리지": "전년 대비 부채비율 감소.",
      "F6_유동성": "전년 대비 유동비율 증가.",
      "F7_희석없음": "주식수 미증가 (희석 없음).",
      "F8_매출총이익률": "전년 대비 GPM 증가.",
      "F9_자산회전율": "전년 대비 자산회전율 증가.",
      "부채비율(%)": "자본 대비 부채 비율. (Good: ≤ 100%, Bad: ≥ 200%)",
      "유동비율(%)": "유동자산 / 유동부채 × 100. 단기 채무 상환 능력. (Good: ≥ 150%, Bad: ≤ 100%)",
      "부채상환능력": "영업활동현금흐름 / 총부채. (Good: ≥ 0.5, Bad: ≤ 0.1)",
      "이자보상배율": "영업이익 / 이자비용. (Good: ≥ 5, Bad: < 1)",
      "이익품질_양호": "영업CF > 순이익 여부. 현금 흐름 건전성.",
      "FCF수익률(%)": "FCF / 시가총액. (Good: ≥ 5%, Bad: ≤ 0%)",
      "현금전환율(%)": "영업CF / 순이익. (Good: ≥ 100%, Bad: ≤ 50%)",
      "CAPEX비율(%)": "CAPEX / 영업CF. 낮을수록 좋음. (Good: ≤ 30%, Bad: ≥ 80%)",

      // 3. 수익성 & 배당
      "영업이익률(%)": "매출 대비 영업이익. (Good: ≥ 10%, Bad: ≤ 0%)",
      "배당수익률(%)": "주가 대비 배당금. (Good: ≥ 3%, Bad: 0%)",
      "DPS_최근": "최근 주당 배당금.",
      "DPS_CAGR": "배당 성장률. (Good: ≥ 10%, Bad: ≤ 0%)",
      "배당_연속증가": "연속 배당 증가 연수. (Good: ≥ 5년)",
      "배당_수익동반증가": "순이익과 배당이 함께 성장.",
      "배당성향(%)": "EPS 대비 배당금 비율(DPS/EPS×100). (Good: ≤ 50%, Bad: ≥ 80%). 80%↑은 이익 이상 배당(Payout Trap) 위험.",
      "배당_경고신호": "Value/Payout Trap 감지. 배당성향>80% OR 배당수익률>10%+RS하위권 OR 현금전환율<70% 중 하나 해당 시 1(경고). 0=이상없음.",

      // 4. 성장성 (CAGR / YoY)
      "매출_CAGR": "매출 연평균 성장률. (Good: ≥ 15%, Bad: ≤ 0%)",
      "영업이익_CAGR": "영업이익 연평균 성장률. (Good: ≥ 15%, Bad: ≤ 0%)",
      "순이익_CAGR": "순이익 연평균 성장률. (Good: ≥ 15%, Bad: ≤ 0%)",
      "영업CF_CAGR": "영업활동현금흐름 연평균 성장률. (Good: ≥ 15%, Bad: ≤ 0%)",
      "FCF_CAGR": "FCF 연평균 성장률. (Good: ≥ 15%, Bad: ≤ 0%)",
      "Q_매출_YoY(%)": "분기 매출 전년비 성장. (Good: ≥ 15%, Bad: ≤ 0%)",
      "Q_영업이익_YoY(%)": "분기 영업이익 전년비 성장. (Good: ≥ 15%, Bad: ≤ 0%)",
      "Q_순이익_YoY(%)": "분기 순이익 전년비 성장. (Good: ≥ 15%, Bad: ≤ 0%)",
      "TTM_매출_YoY(%)": "TTM 매출 전년비 성장. (Good: ≥ 15%, Bad: ≤ 0%)",
      "TTM_영업이익_YoY(%)": "TTM 영업이익 전년비 성장. (Good: ≥ 15%, Bad: ≤ 0%)",
      "TTM_순이익_YoY(%)": "TTM 순이익 전년비 성장. (Good: ≥ 15%, Bad: ≤ 0%)",
      "Q_매출_연속YoY성장": "최근 분기까지 YoY 매출 성장이 연속된 분기 수. (Good: ≥ 4분기)",
      "Q_영업이익_연속YoY성장": "최근 분기까지 YoY 영업이익 성장이 연속된 분기 수. (Good: ≥ 4분기)",
      "Q_순이익_연속YoY성장": "최근 분기까지 YoY 순이익 성장이 연속된 분기 수. (Good: ≥ 4분기)",
      "흑자전환": "적자에서 흑자로 전환.",
      "이익률_개선": "전년 대비 영업이익률 상승.",
      "이익률_급개선": "영업이익률 2%p 이상 급등.",
      "이익률_변동폭": "영업이익률 변화폭(pp).",

      // 5. 성장 가속도
      "영업이익_가속도": "영업이익 성장 가속도.",
      "매출_가속도": "매출 성장 가속도.",
      "실적가속_연속": "2분기 연속 실적 가속.",
      "매출_연속성장": "매출 연속 성장 연수.",
      "영업이익_연속성장": "영업이익 연속 성장 연수.",
      "순이익_연속성장": "순이익 연속 성장 연수.",
      "영업CF_연속성장": "영업CF 연속 성장 연수.",

      // 6. 효율성 & 마진
      "ROIC(%)": "투자자본수익률(NOPAT/IC). (Good: ≥ 15%, Bad: ≤ 5%)",
      "ROIC_전년(%)": "전년 ROIC.",
      "ROIC_개선": "전년 대비 ROIC 상승.",
      "영업이익률_최근": "최근 연도 영업이익률(%). 추세 확인용. (Good: ≥ 10%, Bad: ≤ 0%)",
      "영업이익률_전년": "전년도 영업이익률(%). 최근값과 비교해 개선·악화 파악. (Good: ≥ 10%, Bad: ≤ 0%)",
      "GPM_최근(%)": "최근 매출총이익률.",
      "GPM_전년(%)": "전년 매출총이익률.",
      "GPM_변화(pp)": "GPM 변화폭.",
      "퀄리티_턴어라운드": "GPM개선 + 영업CF흑자 + ROIC개선.",

      // 7. 기술적 & 수급
      "52주_최고대비(%)": "고점 대비 위치. (Good: -5% 이내, Bad: -30% 이하)",
      "52주_최저대비(%)": "저점 대비 상승폭.",
      "MA20_이격도(%)": "20일선 이격. (Good: ≥ 0%, Bad: ≤ -5%)",
      "MA60_이격도(%)": "60일선 이격. (Good: ≥ 0%, Bad: ≤ -10%)",
      "RSI_14": "상대강도지수. (Good: ≤ 30[과매도], Bad: ≥ 70[과매수])",
      "거래대금_20일평균": "일평균 거래대금(단위: 억).",
      "거래대금_증감(%)": "최근 거래대금 급증. (Good: ≥ 50%, Bad: ≤ -20%)",
      "변동성_60일(%)": "주가 변동성.",
      "수급강도": "외인+기관 순매수 강도. (Good: ≥ 1.0, Bad: < 0)",
      "외인순매수_20d": "최근 20일 외국인 순매수 금액(억).",
      "기관순매수_20d": "최근 20일 기관 순매수 금액(억).",
      "RS_60d": "60일 상대강도. (Good: > 0%, Bad: < 0%)",
      "RS_120d": "120일 상대강도. (Good: > 0%, Bad: < 0%)",
      "RS_250d": "250일 상대강도. (Good: > 0%, Bad: < 0%)",
      "Composite_RS": "종합 상대강도 점수. (Good: ≥ 70, Bad: ≤ 30)",
      "RS_등급": "RS 백분위 등급. (Good: ≥ 70, Bad: ≤ 30)",
      "스마트머니_승률": "최근 20일 중 외인 또는 기관이 순매수한 날의 비율. (Good: ≥ 60%)",
      "양매수_비율": "최근 20일 중 외인과 기관이 동시에 순매수한 날의 비율. 수급의 질적 강도.",
      "VCP_신호": "변동성 축소(CV20<CV60) + 거래량 감소(Vol20<Vol60) + 스마트머니 승률 60% 이상을 만족하는 급등 전조 패턴.",

      // 8. TTM 실적
      "TTM_매출": "최근 4분기 합산 매출.",
      "TTM_영업이익": "최근 4분기 합산 영업이익.",
      "TTM_순이익": "최근 4분기 합산 순이익.",
      "TTM_영업CF": "최근 4분기 합산 영업CF.",
      "TTM_CAPEX": "최근 4분기 합산 CAPEX.",
      "TTM_FCF": "최근 4분기 합산 FCF.",

      // 10. Forward 컨센서스 추정치 (⚠️ 커버리지 종목만 유효)
      "컨센서스_커버리지": "애널리스트 컨센서스 추정치 존재 여부 (1=커버, 0=미커버). 커버 종목은 대형·중형주 위주 ~535종목.",
      "Fwd_PER": "Forward PER. 내년 예상 EPS 기준 주가수익비율. (Good: ≤ 12, Bad: ≥ 30) ⚠️ 커버리지 종목만 유효.",
      "Fwd_PBR": "Forward PBR. 내년 예상 BPS 기준 주가순자산비율. (Good: ≤ 1.2, Bad: ≥ 3.0) ⚠️ 커버리지 종목만 유효.",
      "Fwd_EPS": "Forward EPS. 내년 예상 주당순이익(원). ⚠️ 커버리지 종목만 유효.",
      "Fwd_ROE(%)": "Forward ROE. 내년 예상 자기자본이익률. (Good: ≥ 15%, Bad: ≤ 5%) ⚠️ 커버리지 종목만 유효.",
      "Fwd_OPM(%)": "Forward 영업이익률. 내년 예상 수익성. (Good: ≥ 10%, Bad: ≤ 0%) ⚠️ 커버리지 종목만 유효.",
      "Fwd_영업이익_성장률(%)": "TTM 대비 내년 예상 영업이익 성장률. (Good: ≥ 15%, Bad: ≤ 0%) ⚠️ 커버리지 종목만 유효.",
      "Fwd_매출_성장률(%)": "TTM 대비 내년 예상 매출 성장률. (Good: ≥ 10%, Bad: ≤ 0%) ⚠️ 커버리지 종목만 유효.",
      "Fwd_순이익_성장률(%)": "TTM 대비 내년 예상 순이익(지배주주) 성장률. (Good: ≥ 15%, Bad: ≤ 0%) ⚠️ 커버리지 종목만 유효.",
      "Fwd_2yr_영업이익_성장(%)": "내년 대비 내후년 예상 영업이익 성장률(2년 지속 성장 확인). (Good: ≥ 10%, Bad: ≤ 0%) ⚠️ 커버리지 종목만 유효.",
      "Fwd_모멘텀_점수": "Forward 모멘텀 종합 점수. 커버리지 ~535종목 내 백분위 랭킹. (Good: ≥ 80, Bad: ≤ 40) ⚠️ forward_covered 탭에서만 계산됨.",

      // 11. 데이터 품질
      "데이터_연수": "재무데이터 누적 연수. 값이 클수록 CAGR 등 장기 지표의 신뢰도 높음. (Good: ≥ 5년)",
      "최근분기": "TTM·분기 지표의 기준이 되는 가장 최근 분기 코드 (예: 2024Q3).",
      "PER_이상": "PER 계산 불가 또는 음수 여부 플래그. 1이면 PER 신뢰 불가.",
      "순이익_전년음수": "전년도 순이익이 적자(음수)였는지 여부. 1이면 전년 적자.",
      "순이익_당기양수": "당기 순이익이 흑자(양수)인지 여부. 1이면 당기 흑자.",

      // 9. 점수 (Good: ≥ 80, Bad: ≤ 50)
      "종합점수": "전체 전략 종합 점수.",
      "성장성_점수": "성장성 부문 점수.",
      "안정성_점수": "안정성 부문 점수.",
      "가격_점수": "밸류에이션 점수.",
      "주도주_점수": "시장 주도주 점수.",
      "우량가치_점수": "우량 가치주 점수.",
      "고성장_점수": "고성장 모멘텀 점수.",
      "현금배당_점수": "배당주 점수.",
      "턴어라운드_점수": "턴어라운드 점수.",
      "전략수": "해당되는 전략 개수.",
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
        { key: "매출_CAGR",           label: "매출CAGR%",       fmt: "f1" },
        { key: "영업이익_CAGR",       label: "OP CAGR%",        fmt: "f1" },
        { key: "순이익_CAGR",         label: "NP CAGR%",        fmt: "f1" },
        { key: "영업CF_CAGR",         label: "영업CF CAGR%",    fmt: "f1" },
        { key: "FCF_CAGR",            label: "FCF CAGR%",       fmt: "f1" },
        { key: "Q_매출_YoY(%)",       label: "Q 매출YoY%",      fmt: "f1" },
        { key: "Q_영업이익_YoY(%)",   label: "Q OP YoY%",       fmt: "f1" },
        { key: "Q_순이익_YoY(%)",     label: "Q NP YoY%",       fmt: "f1" },
        { key: "TTM_매출_YoY(%)",     label: "TTM 매출YoY%",    fmt: "f1" },
        { key: "TTM_영업이익_YoY(%)", label: "TTM OP YoY%",     fmt: "f1" },
        { key: "TTM_순이익_YoY(%)",   label: "TTM NP YoY%",     fmt: "f1" },
      ]
    },
    {
      title: "재무건전성",
      metrics: [
        { key: "F스코어",      label: "F-Score",    fmt: "int" },
        { key: "F1_수익성",    label: "F1 수익성",  fmt: "flag" },
        { key: "F2_영업CF",    label: "F2 영업CF",  fmt: "flag" },
        { key: "F3_ROA개선",   label: "F3 ROA개선", fmt: "flag" },
        { key: "F4_이익품질",  label: "F4 이익품질", fmt: "flag" },
        { key: "F5_레버리지", label: "F5 레버리지", fmt: "flag" },
        { key: "F6_유동성",    label: "F6 유동성",  fmt: "flag" },
        { key: "F7_희석없음",  label: "F7 희석없음", fmt: "flag" },
        { key: "F8_매출총이익률", label: "F8 매출총이익률", fmt: "flag" },
        { key: "F9_자산회전율", label: "F9 자산회전율", fmt: "flag" },
        { key: "부채비율(%)",  label: "부채비율%",  fmt: "f1" },
        { key: "유동비율(%)",  label: "유동비율%",  fmt: "f1" },
        { key: "부채상환능력", label: "부채상환능력", fmt: "f2" },
        { key: "이자보상배율", label: "이자보상배율", fmt: "f2" },
      ]
    },
    {
      title: "배당",
      metrics: [
        { key: "배당수익률(%)",    label: "배당수익률%",  fmt: "f2" },
        { key: "DPS_최근",         label: "DPS",          fmt: "int" },
        { key: "배당성향(%)",      label: "배당성향%",    fmt: "f1" },
        { key: "배당_경고신호",    label: "배당경고",     fmt: "flag" },
        { key: "DPS_CAGR",         label: "DPS CAGR%",    fmt: "f1" },
        { key: "배당_연속증가",    label: "배당연속증가",  fmt: "int" },
        { key: "배당_수익동반증가", label: "수익동반증가", fmt: "flag" },
        { key: "현금전환율(%)",    label: "현금전환%",    fmt: "f1" },
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
        { key: "거래대금_20일평균", label: "거래대금(20일평균)", fmt: "eok" },
        { key: "거래대금_증감(%)", label: "거래대금 증감%",  fmt: "f1" },
        { key: "변동성_60일(%)",   label: "변동성(60일)%",  fmt: "f1" },
      ]
    },
    {
      title: "모멘텀 & 상대강도",
      metrics: [
        { key: "RS_60d",        label: "RS(60d)",         fmt: "f1" },
        { key: "RS_120d",       label: "RS(120d)",        fmt: "f1" },
        { key: "RS_250d",       label: "RS(250d)",        fmt: "f1" },
        { key: "Composite_RS",  label: "Composite RS",    fmt: "f1" },
        { key: "RS_등급",       label: "RS 등급",         fmt: "f1" },
      ]
    },
    {
      title: "수급 & 매집",
      metrics: [
        { key: "수급강도",       label: "수급강도",         fmt: "f1" },
        { key: "외인순매수_20d", label: "외인순매수(20d)",  fmt: "eok" },
        { key: "기관순매수_20d", label: "기관순매수(20d)",  fmt: "eok" },
        { key: "스마트머니_승률", label: "스마트머니 승률",  fmt: "f1" },
        { key: "양매수_비율",    label: "양매수 비율",      fmt: "f1" },
        { key: "VCP_신호",       label: "VCP 신호",        fmt: "flag" },
      ]
    },
    {
      title: "TTM 실적 (억원)",
      metrics: [
        { key: "TTM_매출",    label: "TTM 매출 (억)",    fmt: "int" },
        { key: "TTM_영업이익", label: "TTM 영업이익 (억)", fmt: "int" },
        { key: "TTM_순이익",  label: "TTM 순이익 (억)",  fmt: "int" },
        { key: "TTM_영업CF",  label: "TTM 영업CF (억)",  fmt: "int" },
        { key: "TTM_CAPEX",   label: "TTM CAPEX (억)",   fmt: "int" },
        { key: "TTM_FCF",     label: "TTM FCF (억)",     fmt: "int" },
        { key: "자본",        label: "자본 (억)",        fmt: "int" },
        { key: "부채",        label: "부채 (억)",        fmt: "int" },
        { key: "자산총계",    label: "자산총계 (억)",    fmt: "int" },
      ]
    },
    {
      title: "전략 점수",
      metrics: [
        { key: "종합점수",      label: "종합점수",      fmt: "f1" },
        { key: "성장성_점수",   label: "성장성 점수",   fmt: "f1" },
        { key: "안정성_점수",   label: "안정성 점수",   fmt: "f1" },
        { key: "가격_점수",     label: "가격 점수",     fmt: "f1" },
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
        { key: "매출_연속성장",         label: "매출 연속성장",       fmt: "int" },
        { key: "영업이익_연속성장",     label: "OP 연속성장",         fmt: "int" },
        { key: "순이익_연속성장",       label: "NP 연속성장",         fmt: "int" },
        { key: "영업CF_연속성장",       label: "CF 연속성장",         fmt: "int" },
        { key: "Q_매출_연속YoY성장",    label: "Q 매출 연속YoY성장",  fmt: "int" },
        { key: "Q_영업이익_연속YoY성장", label: "Q OP 연속YoY성장",   fmt: "int" },
        { key: "Q_순이익_연속YoY성장",  label: "Q NP 연속YoY성장",   fmt: "int" },
        { key: "흑자전환",         label: "흑자전환",       fmt: "flag" },
        { key: "이익률_개선",      label: "이익률 개선",    fmt: "flag" },
        { key: "이익률_급개선",    label: "이익률 급개선",  fmt: "flag" },
        { key: "이익률_변동폭",    label: "이익률 변동폭",  fmt: "f1" },
        { key: "영업이익_가속도",  label: "OP 가속도",      fmt: "f1" },
        { key: "매출_가속도",      label: "매출 가속도",    fmt: "f1" },
        { key: "실적가속_연속",    label: "실적 가속 연속", fmt: "flag" },
      ]
    },
    {
      title: "효율성 & 마진",
      metrics: [
        { key: "ROIC(%)",          label: "ROIC%",             fmt: "f1" },
        { key: "ROIC_전년(%)",     label: "ROIC 전년%",        fmt: "f1" },
        { key: "ROIC_개선",        label: "ROIC 개선",         fmt: "flag" },
        { key: "영업이익률_최근",  label: "영업이익률% (최근)", fmt: "f1" },
        { key: "영업이익률_전년",  label: "영업이익률% (전년)", fmt: "f1" },
        { key: "GPM_최근(%)",      label: "GPM 최근%",         fmt: "f1" },
        { key: "GPM_전년(%)",      label: "GPM 전년%",         fmt: "f1" },
        { key: "GPM_변화(pp)",     label: "GPM 변화(pp)",      fmt: "f1" },
        { key: "퀄리티_턴어라운드", label: "퀄리티 턴어라운드", fmt: "flag" },
      ]
    },
    {
      title: "Forward 컨센서스 추정치 ⚠️",
      metrics: [
        { key: "컨센서스_커버리지",          label: "커버리지",       fmt: "flag" },
        { key: "Fwd_PER",                    label: "Fwd PER",        fmt: "f2" },
        { key: "Fwd_PBR",                    label: "Fwd PBR",        fmt: "f2" },
        { key: "Fwd_EPS",                    label: "Fwd EPS",        fmt: "int" },
        { key: "Fwd_ROE(%)",                 label: "Fwd ROE%",       fmt: "f1" },
        { key: "Fwd_OPM(%)",                 label: "Fwd OPM%",       fmt: "f1" },
        { key: "Fwd_영업이익_성장률(%)",     label: "Fwd OP성장%",    fmt: "f1" },
        { key: "Fwd_매출_성장률(%)",         label: "Fwd 매출성장%",  fmt: "f1" },
        { key: "Fwd_순이익_성장률(%)",       label: "Fwd NP성장%",    fmt: "f1" },
        { key: "Fwd_2yr_영업이익_성장(%)",   label: "2yr OP성장%",    fmt: "f1" },
        { key: "Fwd_모멘텀_점수",            label: "Fwd 모멘텀점수", fmt: "f1" },
      ]
    },
    {
      title: "데이터 품질",
      metrics: [
        { key: "데이터_연수",      label: "데이터 연수",      fmt: "int" },
        { key: "최근분기",         label: "기준분기",         fmt: "str" },
        { key: "PER_이상",         label: "PER 이상",         fmt: "flag" },
        { key: "순이익_전년음수",  label: "전년 순손실",      fmt: "flag" },
        { key: "순이익_당기양수",  label: "당기 흑자",        fmt: "flag" },
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
        { col: "배당성향(%)",   label: "배당성향%" },
        { col: "ROIC(%)",       label: "ROIC%" },
        { col: "현금전환율(%)", label: "현금전환율%" },
      ]
    },
    {
      key: "market", label: "시가총액 / 점수",
      fields: [
        { col: "시가총액", label: "시가총액(억)", unit: 1e8 },
        { col: "종합점수", label: "종합점수" },
      ]
    },
    {
      key: "forward", label: "Forward 추정치 ⚠️",
      fields: [
        { col: "Fwd_PER",                label: "Fwd PER" },
        { col: "Fwd_PBR",                label: "Fwd PBR" },
        { col: "Fwd_ROE(%)",             label: "Fwd ROE%" },
        { col: "Fwd_OPM(%)",             label: "Fwd OPM%" },
        { col: "Fwd_영업이익_성장률(%)", label: "Fwd OP성장%" },
        { col: "Fwd_매출_성장률(%)",     label: "Fwd 매출성장%" },
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
    if (type === "flag") return Number(v) === 1 ? "O" : "X";
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
    disposeTooltips(); // 로드 시작 시 툴팁 제거
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
    const sector = document.getElementById("f-sector")?.value || "";
    if (market)     params.set("market", market);
    if (sector)     params.set("sector", sector);
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
        const rep = reportMap[code];
        const aiBadge = (rep && c.key === "종목명")
          ? ` <span class="badge badge-ai" title="${rep.model} · ${rep.date}">AI</span>` : "";
        return `<td class="${cls}">${fmt(s[c.key], c.fmt)}${newBadge}${aiBadge}</td>`;
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
      `<tr><th width="20"></th><th width="30">★</th>` +
      cols.map(c => {
        const arrow = sortCol === c.key ? (sortOrder === "desc" ? " ↓" : " ↑") : "";
        const tip = METRIC_TOOLTIPS[c.key]
          ? ` data-bs-toggle="tooltip" data-bs-placement="bottom" title="${METRIC_TOOLTIPS[c.key]}"` : "";
        return `<th data-col="${c.key}" style="cursor:pointer; user-select:none;"${tip}>${c.label}<span class="sort-arrow text-muted small">${arrow}</span></th>`;
      }).join("") + `</tr>`;

    headerRow.querySelectorAll("th[data-col]").forEach(th => {
      th.addEventListener("click", () => {
        const col = th.dataset.col;
        if (sortCol === col) {
          // 3-state: Desc -> Asc -> Reset (Default)
          if (sortOrder === "desc") {
            sortOrder = "asc";
          } else {
            // Reset to default sort for this tab
            sortCol   = TAB_DEFAULT_SORT[currentScreen] || "종합점수";
            sortOrder = "desc";
          }
        } else {
          // New column clicked -> Start with Desc
          sortCol   = col;
          sortOrder = "desc";
        }
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
      const modalEl = document.getElementById("detail-modal");
      const modal = new bootstrap.Modal(modalEl);
      // 모달 애니메이션 완료 후 차트 렌더 (display:none 상태에서 크기 0 방지)
      modalEl.addEventListener("shown.bs.modal", () => {
        loadFinancialChart(code);
      }, { once: true });
      modal.show();
    } catch (e) { console.error("openDetail:", e); }
  }

  function renderDetailModal(stock) {
    const code   = stock["종목코드"] || currentDetailCode;
    const name   = stock["종목명"]   || "Unknown";
    const market = stock["시장구분"] || "";
    const sector = stock["섹터"]     || "";
    const price  = stock["종가"];

    const rep = reportMap[code];
    const aiHeaderBadge = rep
      ? `<span class="badge badge-ai ms-2" title="${rep.model}으로 분석됨 · ${rep.date}">AI 분석 완료</span>` : "";

    document.getElementById("detail-title").innerHTML =
      `<strong>${name}</strong> <span class="text-muted fs-6">${code}</span>
       <span class="badge ${market === "KOSPI" ? "bg-primary" : "bg-danger"} ms-2">${market}</span>
       ${sector ? `<span class="badge bg-secondary ms-1">${sector}</span>` : ""}
       ${price != null ? `<span class="ms-2 fw-bold">${fmt(price, "int")}원</span>` : ""}
       ${aiHeaderBadge}`;

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
        
        const cls = getMetricColor(m.key, v);

        return `<div class="metric-pill" ${tip}>
          <div class="lbl">${m.label}</div>
          <div class="val ${cls}">${fmt(v, m.fmt)}</div>
        </div>`;
      }).filter(Boolean).join("");
      if (!pills) return "";
      return `<div class="w-100 mt-2 mb-1"><small class="fw-bold text-muted text-uppercase">${group.title}</small></div>${pills}`;
    }).join("");

    // 분석 버튼에 code 설정
    const geminiBtn = document.getElementById("btn-analysis-gemini");
    const claudeBtn = document.getElementById("btn-analysis-claude");
    geminiBtn.dataset.code = code;
    claudeBtn.dataset.code = code;

    // 보고서 캐시 상태를 버튼에 반영
    geminiBtn.classList.remove("btn-success", "has-report");
    claudeBtn.classList.remove("has-report");
    geminiBtn.innerHTML = "Gemini 분석 (무료)";
    claudeBtn.innerHTML = "Claude 분석 (프리미엄)";
    if (rep) {
      const isGemini = rep.model.toLowerCase().includes("gemini");
      if (isGemini) {
        geminiBtn.innerHTML = "✓ Gemini 분석 (캐시됨)";
        geminiBtn.classList.add("btn-success");
      } else {
        claudeBtn.innerHTML = "✓ Claude 분석 (캐시됨)";
        claudeBtn.classList.add("has-report");
      }
    }

    // 점수 브레이크다운 렌더링
    renderScoreBreakdown(stock);
    // 재무 차트는 shown.bs.modal 이벤트 후 openDetail()에서 호출됨
    initTooltips();
  }

  // 점수 색상 계산 (0~100 → 빨강~초록)
  function scoreColor(v) {
    if (v == null) return "#ced4da";
    if (v >= 75) return "#198754";
    if (v >= 55) return "#20c997";
    if (v >= 40) return "#ffc107";
    return "#dc3545";
  }

  // 전략 배지 설정
  const STRATEGY_BADGE_INFO = {
    "주도주_점수":    { label: "주도주",  color: "#dc3545" },
    "우량가치_점수":  { label: "우량가치", color: "#0d6efd" },
    "고성장_점수":    { label: "고성장",  color: "#198754" },
    "현금배당_점수":  { label: "현금배당", color: "#fd7e14" },
    "턴어라운드_점수":{ label: "턴어라운드", color: "#6f42c1" },
  };

  let scoreRadarChart = null;

  function renderScoreBreakdown(stock) {
    // 레이더 축 정의: 3개 기본 + 5개 전략
    const RADAR_AXES = [
      { key: "성장성_점수",    label: "성장성" },
      { key: "안정성_점수",    label: "안정성" },
      { key: "가격_점수",      label: "가치" },
      { key: "주도주_점수",    label: "주도주" },
      { key: "우량가치_점수",  label: "우량가치" },
      { key: "고성장_점수",    label: "고성장" },
      { key: "현금배당_점수",  label: "배당" },
      { key: "턴어라운드_점수",label: "턴어라운드" },
    ];

    const values = RADAR_AXES.map(a => {
      const v = stock[a.key];
      return (v != null && !isNaN(v)) ? Math.round(v) : 0;
    });
    const labels = RADAR_AXES.map(a => a.label);

    // 레이더 차트
    if (scoreRadarChart) { scoreRadarChart.destroy(); scoreRadarChart = null; }
    const ctx = document.getElementById("score-radar-chart").getContext("2d");
    scoreRadarChart = new Chart(ctx, {
      type: "radar",
      data: {
        labels,
        datasets: [{
          label: "점수",
          data: values,
          backgroundColor: "rgba(13,110,253,0.15)",
          borderColor: "rgba(13,110,253,0.8)",
          borderWidth: 2,
          pointBackgroundColor: values.map(v => scoreColor(v)),
          pointRadius: 4,
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        scales: {
          r: {
            min: 0, max: 100,
            ticks: { stepSize: 25, font: { size: 9 }, color: "#adb5bd" },
            grid: { color: "rgba(0,0,0,0.06)" },
            pointLabels: { font: { size: 10 }, color: "#495057" },
          }
        },
        plugins: {
          legend: { display: false },
          tooltip: {
            callbacks: {
              label: ctx => ` ${ctx.parsed.r}점`,
            }
          }
        }
      }
    });

    // 바 리스트 (우측 패널)
    const BAR_ITEMS = [
      { key: "종합점수",      label: "종합 점수" },
      { key: "성장성_점수",   label: "성장성" },
      { key: "안정성_점수",   label: "안정성" },
      { key: "가격_점수",     label: "가치" },
      { key: "주도주_점수",   label: "주도주" },
      { key: "우량가치_점수", label: "우량가치" },
      { key: "고성장_점수",   label: "고성장" },
      { key: "현금배당_점수", label: "배당" },
      { key: "턴어라운드_점수", label: "턴어라운드" },
    ];

    const barHtml = BAR_ITEMS.map(item => {
      const v = stock[item.key];
      const pct = (v != null && !isNaN(v)) ? Math.min(100, Math.max(0, v)) : 0;
      const color = scoreColor(v);
      const isBold = item.key === "종합점수";
      return `<div class="score-bd-row${isBold ? " fw-bold" : ""}">
        <div class="score-bd-label">${item.label}</div>
        <div class="score-bd-track">
          <div class="score-bd-fill" style="width:${pct}%; background:${color};"></div>
        </div>
        <div class="score-bd-val" style="color:${color};">${v != null ? Math.round(v) : "-"}</div>
      </div>`;
    }).join("");
    document.getElementById("score-bar-list").innerHTML = barHtml;

    // 전략 배지 (점수 높은 전략만 표시)
    const BADGE_THRESHOLD = 65;
    const badgeHtml = Object.entries(STRATEGY_BADGE_INFO).map(([key, info]) => {
      const v = stock[key];
      if (v == null || v < BADGE_THRESHOLD) return "";
      const opacity = v >= 80 ? "1" : "0.65";
      return `<span class="strategy-badge" style="color:${info.color}; border-color:${info.color}; opacity:${opacity};">${info.label} ${Math.round(v)}</span>`;
    }).join("");
    document.getElementById("strategy-badges").innerHTML = badgeHtml || `<small class="text-muted">해당 전략 없음</small>`;
  }

  let _finPeriod = "annual"; // "annual" | "quarter"

  async function loadFinancialChart(code) {
    const area = document.getElementById("financial-chart-area");
    try {
      const period = _finPeriod;
      const url = `/api/stocks/${code}/financials?period=${period}`;
      const res  = await fetch(url);
      const data = await res.json();
      if (!data.years || data.years.length === 0) { area.style.display = "none"; return; }
      // display를 먼저 설정 후 다음 tick에 chart 생성 (숨겨진 상태에서 렌더 시 크기=0 방지)
      area.style.display = "";
      if (financialChart) { financialChart.destroy(); financialChart = null; }
      await new Promise(r => setTimeout(r, 0));
      const ctx = document.getElementById("financial-chart").getContext("2d");

      // 영업이익 성장률 (YoY) 계산
      const opSeries = data.series.find(s => s.name === "영업이익");
      const opGrowth = opSeries ? opSeries.data.map((v, i) => {
        if (i === 0 || opSeries.data[i-1] == null || opSeries.data[i-1] === 0 || v == null) return null;
        return parseFloat(((v - opSeries.data[i-1]) / Math.abs(opSeries.data[i-1]) * 100).toFixed(1));
      }) : [];

      const barDatasets = data.series.map((s, i) => ({
        type: "bar",
        label: s.name === "매출액" ? "매출" : s.name === "영업이익" ? "영업이익" : "순이익",
        data: s.data.map(v => v != null ? Math.round(v) : null),
        backgroundColor: ["rgba(13,110,253,0.65)", "rgba(25,135,84,0.65)", "rgba(220,53,69,0.65)"][i],
        yAxisID: "y",
        order: 1,
      }));

      const datasets = [...barDatasets];
      const hasYoY = opGrowth.some(v => v !== null);
      if (hasYoY) {
        datasets.push({
          type: "line",
          label: "영업이익 YoY%",
          data: opGrowth,
          borderColor: "#fd7e14",
          backgroundColor: "rgba(253,126,20,0.1)",
          borderWidth: 2,
          pointRadius: 3,
          yAxisID: "y2",
          order: 0,
        });
      }

      const titleText = period === "quarter" ? "분기 실적 추이 (억원)" : "연간 실적 추이 (억원)";
      financialChart = new Chart(ctx, {
        type: "bar",
        data: { labels: data.years, datasets },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          interaction: { mode: "index", intersect: false },
          plugins: {
            title: { display: true, text: titleText, font: { size: 12 } },
            legend: { position: "bottom", labels: { font: { size: 10 }, boxWidth: 12 } },
          },
          scales: {
            y: { beginAtZero: false, ticks: { font: { size: 10 } },
                 title: { display: true, text: "억원", font: { size: 9 } } },
            ...(hasYoY ? { y2: { position: "right", beginAtZero: false,
                  ticks: { font: { size: 10 }, callback: v => v + "%" },
                  grid: { drawOnChartArea: false },
                  title: { display: true, text: "YoY%", font: { size: 9 } } } } : {}),
          }
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
        console.log("[AI분석] POST 요청 시작:", code, mode);
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 360000);
        const loadingText = document.getElementById("report-loading-text");
        const baseMsg = mode === "gemini" ? "Gemini로 분석 중 (Google Search 포함)" : "Claude로 심층 분석 중";
        let elapsed = 0;
        const timerInterval = setInterval(() => {
          elapsed++;
          loadingText.textContent = `${baseMsg}... (${elapsed}초)`;
        }, 1000);
        try {
          const postRes = await fetch(`/api/stocks/${code}/analysis`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ mode }),
            signal: controller.signal,
          });
          data = await postRes.json();
        } finally {
          clearInterval(timerInterval);
          clearTimeout(timeoutId);
        }
      }

      document.getElementById("report-loading").style.display = "none";
      if (data.error) {
        document.getElementById("report-content").innerHTML =
          `<div class="alert alert-danger"><strong>오류:</strong> ${data.error}</div>`;
      } else {
        document.getElementById("report-content").innerHTML = data.report_html || "";
        document.getElementById("report-meta").textContent  =
          `${data.model || ""} · ${data.generated_date || ""}`;
        // reportMap 갱신 → 테이블 행 뱃지 즉시 반영
        const paddedCode = code.toString().padStart(6, "0");
        reportMap[paddedCode] = { model: data.model || "", date: data.generated_date || "" };
        const nameCell = document.querySelector(`tr[data-code="${paddedCode}"] td:nth-child(3)`);
        if (nameCell && !nameCell.querySelector(".badge-ai")) {
          nameCell.insertAdjacentHTML("beforeend",
            ` <span class="badge badge-ai" title="${data.model} · ${data.generated_date}">AI</span>`);
        }
      }
      document.getElementById("btn-regenerate").dataset.code = code;
      document.getElementById("btn-regenerate").dataset.mode = mode;
    } catch (e) {
      document.getElementById("report-loading").style.display = "none";
      const msg = e.name === "AbortError" ? "분석 시간이 초과되었습니다. 다시 시도해주세요." : e.message;
      document.getElementById("report-content").innerHTML =
        `<div class="alert alert-danger">오류: ${msg}</div>`;
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

  // ─── 필터 프리셋 ─────────────────────────────────────────────────────
  const PRESET_KEY = "quant_filter_presets";
  function getPresets() {
    try { return JSON.parse(localStorage.getItem(PRESET_KEY) || "[]"); }
    catch { return []; }
  }
  function savePresets(list) { localStorage.setItem(PRESET_KEY, JSON.stringify(list)); }

  function getCurrentFilterSnapshot() {
    return {
      market:        document.getElementById("f-market")?.value  || "",
      sector:        document.getElementById("f-sector")?.value  || "",
      search:        document.getElementById("f-search")?.value  || "",
      columnFilters: JSON.parse(JSON.stringify(columnFilters)),
    };
  }

  function applyPreset(snap) {
    if (snap.market !== undefined) document.getElementById("f-market").value = snap.market;
    if (snap.sector !== undefined) {
      const sel = document.getElementById("f-sector");
      if (sel) sel.value = snap.sector;
    }
    if (snap.search !== undefined) document.getElementById("f-search").value = snap.search;
    Object.keys(columnFilters).forEach(k => delete columnFilters[k]);
    if (snap.columnFilters) {
      Object.assign(columnFilters, snap.columnFilters);
    }
    if (advFilterOpen) renderFilterPanel(); else updateAdvButton();
    currentPage = 1; loadStocks();
  }

  function renderPresets() {
    const list = document.getElementById("preset-list");
    if (!list) return;
    const presets = getPresets();
    if (!presets.length) { list.innerHTML = `<span class="text-muted small">저장된 프리셋 없음</span>`; return; }
    list.innerHTML = presets.map((p, i) =>
      `<span class="preset-chip" data-idx="${i}">
        ${p.name}
        <span class="preset-del" data-idx="${i}" title="삭제">✕</span>
      </span>`
    ).join("");
    list.querySelectorAll(".preset-chip").forEach(chip => {
      chip.addEventListener("click", e => {
        if (e.target.classList.contains("preset-del")) return;
        const idx = parseInt(chip.dataset.idx, 10);
        applyPreset(getPresets()[idx]);
      });
    });
    list.querySelectorAll(".preset-del").forEach(btn => {
      btn.addEventListener("click", e => {
        e.stopPropagation();
        const idx = parseInt(btn.dataset.idx, 10);
        const presets = getPresets();
        presets.splice(idx, 1);
        savePresets(presets);
        renderPresets();
      });
    });
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
    if (compareSet.size < 2) { alert("2개 이상 종목을 선택하세요 (최대 5개)."); return; }
    if (compareSet.size > 5) { alert("최대 5개까지 비교 가능합니다."); return; }
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
      { key: "배당성향(%)",   label: "배당성향%",    fmt: "f1" },
      { key: "배당_경고신호", label: "배당경고",     fmt: "flag" },
      { key: "DPS_CAGR",     label: "DPS CAGR%",    fmt: "f1" },
      { key: "배당_연속증가", label: "배당연속증가", fmt: "int" },
      { key: "ROIC(%)",       label: "ROIC%",        fmt: "f1" },
      { key: "현금전환율(%)", label: "현금전환%",    fmt: "f1" },
    ],
  };

  let _compareCharts = []; // 비교 모달 내 미니 차트 인스턴스

  function destroyCompareCharts() {
    _compareCharts.forEach(c => { try { c.destroy(); } catch (e) {} });
    _compareCharts = [];
  }

  function renderCompareModal(data) {
    destroyCompareCharts();
    const stocks   = data.stocks      || [];
    const metaMeta = data.metrics_meta || {};
    const cats = [
      { key: "all",           label: "전체" },
      { key: "valuation",     label: "밸류에이션" },
      { key: "profitability", label: "수익성" },
      { key: "growth",        label: "성장성" },
      { key: "stability",     label: "안정성" },
      { key: "technical",     label: "기술적" },
      { key: "dividend",      label: "배당" },
      { key: "financials",    label: "재무추이" },
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
          if (a.dataset.cat === "financials") {
            renderCompareFinancials(stocks);
          } else {
            renderCompareTable(stocks, metaMeta, a.dataset.cat);
          }
        })
      );
    }
    renderCompareTable(stocks, metaMeta, "all");
  }

  async function renderCompareFinancials(stocks) {
    destroyCompareCharts();
    const container = document.getElementById("compare-table-container");
    if (!container) return;
    const COLORS = ["#0d6efd","#198754","#dc3545","#fd7e14","#6f42c1"];
    const rows = stocks.map((s, si) =>
      `<div class="col-md-6 mb-3">
        <div class="card h-100">
          <div class="card-header py-1 px-2 small fw-bold">${s["종목명"]} <span class="text-muted">${s["종목코드"]}</span></div>
          <div class="card-body p-2" style="height:180px;">
            <canvas id="cmp-fin-${si}"></canvas>
          </div>
        </div>
      </div>`
    ).join("");
    container.innerHTML = `<div class="row g-2">${rows}</div>`;

    for (let si = 0; si < stocks.length; si++) {
      const s = stocks[si];
      try {
        const res  = await fetch(`/api/stocks/${s["종목코드"]}/financials`);
        const data = await res.json();
        if (!data.years || !data.years.length) continue;
        const ctx = document.getElementById(`cmp-fin-${si}`)?.getContext("2d");
        if (!ctx) continue;
        const chart = new Chart(ctx, {
          type: "bar",
          data: {
            labels: data.years,
            datasets: data.series.map((sr, i) => ({
              label: sr.name,
              data: sr.data.map(v => v != null ? Math.round(v / 1e8) : null),
              backgroundColor: ["rgba(13,110,253,0.7)","rgba(25,135,84,0.7)","rgba(220,53,69,0.7)"][i],
            }))
          },
          options: {
            responsive: true, maintainAspectRatio: false,
            plugins: { legend: { position: "bottom", labels: { font: { size: 9 }, boxWidth: 10 } } },
            scales: { x: { ticks: { font: { size: 9 } } }, y: { ticks: { font: { size: 9 } }, beginAtZero: false } }
          }
        });
        _compareCharts.push(chart);
      } catch (e) { /* 개별 실패 무시 */ }
    }
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

  function disposeTooltips() {
    document.querySelectorAll('[data-bs-toggle="tooltip"]').forEach(el => {
      if (el._bsTooltip) {
        el._bsTooltip.dispose();
        el._bsTooltip = null;
      } else {
        const instance = bootstrap.Tooltip.getInstance(el);
        if (instance) instance.dispose();
      }
    });
    document.querySelectorAll('.tooltip').forEach(e => e.remove());
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
    loadMarketSummary(); loadBatchChanges(); loadDataInfo(); loadStocks();
  });
  document.getElementById("btn-trigger").addEventListener("click", triggerPipeline);

  // 검색 폼
  document.getElementById("filter-form").addEventListener("submit", e => {
    e.preventDefault(); currentPage = 1; loadStocks();
  });
  document.getElementById("btn-clear").addEventListener("click", () => {
    document.getElementById("f-market").value = "";
    const sectorSel = document.getElementById("f-sector");
    if (sectorSel) sectorSel.value = "";
    document.getElementById("f-search").value = "";
    currentPage = 1; loadStocks();
  });

  document.getElementById("f-pagesize")?.addEventListener("change", () => {
    currentPage = 1; loadStocks();
  });

  // Advanced Filter
  document.getElementById("btn-adv-toggle")?.addEventListener("click", () => {
    toggleAdvFilter();
    if (advFilterOpen) renderPresets();
  });
  document.getElementById("btn-adv-reset")?.addEventListener("click", resetAdvFilter);
  document.getElementById("adv-search")?.addEventListener("input", () => renderFilterPanel());

  // 프리셋 저장 버튼
  document.getElementById("btn-preset-save")?.addEventListener("click", () => {
    const snap = getCurrentFilterSnapshot();
    const hasFilter = snap.market || snap.sector || snap.search || Object.keys(snap.columnFilters).length;
    if (!hasFilter) { alert("저장할 필터 조건이 없습니다."); return; }
    const name = prompt("프리셋 이름을 입력하세요:", "내 프리셋");
    if (!name) return;
    const presets = getPresets();
    presets.push({ name: name.trim(), ...snap });
    savePresets(presets);
    renderPresets();
  });

  // 비교 기능
  document.getElementById("btn-compare")?.addEventListener("click", openCompareModal);
  document.getElementById("btn-compare-clear")?.addEventListener("click", () => {
    compareSet.clear();
    updateCompareBar();
    document.querySelectorAll(".compare-cb").forEach(cb => { cb.checked = false; });
  });

  // CSV 내보내기
  document.getElementById("btn-export-csv")?.addEventListener("click", exportTableCSV);

  // 관심종목 버튼 (세부 모달)
  document.getElementById("btn-watch-detail").addEventListener("click", function () {
    if (this.dataset.code) toggleWatch(this.dataset.code);
  });

  // 재무 차트 연간/분기 토글
  document.getElementById("fin-period-toggle")?.querySelectorAll("button").forEach(btn => {
    btn.addEventListener("click", () => {
      document.querySelectorAll("#fin-period-toggle button").forEach(b => b.classList.remove("active"));
      btn.classList.add("active");
      _finPeriod = btn.dataset.period;
      if (currentDetailCode) loadFinancialChart(currentDetailCode);
    });
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

  // 분석 가이드 버튼
  document.getElementById("btn-strategy-guide").addEventListener("click", () => {
    const guideHtml = STRATEGY_GUIDES[currentScreen];
    if (guideHtml) {
      document.getElementById("guide-content").innerHTML = guideHtml;
      document.getElementById("guide-title").textContent = `💡 ${STRATEGY_DESCRIPTIONS[currentScreen].title} 분석 가이드`;
      new bootstrap.Modal(document.getElementById("guide-modal")).show();
    }
  });

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
      // CSV 버튼은 모든 탭에서 표시
      const csvBtn = document.getElementById("btn-export-csv");
      if (csvBtn) csvBtn.style.display = "";
      renderChangeBanner();
      
      const desc = STRATEGY_DESCRIPTIONS[currentScreen];
      if (desc) {
        document.getElementById("strategy-desc").innerHTML =
          `<strong>${desc.title}</strong>&nbsp;&nbsp;<small class="text-muted">${desc.criteria}</small>`;
      }
      
      // 가이드 버튼 표시 제어
      const btnGuide = document.getElementById("btn-strategy-guide");
      if (STRATEGY_GUIDES[currentScreen]) {
        btnGuide.style.display = "";
      } else {
        btnGuide.style.display = "none";
      }
    })
  );

  // 섹터 드롭다운 초기화
  async function loadSectorOptions() {
    const sel = document.getElementById("f-sector");
    if (!sel) return;
    try {
      const res  = await fetch("/api/sectors");
      const data = await res.json();
      // 기존 옵션 (전체 섹터) 유지 후 추가
      const current = sel.value;
      while (sel.options.length > 1) sel.remove(1);
      data.forEach(({ 섹터: name, count }) => {
        const opt = document.createElement("option");
        opt.value = name;
        opt.textContent = `${name} (${count})`;
        sel.appendChild(opt);
      });
      sel.value = current;
    } catch (e) { /* 섹터 API 실패 시 무시 */ }
  }
  const sectorEl = document.getElementById("f-sector");
  if (sectorEl) sectorEl.addEventListener("change", () => { currentPage = 1; loadStocks(); });

  // ─── 데이터 품질 정보 ─────────────────────────────────────────────────
  async function loadDataInfo() {
    try {
      const res  = await fetch("/api/info");
      const info = await res.json();
      const el   = document.getElementById("data-quality-badge");
      if (!el) return;
      const days = info.days_old;
      const mtime = info.db_mtime || "";
      const quarter = info.latest_quarter ? ` · ${info.latest_quarter}` : "";
      const count = info.stock_count ? ` · ${info.stock_count.toLocaleString()}종목` : "";
      if (days === null) { el.style.display = "none"; return; }
      el.style.display = "";
      if (days >= 30) {
        el.innerHTML = `<span class="badge bg-danger" title="마지막 수집: ${mtime}">⚠ ${days}일 전 수집${count}${quarter}</span>`;
      } else if (days >= 7) {
        el.innerHTML = `<span class="badge bg-warning text-dark" title="마지막 수집: ${mtime}">${days}일 전 수집${count}${quarter}</span>`;
      } else {
        el.innerHTML = `<span class="badge bg-success" title="마지막 수집: ${mtime}">최신 데이터${count}${quarter}</span>`;
      }
    } catch (e) { /* 무시 */ }
  }

  // ─── 보고서 맵 ────────────────────────────────────────────────────────
  async function loadReportMap() {
    try {
      const res = await fetch("/api/reports");
      if (res.ok) reportMap = await res.json();
    } catch (e) { /* 무시 */ }
  }

  // ─── 초기화 ──────────────────────────────────────────────────────────
  buildHeader();
  loadMarketSummary();
  loadBatchChanges();
  loadTabCounts();
  loadSectorOptions();
  loadDataInfo();
  loadReportMap();
  loadStocks();
  updateWatchlistCount();

  const initDesc = STRATEGY_DESCRIPTIONS.all;
  document.getElementById("strategy-desc").innerHTML =
    `<strong>${initDesc.title}</strong>&nbsp;&nbsp;<small class="text-muted">${initDesc.criteria}</small>`;

})();
