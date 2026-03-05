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
    portfolio:        "비중",
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
    portfolio:       { title: "📋 포트폴리오 (보유종목)",   criteria: "매수 수량/단가 기록, 수익률/비중 분석" },
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
      <h6>📐 종합점수 구성</h6>
      <p>종합점수 = (성장성 + 안정성 + 가격) ÷ 3 — 각 세부 점수는 전체 종목 대비 백분위 순위로 산출됩니다.</p>
      <ul>
        <li><strong>성장성:</strong> 영업이익_CAGR×35% + 매출_CAGR×30% + 분기영업이익YoY×25% + 실적가속연속×10%</li>
        <li><strong>안정성:</strong> ROE×40% + F스코어×35% + FCF수익률×25%</li>
        <li><strong>가격:</strong> PER역순×40% + 괴리율(S-RIM)×35% + PBR역순×25%</li>
      </ul>
    `,
    leaders: `
      <h6>🔥 시장 주도주 (Leaders) 공략법</h6>
      <p>시장의 관심(수급)과 실적 성장이 <strong>동시에</strong> 받쳐주는 주도주를 찾습니다.</p>

      <h6>✅ 진입 조건 (모두 충족 필요)</h6>
      <ul>
        <li>시가총액 ≥ 1,000억 · TTM 순이익 흑자</li>
        <li><strong>RS_등급 ≥ 80</strong> (전체 종목 대비 상위 20% 상대강도)</li>
        <li>수급강도 > 0 (외인+기관 순매수 우위)</li>
        <li>20일 평균 거래대금 > 5억 원</li>
      </ul>

      <h6>📐 주도주 점수 구성</h6>
      <ul>
        <li><span class="badge bg-primary text-white">RS_등급</span> × <strong>25%</strong> — 핵심. 60일(40%)·120일(30%)·250일(30%) 복합 상대강도 백분위</li>
        <li><span class="badge bg-light text-dark border">수급강도</span> × 20% — 외인+기관 순매수액 / 시총 (20일 합산)</li>
        <li><span class="badge bg-light text-dark border">영업이익_CAGR</span> × 15% — 연간 이익 성장 지속성</li>
        <li><span class="badge bg-light text-dark border">분기영업이익YoY</span> × 15% — 최근 분기 실적 모멘텀</li>
        <li><span class="badge bg-light text-dark border">실적가속_연속</span> × 10% — 2분기 연속 ΔYoY > 0 (가속 가중치)</li>
        <li><span class="badge bg-light text-dark border">거래대금</span> × 10% · <span class="badge bg-light text-dark border">RSI</span> × 5%</li>
      </ul>

      <h6>🎯 매매 포인트</h6>
      <ul>
        <li><strong>수급강도 양수(+) 지속:</strong> 외국인/기관이 꾸준히 순매수 중이라는 신호입니다.</li>
        <li><strong>양매수_비율 확인:</strong> 외인과 기관이 <em>동시에</em> 순매수한 날의 비율 — 높을수록 수급 확신도가 강합니다.</li>
        <li><strong>RSI 50~70 구간:</strong> 상승 추세 중 과열이 아닌 눌림목 구간을 노리세요. 70 초과는 단기 과열.</li>
        <li><strong>MA20 이격도 체크:</strong> 105% 이하 종목이 상대적으로 안전합니다.</li>
        <li><strong>실적가속_연속 = 1:</strong> 분기 이익 성장이 가속도를 내고 있는 구간 — 주가 상승 탄력의 핵심 신호.</li>
      </ul>
    `,
    quality_value: `
      <h6>💎 우량가치주 (Quality & Value) 발굴</h6>
      <p>싸면서도 돈을 잘 벌고 재무가 튼튼한 '육각형 미인' 종목을 찾습니다.</p>

      <h6>✅ 진입 조건 — 듀얼 트랙</h6>
      <p>기업 성격에 따라 <strong>자동으로 두 가지 기준</strong> 중 하나가 적용됩니다.</p>
      <div class="row g-2 mb-2">
        <div class="col-6">
          <div class="border rounded p-2 h-100">
            <strong>일반기업 트랙</strong>
            <ul class="mb-0 mt-1 small">
              <li>ROIC ≥ 10%</li>
              <li>PEG &lt; 1.2</li>
              <li>F스코어 ≥ 5</li>
              <li>부채비율 &lt; 120%</li>
              <li>유동비율 > 120%</li>
              <li>순이익 연속 흑자</li>
              <li>시총 ≥ 1,000억</li>
            </ul>
          </div>
        </div>
        <div class="col-6">
          <div class="border rounded p-2 h-100">
            <strong>금융/지주 트랙</strong>
            <small class="text-muted d-block">(금융·은행·증권·보험 등)</small>
            <ul class="mb-0 mt-1 small">
              <li>ROE ≥ 8%</li>
              <li>PBR &lt; 1.5</li>
              <li>배당수익률 ≥ 2%</li>
              <li>F스코어 ≥ 4</li>
              <li>시총 ≥ 3,000억</li>
            </ul>
          </div>
        </div>
      </div>

      <h6>📐 우량가치 점수 구성</h6>
      <ul>
        <li><span class="badge bg-primary text-white">FCF수익률</span> × <strong>25%</strong> — 시총 대비 잉여현금 창출력</li>
        <li><span class="badge bg-primary text-white">ROIC</span> × <strong>25%</strong> — NOPAT / 투하자본 (세율 22% 적용)</li>
        <li><span class="badge bg-light text-dark border">F스코어</span> × 20% — Piotroski 9개 항목 재무 건전성</li>
        <li><span class="badge bg-light text-dark border">괴리율(S-RIM)</span> × 20% — (적정주가 − 현재가) / 현재가</li>
        <li><span class="badge bg-light text-dark border">PEG 역순</span> × 10% — PER ÷ min(순이익CAGR, 100%)</li>
      </ul>

      <h6>🔑 핵심 지표 해석</h6>
      <ul>
        <li><strong>ROIC 계산:</strong> NOPAT(영업이익×78%) ÷ 투하자본(총자산 − 유동부채 − 잉여현금). 자본비용(통상 9%)을 넘어야 진짜 가치 창출.</li>
        <li><strong>PEG 0.5 ~ 1.0:</strong> 이익 성장률 대비 저평가 구간. 0.5 이하면 강력 매수 후보. PEG = PER ÷ 순이익CAGR(최대 100% 캡).</li>
        <li><strong>S-RIM 적정주가:</strong> BPS 기반 초과이익 현가 모델 (자본비용 Ke=9%, 지속계수 0.9). 괴리율이 높을수록 저평가.</li>
        <li><strong>F-Score 7점↑:</strong> 수익성·레버리지·유동성·효율성 9항목 합산. 8~9점은 최상위 재무 건전성.</li>
      </ul>

      <h6>⚠️ Value Trap 피하기</h6>
      <ul>
        <li>PER/PBR이 낮아도 <strong>ROE가 10% 미만이면 함정</strong>일 수 있습니다. 수익 창출력이 먼저입니다.</li>
        <li>ROIC가 WACC(자본비용)보다 낮은 기업은 성장할수록 가치가 줄어듭니다.</li>
      </ul>
    `,
    growth_mom: `
      <h6>🚀 고성장 모멘텀주 (Growth) 투자</h6>
      <p>매출과 이익이 폭발적으로 성장하며 주가 추세가 살아있는 종목입니다.</p>

      <h6>✅ 진입 조건 (모두 충족 필요)</h6>
      <ul>
        <li>매출_CAGR ≥ 10% & 영업이익_CAGR ≥ 10% (연간 복합 성장)</li>
        <li>분기 영업이익 YoY > 0 (최근 분기도 성장 중)</li>
        <li><strong>RS_등급 ≥ 50</strong> (시장 대비 상대강도 중위권 이상)</li>
        <li>TTM 영업현금흐름 > 0 (흑자도산 방지)</li>
        <li>시총 ≥ 500억</li>
      </ul>

      <h6>📐 고성장 점수 구성</h6>
      <ul>
        <li><span class="badge bg-primary text-white">RS_등급</span> × <strong>25%</strong> — 주가 추세가 점수 최대 비중. 성장주는 주가 선행이 핵심.</li>
        <li><span class="badge bg-primary text-white">분기영업이익YoY</span> × 20% — 최근 실적 모멘텀</li>
        <li><span class="badge bg-primary text-white">실적가속_연속</span> × 20% — 2분기 연속 YoY 가속도 양수</li>
        <li><span class="badge bg-light text-dark border">PEG 역순</span> × 20% — 성장률 대비 밸류에이션</li>
        <li><span class="badge bg-light text-dark border">영업이익_CAGR</span> × 15% — 연간 성장 지속성</li>
      </ul>

      <h6>🎯 핵심 지표 해석</h6>
      <ul>
        <li><strong>실적가속_연속 = 1:</strong> 분기 영업이익 YoY가 2분기 연속으로 가속 중 (ΔYoY > 0 두 번 연속). 성장 모멘텀이 강화되는 가장 강력한 신호.</li>
        <li><strong>OP가속도(영업이익_가속도):</strong> 이번 분기 YoY% − 전 분기 YoY%. 양수면 성장 가속, 음수면 감속. 가속 초기 진입이 최적.</li>
        <li><strong>정배열 초입:</strong> MA60 이격도가 100~103% 구간에서 위를 향하면 추세 상승 초기 시그널입니다.</li>
        <li><strong>실적 가속화 확인:</strong> 연간 CAGR보다 최근 분기 YoY가 더 높으면 성장이 가속 중 — 최우선 선별 조건.</li>
      </ul>

      <h6>⚠️ 주의사항</h6>
      <ul>
        <li><strong>부채비율 200% 이하 권장:</strong> 성장을 위해 과도한 레버리지를 쓰면 금리 상승 시 취약합니다.</li>
        <li><strong>TTM 영업CF > 0 필수:</strong> 이익은 나도 현금이 없는 기업은 흑자도산 위험. 현금 창출력 반드시 확인.</li>
      </ul>
    `,
    cash_div: `
      <h6>💰 현금배당주 (Cash & Dividend) 선별</h6>
      <p>배당만 많이 주는 게 아니라, <strong>실제 현금 창출력</strong>과 <strong>배당 지속 가능성</strong>이 검증된 기업입니다.</p>

      <h6>✅ 진입 조건 (모두 충족 필요)</h6>
      <ul>
        <li>FCF수익률 ≥ 3% (시총 대비 잉여현금흐름 비율)</li>
        <li>배당수익률 ≥ 1%</li>
        <li>배당성향 &lt; 80% (EPS 대비 DPS 비율)</li>
        <li>현금전환율 ≥ 70% (순이익 → 실제 현금 전환율)</li>
        <li>부채비율 &lt; 120%</li>
        <li>시총 ≥ 500억</li>
      </ul>

      <h6>📐 현금배당 점수 구성</h6>
      <ul>
        <li><span class="badge bg-primary text-white">FCF수익률</span> × <strong>25%</strong></li>
        <li><span class="badge bg-light text-dark border">배당수익률</span> × 20%</li>
        <li><span class="badge bg-light text-dark border">DPS_CAGR</span> × 15% — 배당금 성장률 (복리 배당의 핵심)</li>
        <li><span class="badge bg-light text-dark border">ROIC</span> × 15%</li>
        <li><span class="badge bg-light text-dark border">배당성향 역순</span> × 10% · <span class="badge bg-light text-dark border">F스코어</span> × 10% · <span class="badge bg-light text-dark border">부채비율 역순</span> × 5%</li>
        <li class="text-success"><strong>보너스:</strong> 배당_연속증가 연수 → log₂ 곡선 가산 (최대 +10pt) + 수익동반증가 시 추가 +2pt</li>
        <li class="text-danger"><strong>패널티:</strong> 배당_경고신호 = 1이면 최종 점수 ×0.7 (30% 감점)</li>
      </ul>

      <h6>🔑 진짜 배당주 구별법</h6>
      <ul>
        <li><strong>배당성향 &lt; 50%:</strong> EPS의 절반 이하를 배당하면 이익 재투자와 배당 삭감 위험 모두 방어됩니다. 80% 초과는 <span class="badge bg-warning text-dark">경고</span> 신호.</li>
        <li><strong>ROIC ≥ 10%:</strong> 자본 대비 수익률이 높아야 5~10년 뒤에도 배당이 유지됩니다. 기업의 경제적 해자(Moat) 확인 지표.</li>
        <li><strong>현금전환율 ≥ 70%:</strong> 계산식 = 영업CF / 순이익. 순이익의 70% 이상이 실제 현금으로 들어와야 진짜 배당 재원입니다.</li>
        <li><strong>DPS_CAGR 확인:</strong> 배당금 자체가 매년 성장하는지 확인하세요. 점수의 15%를 차지하는 중요 지표.</li>
        <li><strong>배당_경고신호 = 0 필수:</strong> 아래 3가지 중 하나라도 해당하면 경고 플래그 켜짐:
          <ul>
            <li>배당성향 > 80%</li>
            <li>배당수익률 > 10% + RS_등급 &lt; 30 (하위 30%, 주가 붕괴 중 고배당)</li>
            <li>현금전환율 &lt; 70% (이익이 현금으로 전환 안 됨)</li>
          </ul>
        </li>
        <li><strong>동반성장(✓) = 복리 배당주:</strong> 순이익 2년↑ + DPS 1년↑ 동시 충족. 이익 성장과 배당 성장이 함께 가는 진정한 복리 배당주.</li>
      </ul>
    `,
    turnaround: `
      <h6>🔄 턴어라운드 (Turnaround) 포착</h6>
      <p>최악의 상황을 지나 실적이 급격히 개선되는 종목을 바닥권에서 잡습니다.</p>

      <h6>✅ 진입 조건</h6>
      <ul>
        <li><strong>흑자전환 OR 이익률 급개선(OPM +5%p↑)</strong> 중 하나 이상</li>
        <li>TTM 순이익 > 0 & TTM 영업CF > 0 (실제 현금 창출 검증)</li>
        <li>분기 매출 YoY > −15% (매출 붕괴 기업 제외)</li>
        <li>이자보상배율 > 1.5 (영업이익이 이자비용의 1.5배 이상)</li>
        <li>시총 ≥ 300억</li>
      </ul>

      <h6>📐 턴어라운드 점수 구성</h6>
      <ul>
        <li><span class="badge bg-primary text-white">흑자전환</span> × <strong>15%</strong></li>
        <li><span class="badge bg-primary text-white">퀄리티_턴어라운드</span> × <strong>15%</strong> — 본업 체질 개선 3종 세트</li>
        <li><span class="badge bg-primary text-white">스마트머니_승률</span> × <strong>15%</strong> — 외인/기관 수급 선행 신호</li>
        <li><span class="badge bg-light text-dark border">분기매출YoY</span> × 15% · <span class="badge bg-light text-dark border">이익률_변동폭</span> × 10%</li>
        <li><span class="badge bg-light text-dark border">GPM_변화</span> × 10% · <span class="badge bg-light text-dark border">이자보상배율</span> × 10% · <span class="badge bg-light text-dark border">괴리율</span> × 10%</li>
      </ul>

      <h6>🔑 핵심 지표 해석</h6>
      <ul>
        <li><strong>퀄리티_턴어라운드 (점수 2위 비중):</strong> 다음 3가지를 <em>동시에</em> 충족해야 켜집니다:
          <ul>
            <li>매출총이익률(GPM) +2%p 이상 개선 — 본업 원가 경쟁력 회복</li>
            <li>영업현금흐름 > 0 — 실제 현금 창출</li>
            <li>ROIC 전년 대비 개선 — 자본 효율성 회복</li>
          </ul>
        </li>
        <li><strong>이익률_급개선 vs 퀄리티_턴어라운드 구분:</strong>
          <ul>
            <li>이익률_급개선: OPM(영업이익률) +5%p↑ → 진입 조건 (큰 폭 실적 개선)</li>
            <li>퀄리티_턴어라운드: GPM +2%p↑ + CF + ROIC → 점수 가산 (체질 개선 확인)</li>
          </ul>
        </li>
        <li><strong>스마트머니_승률:</strong> 최근 20거래일 중 외인 또는 기관이 순매수한 날의 비율. 0.5 이상이면 수급 유입 신호.</li>
        <li><strong>VCP_신호 (변동성 수축 패턴):</strong> 가격 CV 축소 + 거래량 축소 + 스마트머니_승률 ≥ 60% 동시 충족 시 켜짐 — 주가 에너지 응축 구간.</li>
        <li><strong>높은 괴리율:</strong> 실적은 좋아졌는데 주가가 아직 반응하지 않아 S-RIM 적정주가 대비 현저히 저평가된 종목을 찾으세요.</li>
      </ul>

      <h6>⚠️ 주의사항</h6>
      <ul>
        <li><strong>본업 개선 필수 확인:</strong> 일회성 자산 매각으로 인한 흑자전환은 오래가지 않습니다. 영업이익 흑자전환과 퀄리티_턴어라운드 플래그를 함께 확인하세요.</li>
        <li><strong>이자보상배율 1.5 이상:</strong> 이 미만이면 이자도 못 갚는 기업 — 진입 조건으로 걸러지지만, 1.5~2.0 구간은 여전히 주의가 필요합니다.</li>
      </ul>
    `,
    multi_strategy: `
      <h6>🏆 Multi-Pick (다관왕) 활용</h6>
      <p>5가지 전략 중 <strong>3개 이상의 기준을 동시에 만족</strong>하는 '슈퍼 종목'입니다.</p>

      <h6>📐 종합점수 세부 구성</h6>
      <p>Multi-Pick은 전략별 점수 대신 <strong>종합점수</strong>로 정렬됩니다.</p>
      <ul>
        <li><strong>성장성 (33.3%):</strong> 영업이익CAGR×35% + 매출CAGR×30% + 분기영업이익YoY×25% + 실적가속연속×10%</li>
        <li><strong>안정성 (33.3%):</strong> ROE×40% + F스코어×35% + FCF수익률×25%</li>
        <li><strong>가격 (33.3%):</strong> PER역순×40% + 괴리율(S-RIM)×35% + PBR역순×25%</li>
      </ul>

      <h6>🎯 활용법</h6>
      <ul>
        <li><strong>전략수 컬럼:</strong> 3~5까지 가능합니다. 5관왕부터 내림차순 정렬하여 최우선 후보를 확인하세요.</li>
        <li><strong>전략 조합별 해석:</strong>
          <ul>
            <li>🔥주도주 + 🚀고성장 + 💎우량가치 = <strong>성장 + 추세 + 저평가</strong> → 주가 상승 탄력 극대화</li>
            <li>💎우량가치 + 💰현금배당 + 🔄턴어라운드 = <strong>방어적 저가매수</strong> → 하방 리스크 낮음</li>
            <li>🔥주도주 + 🚀고성장 + 💰현금배당 = <strong>성장하며 배당도 주는 우량주</strong> → 장기 보유 적합</li>
          </ul>
        </li>
        <li>종합점수 최상위 + 전략수 4~5개 종목은 포트폴리오의 핵심 비중으로 고려해볼 만합니다.</li>
      </ul>
    `,
    forward_covered: `
      <h6>🔭 Forward 컨센서스 추정치 활용법</h6>
      <p>애널리스트 컨센서스 추정치가 있는 <strong>~535개 커버리지 종목</strong>에 한해 내년도 실적 전망 기준으로 순위를 매깁니다.</p>

      <h6>⚠️ 커버리지 편향 주의</h6>
      <p>이 탭은 대형·중형주 위주의 애널리스트 커버 종목만 표시됩니다. 소형주·성장 초기 기업은 포함되지 않으며, 기존 5개 전략 탭과 직접 비교하지 마세요.</p>

      <h6>📐 Fwd_모멘텀_점수 구성 (10개 요소)</h6>
      <p>성장 전망 60% + 재무 안정성 25% + 주주환원 15%의 종합 평가입니다.</p>
      <ul>
        <li><strong>[성장 전망]</strong>
          <ul>
            <li><span class="badge bg-primary text-white">Fwd_OP성장률</span> × <strong>25%</strong> — 내년 영업이익 성장 모멘텀</li>
            <li><span class="badge bg-light text-dark border">Fwd_ROE%</span> × 15% — 내년 자본수익률</li>
            <li><span class="badge bg-light text-dark border">Fwd_PER (역순)</span> × 10% — 내년 이익 대비 저평가</li>
            <li><span class="badge bg-light text-dark border">Fwd_OPM%</span> × 5% — 내년 영업이익률</li>
            <li><span class="badge bg-light text-dark border">Fwd_2yr_OP성장</span> × 5% — 2년 성장 지속성</li>
          </ul>
        </li>
        <li><strong>[재무 안정성]</strong>
          <ul>
            <li><span class="badge bg-light text-dark border">이자보상배율</span> × 10% · <span class="badge bg-light text-dark border">부채비율 역순</span> × 10% · <span class="badge bg-light text-dark border">F스코어</span> × 5%</li>
          </ul>
        </li>
        <li><strong>[주주환원]</strong>
          <ul>
            <li><span class="badge bg-light text-dark border">배당수익률</span> × 10% · <span class="badge bg-light text-dark border">DPS_CAGR</span> × 5%</li>
          </ul>
        </li>
      </ul>

      <h6>🔑 활용법</h6>
      <ul>
        <li><strong>추정치 신뢰도:</strong> 애널리스트 수가 많을수록, 최근 발표일에 가까울수록 신뢰도가 높습니다. 단일 애널리스트 추정치는 변동성이 클 수 있습니다.</li>
        <li><strong>교차 검증 활용:</strong> 다른 탭에서 발굴한 종목의 Fwd_PER을 확인하여, 실적 개선 기대감이 이미 주가에 과도하게 반영되었는지 점검하세요.</li>
        <li><strong>Fwd_2yr_OP성장 활용:</strong> 내년뿐 아니라 내후년까지 성장이 이어지는 종목이 더 신뢰도 있는 성장 스토리를 가집니다.</li>
      </ul>
    `,
    watchlist: `
      <h6>⭐ 관심종목 관리</h6>
      <p>직접 선별한 종목들의 현황을 한눈에 모니터링합니다.</p>
      <ul>
        <li>다른 탭에서 <span class="text-warning">☆</span> 버튼을 눌러 추가한 종목들이 여기에 표시됩니다.</li>
        <li>정기적으로 리스트를 점검하여 투자 매력이 떨어진 종목은 제외하고, 새로운 유망 종목으로 교체하세요.</li>
        <li>'비교하기' 기능으로 관심 종목 간 지표 우열을 가려보세요. <strong>'재무추이' 탭</strong>에서 종목별 연간 실적 추이를 나란히 비교할 수 있습니다.</li>
        <li>관심종목 행의 <span class="text-info">+PF</span> 버튼으로 포트폴리오에 편입할 수 있습니다.</li>
      </ul>
    `,
    portfolio: `
      <h6>📋 포트폴리오 관리</h6>
      <p>보유 종목의 매수 내역을 기록하고 실시간 수익률과 비중을 관리합니다.</p>
      <ul>
        <li><strong>종목 추가:</strong> 상단 '+ 종목 추가' 버튼이나, 관심종목/상세 모달의 '포트폴리오 추가' 버튼을 사용하세요.</li>
        <li><strong>수정/삭제:</strong> 각 행의 ✎(수정) / ✕(삭제) 버튼으로 매수 내역을 관리합니다.</li>
        <li><strong>요약:</strong> 상단 요약 바에서 총평가, 총손익, 섹터 비중을 확인하세요.</li>
        <li><strong>집중도 관리 팁:</strong> 섹터 비중이 특정 섹터에 30% 이상 몰려 있다면 분산 투자를 고려하세요. 요약 바의 섹터 비중으로 편중도를 확인할 수 있습니다.</li>
      </ul>
    `
  };

  // ─── 탭별 기본 컬럼 정의 ─────────────────────────────────────────────
  const COLUMNS = {
    // 1. 전체 종목 - 균형잡힌 종합 정보 (26개)
    all: [
      // 기본
      { key: "종목코드", label: "코드" }, { key: "종목명", label: "종목명" }, { key: "섹터", label: "섹터" }, { key: "시장구분", label: "시장" },
      { key: "종가", label: "현재가", fmt: "int" }, { key: "시가총액", label: "시총", fmt: "eok" },
      // 가치평가
      { key: "PER", label: "PER", fmt: "f2" }, { key: "PBR", label: "PBR", fmt: "f2" },
      { key: "PEG", label: "PEG", fmt: "f2" }, { key: "PSR", label: "PSR", fmt: "f2" },
      { key: "적정주가_SRIM", label: "적정가", fmt: "int" }, { key: "괴리율(%)", label: "괴리율%", fmt: "f1" },
      // 수익성
      { key: "ROE(%)", label: "ROE%", fmt: "f2" }, { key: "ROIC(%)", label: "ROIC%", fmt: "f1" },
      { key: "영업이익률(%)", label: "OPM%", fmt: "f1" }, { key: "FCF수익률(%)", label: "FCF%", fmt: "f2" },
      // 성장성
      { key: "영업이익_CAGR", label: "OP CAGR", fmt: "f1" }, { key: "Q_영업이익_YoY(%)", label: "Q OP YoY", fmt: "f1" },
      // 안정성
      { key: "부채비율(%)", label: "부채%", fmt: "f1" }, { key: "F스코어", label: "F-Score", fmt: "int" },
      // 수급/기술
      { key: "수급강도", label: "수급", fmt: "f1" }, { key: "RS_등급", label: "RS등급", fmt: "f1" },
      { key: "52주_최고대비(%)", label: "고가대비%", fmt: "f1" },
      { key: "거래대금_20일평균", label: "거래(평)", fmt: "eok" },
      // 배당
      { key: "배당수익률(%)", label: "배당%", fmt: "f2" },
      // 종합
      { key: "종합점수", label: "점수", fmt: "f1" }
    ],
    // 2. 시장 주도주 - 수급+모멘텀+실적 (18개)
    leaders: [
      // 기본
      { key: "종목코드", label: "코드" }, { key: "종목명", label: "종목명" }, { key: "섹터", label: "섹터" },
      { key: "종가", label: "현재가", fmt: "int" }, { key: "시가총액", label: "시총", fmt: "eok" }, { key: "시장구분", label: "시장" },
      // 수급/모멘텀
      { key: "수급강도", label: "수급", fmt: "f1" }, { key: "외인순매수_20d", label: "외인순매수", fmt: "eok" },
      { key: "기관순매수_20d", label: "기관순매수", fmt: "eok" }, { key: "스마트머니_승률", label: "SM승률", fmt: "f1" },
      { key: "양매수_비율", label: "양매수%", fmt: "f1" },
      // RS/기술
      { key: "RS_등급", label: "RS등급", fmt: "f1" }, { key: "Composite_RS", label: "RS복합", fmt: "f1" },
      { key: "52주_최고대비(%)", label: "고가대비%", fmt: "f1" }, { key: "VCP_신호", label: "VCP", fmt: "flag" },
      // 실적
      { key: "거래대금_증감(%)", label: "거래증감%", fmt: "f1" },
      { key: "Q_영업이익_YoY(%)", label: "Q OP YoY", fmt: "f1" }, { key: "실적가속_연속", label: "실적가속" },
      // 점수
      { key: "주도주_점수", label: "주도점수", fmt: "f1" }
    ],
    // 3. 우량가치 - ROE, F-Score, PEG, ROIC (20개)
    quality_value: [
      // 기본
      { key: "종목코드", label: "코드" }, { key: "종목명", label: "종목명" }, { key: "섹터", label: "섹터" },
      { key: "종가", label: "현재가", fmt: "int" }, { key: "시가총액", label: "시총", fmt: "eok" },
      // 밸류에이션
      { key: "PER", label: "PER", fmt: "f2" }, { key: "PBR", label: "PBR", fmt: "f2" },
      { key: "PEG", label: "PEG", fmt: "f2" }, { key: "PSR", label: "PSR", fmt: "f2" },
      { key: "적정주가_SRIM", label: "적정가", fmt: "int" }, { key: "괴리율(%)", label: "괴리율%", fmt: "f1" },
      // 수익성/효율
      { key: "ROE(%)", label: "ROE%", fmt: "f2" }, { key: "ROIC(%)", label: "ROIC%", fmt: "f1" },
      { key: "영업이익률(%)", label: "OPM%", fmt: "f1" }, { key: "FCF수익률(%)", label: "FCF%", fmt: "f2" },
      // 안정성
      { key: "F스코어", label: "F-Score", fmt: "int" }, { key: "부채비율(%)", label: "부채%", fmt: "f1" },
      { key: "이익품질_양호", label: "이익품질", fmt: "flag" },
      // 성장
      { key: "영업이익_CAGR", label: "OP CAGR", fmt: "f1" },
      // 점수
      { key: "우량가치_점수", label: "우량점수", fmt: "f1" }
    ],
    // 4. 고성장 모멘텀 - CAGR, YoY, 추세, 가속도 (21개)
    growth_mom: [
      // 기본
      { key: "종목코드", label: "코드" }, { key: "종목명", label: "종목명" }, { key: "섹터", label: "섹터" },
      { key: "종가", label: "현재가", fmt: "int" }, { key: "시가총액", label: "시총", fmt: "eok" },
      // 성장 CAGR
      { key: "매출_CAGR", label: "매출CAGR", fmt: "f1" }, { key: "영업이익_CAGR", label: "OP CAGR", fmt: "f1" },
      { key: "순이익_CAGR", label: "NP CAGR", fmt: "f1" }, { key: "FCF_CAGR", label: "FCF CAGR", fmt: "f1" },
      // 분기 YoY
      { key: "Q_매출_YoY(%)", label: "Q 매출YoY", fmt: "f1" }, { key: "Q_영업이익_YoY(%)", label: "Q OP YoY", fmt: "f1" },
      { key: "Q_순이익_YoY(%)", label: "Q NP YoY", fmt: "f1" },
      // 가속도
      { key: "실적가속_연속", label: "실적가속" }, { key: "영업이익_가속도", label: "OP가속도", fmt: "f1" },
      { key: "매출_가속도", label: "매출가속도", fmt: "f1" },
      // 기술
      { key: "MA20_이격도(%)", label: "MA20이격", fmt: "f1" }, { key: "MA60_이격도(%)", label: "MA60이격", fmt: "f1" },
      { key: "52주_최고대비(%)", label: "고가대비%", fmt: "f1" }, { key: "RS_등급", label: "RS등급", fmt: "f1" },
      // 수익성
      { key: "영업이익률(%)", label: "OPM%", fmt: "f1" },
      // 점수
      { key: "고성장_점수", label: "성장점수", fmt: "f1" }
    ],
    // 5. 현금배당 - FCF, 배당, 현금흐름 (21개)
    cash_div: [
      // 기본
      { key: "종목코드", label: "코드" }, { key: "종목명", label: "종목명" }, { key: "섹터", label: "섹터" },
      { key: "종가", label: "현재가", fmt: "int" }, { key: "시가총액", label: "시총", fmt: "eok" },
      // 배당
      { key: "배당수익률(%)", label: "배당%", fmt: "f2" }, { key: "배당성향(%)", label: "배당성향%", fmt: "f1" },
      { key: "DPS_CAGR", label: "DPS CAGR", fmt: "f1" }, { key: "배당_연속증가", label: "배당연속", fmt: "int" },
      { key: "배당_수익동반증가", label: "동반성장", fmt: "flag" }, { key: "배당_경고신호", label: "배당경고", fmt: "flag" },
      // 현금흐름
      { key: "FCF수익률(%)", label: "FCF%", fmt: "f2" }, { key: "현금전환율(%)", label: "현금전환%", fmt: "f1" },
      { key: "이익품질_양호", label: "이익품질", fmt: "flag" },
      // 수익성
      { key: "ROIC(%)", label: "ROIC%", fmt: "f1" }, { key: "ROE(%)", label: "ROE%", fmt: "f2" },
      { key: "영업이익률(%)", label: "OPM%", fmt: "f1" },
      // 밸류
      { key: "PER", label: "PER", fmt: "f2" }, { key: "PBR", label: "PBR", fmt: "f2" },
      // 안정성
      { key: "부채비율(%)", label: "부채%", fmt: "f1" },
      // 점수
      { key: "현금배당_점수", label: "배당점수", fmt: "f1" }
    ],
    // 6. 턴어라운드 - 전환신호, 이익률, 수급 (21개)
    turnaround: [
      // 기본
      { key: "종목코드", label: "코드" }, { key: "종목명", label: "종목명" }, { key: "섹터", label: "섹터" },
      { key: "종가", label: "현재가", fmt: "int" }, { key: "시가총액", label: "시총", fmt: "eok" },
      // 전환 신호
      { key: "흑자전환", label: "흑자전환", fmt: "flag" }, { key: "이익률_급개선", label: "OPM급등", fmt: "flag" },
      { key: "ROIC_개선", label: "ROIC↑", fmt: "flag" }, { key: "퀄리티_턴어라운드", label: "퀄리티TA", fmt: "flag" },
      { key: "실적가속_연속", label: "실적가속" },
      // 이익률 변화
      { key: "이익률_변동폭", label: "OPM변동", fmt: "f1" }, { key: "GPM_변화(pp)", label: "GPM변화", fmt: "f1" },
      { key: "GPM_최근(%)", label: "GPM%", fmt: "f1" },
      // TTM 실적
      { key: "TTM_순이익", label: "TTM NI", fmt: "int" }, { key: "ROIC(%)", label: "ROIC%", fmt: "f1" },
      // 수급/기술
      { key: "스마트머니_승률", label: "SM승률", fmt: "f1" }, { key: "VCP_신호", label: "VCP", fmt: "flag" },
      { key: "RSI_14", label: "RSI", fmt: "f1" }, { key: "52주_최저대비(%)", label: "저가대비%", fmt: "f1" },
      // 밸류 (턴 후 저평가 확인)
      { key: "PBR", label: "PBR", fmt: "f2" },
      // 점수
      { key: "턴어라운드_점수", label: "턴점수", fmt: "f1" }
    ],
    // 7. Multi-Strategy (3관왕) - 5개 전략 점수 (18개)
    multi_strategy: [
      // 기본
      { key: "종목코드", label: "코드" }, { key: "종목명", label: "종목명" }, { key: "섹터", label: "섹터" },
      { key: "종가", label: "현재가", fmt: "int" }, { key: "시가총액", label: "시총", fmt: "eok" },
      // 전략 요약
      { key: "전략수", label: "전략수", fmt: "int" },
      // 전략별 점수
      { key: "종합점수", label: "종합점수", fmt: "f1" },
      { key: "성장성_점수", label: "성장점수", fmt: "f1" }, { key: "안정성_점수", label: "안정점수", fmt: "f1" },
      { key: "가격_점수", label: "가격점수", fmt: "f1" }, { key: "주도주_점수", label: "주도점수", fmt: "f1" },
      { key: "우량가치_점수", label: "우량점수", fmt: "f1" }, { key: "고성장_점수", label: "성장전략점수", fmt: "f1" },
      { key: "현금배당_점수", label: "배당점수", fmt: "f1" },
      // 핵심 팩터
      { key: "ROE(%)", label: "ROE%", fmt: "f2" }, { key: "영업이익_CAGR", label: "OP CAGR", fmt: "f1" },
      { key: "부채비율(%)", label: "부채%", fmt: "f1" }, { key: "RS_등급", label: "RS등급", fmt: "f1" },
      { key: "F스코어", label: "F-Score", fmt: "int" }
    ],
    // 8. Forward 추정치 - 커버리지 종목 내 모멘텀 (18개)
    forward_covered: [
      // 기본
      { key: "종목코드", label: "코드" }, { key: "종목명", label: "종목명" }, { key: "섹터", label: "섹터" },
      { key: "종가", label: "현재가", fmt: "int" }, { key: "시가총액", label: "시총", fmt: "eok" },
      // 현재 밸류 (비교 기준)
      { key: "PER", label: "PER(현)", fmt: "f2" }, { key: "PBR", label: "PBR(현)", fmt: "f2" },
      // Forward 밸류
      { key: "Fwd_PER", label: "Fwd PER", fmt: "f2" }, { key: "Fwd_PBR", label: "Fwd PBR", fmt: "f2" },
      { key: "Fwd_ROE(%)", label: "Fwd ROE%", fmt: "f1" }, { key: "Fwd_OPM(%)", label: "Fwd OPM%", fmt: "f1" },
      // Forward 성장률
      { key: "Fwd_매출_성장률(%)", label: "Fwd 매출성장%", fmt: "f1" },
      { key: "Fwd_영업이익_성장률(%)", label: "Fwd OP성장%", fmt: "f1" },
      { key: "Fwd_순이익_성장률(%)", label: "Fwd NP성장%", fmt: "f1" },
      { key: "Fwd_2yr_영업이익_성장(%)", label: "2yr OP성장%", fmt: "f1" },
      // 실적 현황
      { key: "Q_영업이익_YoY(%)", label: "Q OP YoY", fmt: "f1" }, { key: "ROE(%)", label: "ROE%(현)", fmt: "f2" },
      // 점수
      { key: "Fwd_모멘텀_점수", label: "Fwd점수", fmt: "f1" }
    ],
    // 9. 관심종목 - 종합 모니터링 (30개)
    watchlist: [
      // 기본
      { key: "종목코드", label: "코드" }, { key: "종목명", label: "종목명" }, { key: "섹터", label: "섹터" },
      { key: "종가", label: "현재가", fmt: "int" }, { key: "시가총액", label: "시총", fmt: "eok" },
      // 가치평가
      { key: "PER", label: "PER", fmt: "f2" }, { key: "PBR", label: "PBR", fmt: "f2" },
      { key: "PEG", label: "PEG", fmt: "f2" }, { key: "PSR", label: "PSR", fmt: "f2" },
      { key: "적정주가_SRIM", label: "적정가", fmt: "int" }, { key: "괴리율(%)", label: "괴리율%", fmt: "f1" },
      // 수익성
      { key: "ROE(%)", label: "ROE%", fmt: "f2" }, { key: "ROIC(%)", label: "ROIC%", fmt: "f1" },
      { key: "영업이익률(%)", label: "OPM%", fmt: "f1" }, { key: "FCF수익률(%)", label: "FCF%", fmt: "f2" },
      // 성장성
      { key: "매출_CAGR", label: "매출CAGR", fmt: "f1" }, { key: "영업이익_CAGR", label: "OP CAGR", fmt: "f1" },
      { key: "Q_영업이익_YoY(%)", label: "Q OP YoY", fmt: "f1" }, { key: "실적가속_연속", label: "실적가속" },
      // 안정성
      { key: "부채비율(%)", label: "부채%", fmt: "f1" }, { key: "F스코어", label: "F-Score", fmt: "int" },
      { key: "이익품질_양호", label: "이익품질", fmt: "flag" },
      // 배당
      { key: "배당수익률(%)", label: "배당%", fmt: "f2" }, { key: "배당_연속증가", label: "배당연속", fmt: "int" },
      // 기술적/수급
      { key: "RS_등급", label: "RS등급", fmt: "f1" }, { key: "수급강도", label: "수급", fmt: "f1" },
      { key: "52주_최고대비(%)", label: "고가대비%", fmt: "f1" }, { key: "RSI_14", label: "RSI", fmt: "f1" },
      { key: "거래대금_20일평균", label: "거래(평)", fmt: "eok" },
      // 종합
      { key: "종합점수", label: "점수", fmt: "f1" }
    ],
    // 10. 포트폴리오 - 보유종목 관리 (12개)
    portfolio: [
      { key: "종목코드",  label: "코드",    w: "62px"  },
      { key: "종목명",   label: "종목명",   w: "110px", align: "left" },
      { key: "섹터",    label: "섹터",    w: "90px",  align: "left" },
      { key: "종목구분",  label: "구분",    w: "52px"  },
      { key: "현재가",   label: "현재가",   w: "80px",  fmt: "int" },
      { key: "수량",    label: "수량",    w: "54px",  fmt: "int" },
      { key: "평균매입가", label: "매입가",   w: "80px",  fmt: "int" },
      { key: "매입금액",  label: "매입금액",  w: "88px",  fmt: "int" },
      { key: "평가금액",  label: "평가금액",  w: "88px",  fmt: "int" },
      { key: "수익금액",  label: "수익금",   w: "88px",  fmt: "int" },
      { key: "수익률",   label: "수익률%",  w: "72px",  fmt: "f2" },
      { key: "비중",    label: "비중%",   w: "58px",  fmt: "f1" },
    ]
  };

  // ─── 탭별 그룹 구분선 경계 컬럼 ──────────────────────────────────────────
  const TAB_GROUP_STARTS = {
    all:            new Set(["PER", "ROE(%)", "영업이익_CAGR", "부채비율(%)", "수급강도", "배당수익률(%)", "종합점수"]),
    leaders:        new Set(["수급강도", "RS_등급", "거래대금_증감(%)", "주도주_점수"]),
    quality_value:  new Set(["PER", "ROE(%)", "F스코어", "영업이익_CAGR", "우량가치_점수"]),
    growth_mom:     new Set(["매출_CAGR", "Q_매출_YoY(%)", "실적가속_연속", "MA20_이격도(%)", "영업이익률(%)", "고성장_점수"]),
    cash_div:       new Set(["배당수익률(%)", "FCF수익률(%)", "ROIC(%)", "PER", "부채비율(%)", "현금배당_점수"]),
    turnaround:     new Set(["흑자전환", "이익률_변동폭", "TTM_순이익", "스마트머니_승률", "PBR", "턴어라운드_점수"]),
    multi_strategy: new Set(["전략수", "종합점수", "ROE(%)", "F스코어"]),
    forward_covered:new Set(["PER", "Fwd_PER", "Fwd_매출_성장률(%)", "Q_영업이익_YoY(%)", "Fwd_모멘텀_점수"]),
    watchlist:      new Set(["PER", "ROE(%)", "매출_CAGR", "부채비율(%)", "배당수익률(%)", "RS_등급", "종합점수"]),
  };
  const TAB_GROUP_LABELS = {
    all: {
      "PER": "— 가치평가 —", "ROE(%)": "— 수익성 —", "영업이익_CAGR": "— 성장 —",
      "부채비율(%)": "— 안정성 —", "수급강도": "— 수급/기술 —", "배당수익률(%)": "— 배당 —", "종합점수": "— 종합 —"
    },
    leaders: {
      "수급강도": "— 수급 —", "RS_등급": "— RS/기술 —", "거래대금_증감(%)": "— 거래/실적 —", "주도주_점수": "— 점수 —"
    },
    quality_value: {
      "PER": "— 밸류 —", "ROE(%)": "— 수익성 —", "F스코어": "— 안정성 —",
      "영업이익_CAGR": "— 성장 —", "우량가치_점수": "— 점수 —"
    },
    growth_mom: {
      "매출_CAGR": "— CAGR —", "Q_매출_YoY(%)": "— 분기YoY —", "실적가속_연속": "— 가속도 —",
      "MA20_이격도(%)": "— 기술 —", "영업이익률(%)": "— 수익성 —", "고성장_점수": "— 점수 —"
    },
    cash_div: {
      "배당수익률(%)": "— 배당 —", "FCF수익률(%)": "— 현금흐름 —", "ROIC(%)": "— 수익성 —",
      "PER": "— 밸류 —", "부채비율(%)": "— 안정성 —", "현금배당_점수": "— 점수 —"
    },
    turnaround: {
      "흑자전환": "— 전환신호 —", "이익률_변동폭": "— 이익률 —", "TTM_순이익": "— TTM —",
      "스마트머니_승률": "— 수급/기술 —", "PBR": "— 밸류 —", "턴어라운드_점수": "— 점수 —"
    },
    multi_strategy: {
      "전략수": "— 전략수 —", "종합점수": "— 전략점수 —", "ROE(%)": "— 핵심팩터 —"
    },
    forward_covered: {
      "PER": "— 현재 —", "Fwd_PER": "— Fwd밸류 —", "Fwd_매출_성장률(%)": "— Fwd성장 —",
      "Q_영업이익_YoY(%)": "— 실적 —", "Fwd_모멘텀_점수": "— 점수 —"
    },
    watchlist: {
      "PER": "— 가치평가 —", "ROE(%)": "— 수익성 —", "매출_CAGR": "— 성장성 —",
      "부채비율(%)": "— 안정성 —", "배당수익률(%)": "— 배당 —",
      "RS_등급": "— 기술/수급 —", "종합점수": "— 종합 —"
    },
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
        { col: "PER",             label: "PER" },
        { col: "PBR",             label: "PBR" },
        { col: "PSR",             label: "PSR" },
        { col: "PEG",             label: "PEG" },
        { col: "이익수익률(%)",   label: "이익수익률%" },
        { col: "괴리율(%)",       label: "S-RIM 괴리%" },
        { col: "적정주가_SRIM",   label: "적정주가(원)" },
      ]
    },
    {
      key: "profitability", label: "수익성",
      fields: [
        { col: "ROE(%)",          label: "ROE%" },
        { col: "ROIC(%)",         label: "ROIC%" },
        { col: "ROIC_전년(%)",    label: "전년ROIC%" },
        { col: "영업이익률(%)",   label: "영업이익률%" },
        { col: "영업이익률_최근", label: "최근OP이익률" },
        { col: "영업이익률_전년", label: "전년OP이익률" },
        { col: "FCF수익률(%)",    label: "FCF수익률%" },
        { col: "현금전환율(%)",   label: "현금전환율%" },
        { col: "CAPEX비율(%)",    label: "CAPEX비율%" },
        { col: "GPM_최근(%)",     label: "매출총이익률%" },
        { col: "GPM_전년(%)",     label: "전년GPM%" },
        { col: "GPM_변화(pp)",    label: "GPM변화(pp)" },
      ]
    },
    {
      key: "growth", label: "성장성",
      fields: [
        { col: "매출_CAGR",             label: "매출CAGR%" },
        { col: "영업이익_CAGR",         label: "OP CAGR%" },
        { col: "순이익_CAGR",           label: "NP CAGR%" },
        { col: "영업CF_CAGR",           label: "OCF CAGR%" },
        { col: "FCF_CAGR",              label: "FCF CAGR%" },
        { col: "Q_매출_YoY(%)",         label: "Q 매출YoY%" },
        { col: "Q_영업이익_YoY(%)",     label: "Q OP YoY%" },
        { col: "Q_순이익_YoY(%)",       label: "Q NP YoY%" },
        { col: "TTM_매출_YoY(%)",       label: "TTM 매출YoY%" },
        { col: "TTM_영업이익_YoY(%)",   label: "TTM OP YoY%" },
        { col: "TTM_순이익_YoY(%)",     label: "TTM NP YoY%" },
        { col: "영업이익_가속도",       label: "OP 가속도" },
        { col: "매출_가속도",           label: "매출 가속도" },
      ]
    },
    {
      key: "consistency", label: "연속성",
      fields: [
        { col: "매출_연속성장",           label: "매출연속성장" },
        { col: "영업이익_연속성장",       label: "OP연속성장" },
        { col: "순이익_연속성장",         label: "NP연속성장" },
        { col: "영업CF_연속성장",         label: "OCF연속성장" },
        { col: "Q_매출_연속YoY성장",      label: "Q매출연속YoY" },
        { col: "Q_영업이익_연속YoY성장",  label: "Q OP연속YoY" },
        { col: "Q_순이익_연속YoY성장",    label: "Q NP연속YoY" },
        { col: "실적가속_연속",           label: "실적가속연속" },
        { col: "이익률_변동폭",           label: "이익률변동폭" },
        { col: "데이터_연수",             label: "데이터연수" },
      ]
    },
    {
      key: "stability", label: "안정성",
      fields: [
        { col: "F스코어",        label: "F-Score" },
        { col: "부채비율(%)",    label: "부채비율%" },
        { col: "유동비율(%)",    label: "유동비율%" },
        { col: "부채상환능력",   label: "부채상환능력" },
        { col: "이자보상배율",   label: "이자보상배율" },
      ]
    },
    {
      key: "technical", label: "기술적",
      fields: [
        { col: "RSI_14",             label: "RSI(14)" },
        { col: "수급강도",           label: "수급강도" },
        { col: "MA20_이격도(%)",     label: "MA20 이격%" },
        { col: "MA60_이격도(%)",     label: "MA60 이격%" },
        { col: "52주_최저대비(%)",   label: "52주최저대비%" },
        { col: "52주_최고대비(%)",   label: "52주최고대비%" },
        { col: "변동성_60일(%)",     label: "변동성(60일)%" },
        { col: "거래대금_20일평균",  label: "거래대금(20일)", unit: 1e8 },
        { col: "거래대금_증감(%)",   label: "거래대금증감%" },
      ]
    },
    {
      key: "supply", label: "수급",
      fields: [
        { col: "외인순매수_20d",   label: "외인순매수(20d)" },
        { col: "기관순매수_20d",   label: "기관순매수(20d)" },
        { col: "스마트머니_승률",  label: "스마트머니승률" },
        { col: "양매수_비율",      label: "양매수비율" },
        { col: "RS_60d",           label: "RS 60일" },
        { col: "RS_120d",          label: "RS 120일" },
        { col: "RS_250d",          label: "RS 250일" },
        { col: "Composite_RS",     label: "종합RS" },
        { col: "RS_등급",          label: "RS등급" },
      ]
    },
    {
      key: "dividend", label: "배당",
      fields: [
        { col: "배당수익률(%)",      label: "배당수익률%" },
        { col: "DPS_CAGR",           label: "DPS CAGR%" },
        { col: "배당_연속증가",      label: "배당연속증가" },
        { col: "배당성향(%)",        label: "배당성향%" },
        { col: "배당_수익동반증가",  label: "수익동반증가" },
      ]
    },
    {
      key: "turnaround", label: "턴어라운드",
      fields: [
        { col: "흑자전환",          label: "흑자전환" },
        { col: "이익률_개선",       label: "이익률개선" },
        { col: "이익률_급개선",     label: "이익률급개선" },
        { col: "ROIC_개선",         label: "ROIC개선" },
        { col: "퀄리티_턴어라운드", label: "퀄리티턴어라운드" },
        { col: "VCP_신호",          label: "VCP신호" },
      ]
    },
    {
      key: "market", label: "시가총액 / 점수",
      fields: [
        { col: "시가총액",         label: "시가총액(억)", unit: 1e8 },
        { col: "종합점수",         label: "종합점수" },
        { col: "성장성_점수",      label: "성장성점수" },
        { col: "안정성_점수",      label: "안정성점수" },
        { col: "가격_점수",        label: "가격점수" },
        { col: "주도주_점수",      label: "주도주점수" },
        { col: "우량가치_점수",    label: "우량가치점수" },
        { col: "고성장_점수",      label: "고성장점수" },
        { col: "현금배당_점수",    label: "현금배당점수" },
        { col: "턴어라운드_점수",  label: "턴어라운드점수" },
        { col: "전략수",           label: "통과전략수" },
      ]
    },
    {
      key: "fundamental", label: "재무 규모",
      fields: [
        { col: "TTM_매출",      label: "TTM매출(억)",  unit: 1e8 },
        { col: "TTM_영업이익",  label: "TTM OP(억)",   unit: 1e8 },
        { col: "TTM_순이익",    label: "TTM NP(억)",   unit: 1e8 },
        { col: "TTM_영업CF",    label: "TTM OCF(억)",  unit: 1e8 },
        { col: "TTM_FCF",       label: "TTM FCF(억)",  unit: 1e8 },
        { col: "자본",          label: "자본(억)",     unit: 1e8 },
        { col: "부채",          label: "부채(억)",     unit: 1e8 },
      ]
    },
    {
      key: "forward", label: "Forward 추정치",
      fields: [
        { col: "Fwd_PER",                  label: "Fwd PER" },
        { col: "Fwd_PBR",                  label: "Fwd PBR" },
        { col: "Fwd_ROE(%)",               label: "Fwd ROE%" },
        { col: "Fwd_OPM(%)",               label: "Fwd OPM%" },
        { col: "Fwd_영업이익_성장률(%)",   label: "Fwd OP성장%" },
        { col: "Fwd_매출_성장률(%)",       label: "Fwd 매출성장%" },
        { col: "Fwd_순이익_성장률(%)",     label: "Fwd NP성장%" },
        { col: "Fwd_2yr_영업이익_성장(%)", label: "Fwd 2yr OP성장%" },
        { col: "컨센서스_커버리지",        label: "커버리지" },
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
      if (!res.ok) return;
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
      if (!res.ok) return;
      tabCounts = await res.json();
      renderTabBadges();
    } catch (e) { console.error("loadTabCounts:", e); }
  }

  function renderTabBadges() {
    document.querySelectorAll("#screen-tabs .nav-link[data-screen]").forEach(link => {
      const screen = link.dataset.screen;
      if (screen === "watchlist" || screen === "portfolio") return;

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
      if (!res.ok) return;
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

    // 포트폴리오 탭은 전용 API 사용
    if (currentScreen === "portfolio") {
      const pf = await loadPortfolio();
      renderPortfolioSummary();
      if (!pf || !pf.items.length) {
        renderPortfolioTable([]);
        pageInfo.textContent = "0건";
      } else {
        renderPortfolioTable(pf.items);
        pageInfo.textContent = `${pf.items.length}건`;
      }
      btnPrev.disabled = true;
      btnNext.disabled = true;
      return;
    }

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
    const sectors = getSelectedSectors();
    if (market)          params.set("market", market);
    if (sectors.length)  params.set("sectors", sectors.join(","));
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
      if (!res.ok) throw new Error(`서버 오류: ${res.status}`);
      const data = await res.json();
      renderTable(data.items);
      const totalPages = Math.ceil(data.total / data.size) || 1;
      pageInfo.textContent = `Page ${data.page} / ${totalPages} (${data.total.toLocaleString()}종목)`;
      btnPrev.disabled = data.page <= 1;
      btnNext.disabled = data.page >= totalPages;

      // 현재 탭 카운트 실시간 갱신 (서버 재시작 전에도 보임)
      if (currentScreen !== "watchlist" && currentScreen !== "portfolio") {
        tabCounts[currentScreen] = data.total;
        renderTabBadges();
      }
    } catch (e) { console.error("loadStocks:", e); }
  }

  // ─── 테이블 렌더링 ────────────────────────────────────────────────────
  function renderTable(items) {
    // 포트폴리오 탭에서 설정된 fixed layout 원복
    const tableEl2 = document.getElementById("stock-table");
    if (tableEl2) { tableEl2.style.tableLayout = ""; tableEl2.style.minWidth = "1000px"; }
    const cg = tableEl2?.querySelector("colgroup");
    if (cg) cg.remove();

    const cols = COLUMNS[currentScreen] || COLUMNS.all;
    if (!items.length) {
      tbody.innerHTML = `<tr><td colspan="${cols.length + 4}" class="text-center py-4 text-muted">데이터 없음</td></tr>`;
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
      const pfBtn    = `<td class="text-center p-1"><button class="btn btn-sm btn-outline-info pf-add-btn" data-code="${code}" data-name="${(s["종목명"]||"").replace(/"/g,"&quot;")}" style="font-size:.6rem;padding:1px 4px;line-height:1.2;">+PF</button></td>`;
      const compareCb = `<td class="text-center p-1"><input type="checkbox" class="compare-cb" data-code="${code}" ${compareSet.has(code) ? "checked" : ""}></td>`;
      const star      = `<td class="text-center p-1"><button class="watch-btn${watched ? " watched" : ""}" data-code="${code}">${watched ? "★" : "☆"}</button></td>`;
      const groupStartSet = TAB_GROUP_STARTS[currentScreen] || new Set();
      const cells = cols.map(c => {
        let cls = groupStartSet.has(c.key) ? "wl-group-start" : "";
        if (["거래대금_증감(%)", "수급강도", "MA20_이격도(%)", "MA60_이격도(%)",
             "Q_매출_YoY(%)", "Q_영업이익_YoY(%)", "Q_순이익_YoY(%)",
             "TTM_매출_YoY(%)", "TTM_영업이익_YoY(%)", "TTM_순이익_YoY(%)",
             "매출_CAGR", "영업이익_CAGR", "순이익_CAGR", "괴리율(%)"].includes(c.key)) {
          cls += (cls ? " " : "") + valClass(s[c.key]);
        }
        const newBadge = (isNew && c.key === "종목명")
          ? ' <span class="badge badge-new bg-success">NEW</span>' : "";
        const rep = reportMap[code];
        const aiBadge = (rep && c.key === "종목명")
          ? ` <span class="badge badge-ai" title="${rep.model} · ${rep.date}">AI</span>` : "";
        return `<td class="${cls}">${fmt(s[c.key], c.fmt)}${newBadge}${aiBadge}</td>`;
      }).join("");
      return `<tr data-code="${code}" class="${isNew ? "row-new" : ""}">${pfBtn}${compareCb}${star}${cells}</tr>`;
    }).join("");

    tbody.querySelectorAll(".watch-btn").forEach(btn =>
      btn.addEventListener("click", e => { e.stopPropagation(); toggleWatch(btn.dataset.code); })
    );
    tbody.querySelectorAll(".compare-cb").forEach(cb =>
      cb.addEventListener("change", e => { e.stopPropagation(); toggleCompare(cb.dataset.code, cb.checked); })
    );
    tbody.querySelectorAll(".pf-add-btn").forEach(btn =>
      btn.addEventListener("click", e => { e.stopPropagation(); openPortfolioAdd(btn.dataset.code, btn.dataset.name); })
    );
    tbody.querySelectorAll("tr[data-code]").forEach(tr =>
      tr.addEventListener("click", e => {
        if (e.target.classList.contains("watch-btn") || e.target.classList.contains("compare-cb") || e.target.classList.contains("pf-add-btn")) return;
        openDetail(tr.dataset.code);
      })
    );
    initTooltips();
  }

  // ─── 헤더 구성 ────────────────────────────────────────────────────────
  function buildHeader() {
    const cols = COLUMNS[currentScreen] || COLUMNS.all;
    // 컬럼 수에 따라 테이블 최소 너비 동적 설정 (횡스크롤)
    const tbl = document.getElementById("stock-table");
    if (tbl) tbl.style.minWidth = Math.max(1000, cols.length * 95 + 120) + "px";
    // 탭별 그룹 경계 컬럼 (왼쪽 굵은 선) 및 레이블 정의
    const TAB_GROUP_STARTS = {
      all:            ["PER", "ROE(%)", "영업이익_CAGR", "부채비율(%)", "수급강도", "배당수익률(%)", "종합점수"],
      leaders:        ["수급강도", "RS_등급", "거래대금_증감(%)", "주도주_점수"],
      quality_value:  ["PER", "ROE(%)", "F스코어", "영업이익_CAGR", "우량가치_점수"],
      growth_mom:     ["매출_CAGR", "Q_매출_YoY(%)", "실적가속_연속", "MA20_이격도(%)", "영업이익률(%)", "고성장_점수"],
      cash_div:       ["배당수익률(%)", "FCF수익률(%)", "ROIC(%)", "PER", "부채비율(%)", "현금배당_점수"],
      turnaround:     ["흑자전환", "이익률_변동폭", "TTM_순이익", "스마트머니_승률", "PBR", "턴어라운드_점수"],
      multi_strategy: ["전략수", "종합점수", "ROE(%)", "F스코어"],
      forward_covered:["PER", "Fwd_PER", "Fwd_매출_성장률(%)", "Q_영업이익_YoY(%)", "Fwd_모멘텀_점수"],
      watchlist:      ["PER", "ROE(%)", "매출_CAGR", "부채비율(%)", "배당수익률(%)", "RS_등급", "종합점수"],
    };
    const TAB_GROUP_LABELS = {
      all: {
        "PER": "— 가치평가 —", "ROE(%)": "— 수익성 —", "영업이익_CAGR": "— 성장 —",
        "부채비율(%)": "— 안정성 —", "수급강도": "— 수급/기술 —", "배당수익률(%)": "— 배당 —", "종합점수": "— 종합 —"
      },
      leaders: {
        "수급강도": "— 수급 —", "RS_등급": "— RS/기술 —", "거래대금_증감(%)": "— 거래/실적 —", "주도주_점수": "— 점수 —"
      },
      quality_value: {
        "PER": "— 밸류 —", "ROE(%)": "— 수익성 —", "F스코어": "— 안정성 —", "영업이익_CAGR": "— 성장 —", "우량가치_점수": "— 점수 —"
      },
      growth_mom: {
        "매출_CAGR": "— CAGR —", "Q_매출_YoY(%)": "— 분기YoY —", "실적가속_연속": "— 가속도 —",
        "MA20_이격도(%)": "— 기술 —", "영업이익률(%)": "— 수익성 —", "고성장_점수": "— 점수 —"
      },
      cash_div: {
        "배당수익률(%)": "— 배당 —", "FCF수익률(%)": "— 현금흐름 —", "ROIC(%)": "— 수익성 —",
        "PER": "— 밸류 —", "부채비율(%)": "— 안정성 —", "현금배당_점수": "— 점수 —"
      },
      turnaround: {
        "흑자전환": "— 전환신호 —", "이익률_변동폭": "— 이익률 —", "TTM_순이익": "— TTM —",
        "스마트머니_승률": "— 수급/기술 —", "PBR": "— 밸류 —", "턴어라운드_점수": "— 점수 —"
      },
      multi_strategy: {
        "전략수": "— 전략수 —", "종합점수": "— 전략점수 —", "ROE(%)": "— 핵심팩터 —", "F스코어": "—"
      },
      forward_covered: {
        "PER": "— 현재 —", "Fwd_PER": "— Fwd밸류 —", "Fwd_매출_성장률(%)": "— Fwd성장 —",
        "Q_영업이익_YoY(%)": "— 실적 —", "Fwd_모멘텀_점수": "— 점수 —"
      },
      watchlist: {
        "PER": "— 가치평가 —", "ROE(%)": "— 수익성 —", "매출_CAGR": "— 성장성 —",
        "부채비율(%)": "— 안정성 —", "배당수익률(%)": "— 배당 —",
        "RS_등급": "— 기술/수급 —", "종합점수": "— 종합 —"
      },
    };
    const WL_GROUP_STARTS = new Set(TAB_GROUP_STARTS[currentScreen] || []);
    const WL_GROUP_LABELS = TAB_GROUP_LABELS[currentScreen] || {};
    headerRow.innerHTML =
      `<tr><th width="30" class="text-center small text-muted">+PF</th><th width="20" class="text-center small text-muted"></th><th width="30">★</th>` +
      cols.map(c => {
        const arrow = sortCol === c.key ? (sortOrder === "desc" ? " ↓" : " ↑") : "";
        const tip = METRIC_TOOLTIPS[c.key]
          ? ` data-bs-toggle="tooltip" data-bs-placement="bottom" title="${METRIC_TOOLTIPS[c.key]}"` : "";
        const groupBorder = WL_GROUP_STARTS.has(c.key) ? " wl-group-start" : "";
        const groupLabel  = WL_GROUP_LABELS[c.key] ? `<div class="wl-group-label">${WL_GROUP_LABELS[c.key]}</div>` : "";
        return `<th data-col="${c.key}" class="${groupBorder}" style="cursor:pointer; user-select:none;"${tip}>${groupLabel}${c.label}<span class="sort-arrow text-muted small">${arrow}</span></th>`;
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
      if (!res.ok) throw new Error("종목 정보를 불러올 수 없습니다.");
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
    const isProxy = stock["_proxy"] === true;
    const proxyName = stock["_proxy_name"] || "";
    const proxyFrom = stock["_proxy_from"] || "";

    const rep = reportMap[code];
    const aiHeaderBadge = rep
      ? `<span class="badge badge-ai ms-2" title="${rep.model}으로 분석됨 · ${rep.date}">AI 분석 완료</span>` : "";

    const proxyBadge = isProxy
      ? `<div class="alert alert-info py-1 px-2 mt-1 mb-0 small d-inline-block">
           <strong>우선주</strong> ${proxyName} (${proxyFrom}) → <strong>${name}</strong> (${code}) 보통주 지표 기준
         </div>` : "";

    document.getElementById("detail-title").innerHTML =
      `<strong>${isProxy ? proxyName : name}</strong> <span class="text-muted fs-6">${isProxy ? proxyFrom : code}</span>
       <span class="badge ${market === "KOSPI" ? "bg-primary" : "bg-danger"} ms-2">${market}</span>
       ${sector ? `<span class="badge bg-secondary ms-1">${sector}</span>` : ""}
       ${price != null ? `<span class="ms-2 fw-bold">${fmt(price, "int")}원</span>` : ""}
       ${aiHeaderBadge}${proxyBadge}`;

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
    const claudeBtn = document.getElementById("btn-analysis-claude");
    claudeBtn.dataset.code = code;

    // 보고서 캐시 상태를 버튼에 반영
    claudeBtn.classList.remove("has-report");
    claudeBtn.innerHTML = "🤖 Claude AI 분석";
    if (rep) {
      claudeBtn.innerHTML = "✓ Claude AI 분석 (캐시됨)";
      claudeBtn.classList.add("has-report");
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
      if (!res.ok) { area.style.display = "none"; return; }
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
  // 현재 백그라운드 분석 중인 종목 정보
  let _analysisRunningCode = null;  // 종목코드 or null
  let _analysisRunningName = null;  // 종목명 or null
  let _analysisPollingTimer = null;

  function _setAnalysisLock(locked, runningCode, runningName) {
    _analysisRunningCode = locked ? (runningCode || null) : null;
    _analysisRunningName = locked ? (runningName || null) : null;
    const analysisBtn = document.getElementById("btn-analysis-claude");
    const regenBtn    = document.getElementById("btn-regenerate");
    if (analysisBtn) {
      analysisBtn.disabled = locked;
      analysisBtn.textContent = locked ? "⏳ 분석 진행중..." : "🤖 Claude AI 분석";
    }
    if (regenBtn) {
      regenBtn.disabled = locked;
      regenBtn.textContent = locked ? "분석 진행중..." : "재생성";
    }
  }

  async function loadReportHistory(code) {
    const sel = document.getElementById("report-history-select");
    sel.innerHTML = '<option value="">이전 보고서</option>';
    sel.style.display = "none";
    try {
      const res = await fetch(`/api/stocks/${code}/analysis/history`);
      if (!res.ok) return;
      const list = await res.json();
      if (!list.length) return;
      list.forEach(h => {
        const opt = document.createElement("option");
        opt.value = h.id;
        opt.textContent = h.generated_date;
        sel.appendChild(opt);
      });
      sel.style.display = "";
    } catch { /* ignore */ }
  }

  async function showHistoryReport(historyId, code) {
    if (!historyId) return;
    const diffArea = document.getElementById("report-diff-area");
    const contentEl = document.getElementById("report-content");
    const metaEl = document.getElementById("report-meta");
    diffArea.style.display = "none";
    diffArea.innerHTML = "";
    contentEl.innerHTML = '<div class="text-center py-3"><div class="spinner-border spinner-border-sm"></div></div>';
    try {
      const res = await fetch(`/api/stocks/analysis/history/${historyId}`);
      if (!res.ok) throw new Error("조회 실패");
      const data = await res.json();
      contentEl.innerHTML =
        `<div class="history-banner mb-3">
          <span>📄 이전 보고서를 보고 있습니다 (${data.generated_date})</span>
          <button class="btn btn-sm btn-outline-primary" id="btn-back-to-current">최신 보고서로 돌아가기</button>
        </div>` + (data.report_html || "");
      metaEl.textContent = `${data.model || ""} · ${data.generated_date} (이전 보고서)`;
      document.getElementById("btn-back-to-current").addEventListener("click", () => {
        document.getElementById("report-history-select").value = "";
        requestAnalysis(code, "claude");
      });
    } catch (e) {
      contentEl.innerHTML = `<div class="alert alert-danger">이전 보고서 조회 실패: ${e.message}</div>`;
    }
  }

  function _showAnalysisResult(code, mode, data) {
    const diffArea = document.getElementById("report-diff-area");
    document.getElementById("report-loading").style.display = "none";
    if (data.error) {
      document.getElementById("report-content").innerHTML =
        `<div class="alert alert-danger"><strong>오류:</strong> ${data.error}</div>`;
    } else {
      if (data.diff_html) {
        diffArea.innerHTML =
          `<div class="diff-header" id="diff-toggle-header">
            <h6>📊 이전 보고서 대비 변경점</h6>
            <span class="diff-toggle">접기 ▲</span>
          </div>
          <div id="diff-body">${data.diff_html}</div>`;
        diffArea.style.display = "";
        document.getElementById("diff-toggle-header").addEventListener("click", () => {
          const body   = document.getElementById("diff-body");
          const toggle = diffArea.querySelector(".diff-toggle");
          if (body.style.display === "none") {
            body.style.display = "";
            toggle.textContent = "접기 ▲";
          } else {
            body.style.display = "none";
            toggle.textContent = "펼치기 ▼";
          }
        });
      }
      document.getElementById("report-content").innerHTML = data.report_html || "";
      document.getElementById("report-meta").textContent  =
        `${data.model || ""} · ${data.generated_date || ""}`;
      const paddedCode = code.toString().padStart(6, "0");
      reportMap[paddedCode] = { model: data.model || "", date: data.generated_date || "" };
      const nameCell = document.querySelector(`tr[data-code="${paddedCode}"] td:nth-child(3)`);
      if (nameCell && !nameCell.querySelector(".badge-ai")) {
        nameCell.insertAdjacentHTML("beforeend",
          ` <span class="badge badge-ai" title="${data.model} · ${data.generated_date}">AI</span>`);
      }
      loadReportHistory(code);
    }
    document.getElementById("btn-regenerate").dataset.code = code;
    document.getElementById("btn-regenerate").dataset.mode = mode;
    _setAnalysisLock(false);
  }

  async function _pollAnalysisStatus(code, mode, elapsed) {
    try {
      const res  = await fetch(`/api/stocks/${code}/analysis/status`);
      const data = await res.json();
      if (data.status === "running") {
        document.getElementById("report-loading-text").textContent =
          `Claude로 심층 분석 중... (${elapsed}초)`;
        _analysisPollingTimer = setTimeout(() => _pollAnalysisStatus(code, mode, elapsed + 3), 3000);
      } else {
        // done or error
        _showAnalysisResult(code, mode, data);
      }
    } catch (e) {
      document.getElementById("report-loading").style.display = "none";
      document.getElementById("report-content").innerHTML =
        `<div class="alert alert-danger">상태 조회 실패: ${e.message}</div>`;
      _setAnalysisLock(false);
    }
  }

  async function requestAnalysis(code, mode, forceRegenerate) {
    // 이미 같은 종목 분석 진행 중 → 모달만 열어서 진행 상태 보여줌
    if (_analysisRunningCode === code) {
      const name = (currentDetailData && currentDetailData["종목명"]) || code;
      const reportModal = new bootstrap.Modal(document.getElementById("report-modal"));
      reportModal.show();
      document.getElementById("report-title").textContent = `AI 분석 보고서 — ${name}`;
      // 로딩 UI가 이미 표시 중이므로 그대로 둠
      return;
    }
    // 다른 종목 분석 진행 중
    if (_analysisRunningCode !== null) {
      const runningLabel = _analysisRunningName || _analysisRunningCode;
      alert(`[${runningLabel}] AI 분석이 진행 중입니다. 완료 후 다시 시도해주세요.`);
      return;
    }

    const name = (currentDetailData && currentDetailData["종목명"]) || code;
    const reportModal = new bootstrap.Modal(document.getElementById("report-modal"));
    reportModal.show();

    const diffArea = document.getElementById("report-diff-area");
    diffArea.style.display = "none";
    diffArea.innerHTML = "";
    document.getElementById("report-title").textContent   = `AI 분석 보고서 — ${name}`;
    document.getElementById("report-loading").style.display = "";
    document.getElementById("report-content").innerHTML   = "";
    document.getElementById("report-meta").textContent    = "";
    document.getElementById("report-loading-text").textContent = "Claude로 심층 분석 중...";

    // 캐시 확인 (재생성 시 건너뜀)
    if (!forceRegenerate) {
      try {
        const getRes = await fetch(`/api/stocks/${code}/analysis`);
        if (getRes.ok) {
          const cached = await getRes.json();
          if (cached.mode === mode) {
            document.getElementById("report-loading").style.display = "none";
            _showAnalysisResult(code, mode, cached);
            return;
          }
        }
      } catch { /* 캐시 없으면 생성으로 진행 */ }
    }

    // 서버에 백그라운드 분석 시작 요청
    try {
      console.log("[AI분석] POST 요청 시작:", code, mode);
      const postRes = await fetch(`/api/stocks/${code}/analysis`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ mode }),
      });
      const postData = await postRes.json();

      if (postData.status === "running") {
        _setAnalysisLock(true, code, name);
        // 3초 간격 폴링 시작
        _analysisPollingTimer = setTimeout(() => _pollAnalysisStatus(code, mode, 3), 3000);
      } else if (postData.error) {
        document.getElementById("report-loading").style.display = "none";
        document.getElementById("report-content").innerHTML =
          `<div class="alert alert-danger"><strong>오류:</strong> ${postData.error}</div>`;
      }
    } catch (e) {
      document.getElementById("report-loading").style.display = "none";
      document.getElementById("report-content").innerHTML =
        `<div class="alert alert-danger">요청 실패: ${e.message}</div>`;
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
      const tipText  = METRIC_TOOLTIPS[f.col] || "";
      const tipAttr  = tipText ? ` data-tip="${tipText.replace(/"/g, '&quot;')}"` : "";
      return `<div class="adv-filter-row${isActive ? " adv-filter-row--active" : ""}">
        <span class="adv-filter-label" title="${f.col}"${tipAttr}>${f.label}${unitNote}</span>
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
      sectors:       getSelectedSectors(),
      search:        document.getElementById("f-search")?.value  || "",
      columnFilters: JSON.parse(JSON.stringify(columnFilters)),
    };
  }

  function applyPreset(snap) {
    if (snap.market !== undefined) document.getElementById("f-market").value = snap.market;
    if (snap.sectors !== undefined) setSelectedSectors(snap.sectors);
    // 하위 호환: 구버전 preset에 단일 sector 문자열이 저장된 경우
    else if (snap.sector !== undefined) setSelectedSectors(snap.sector ? [snap.sector] : []);
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
      if (!res.ok) throw new Error("비교 데이터 로드 실패");
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
        if (!res.ok) continue;
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

  // ─── 포트폴리오 ──────────────────────────────────────────────────────
  let portfolioData = null;

  async function loadPortfolio() {
    try {
      const res = await fetch("/api/portfolio");
      portfolioData = await res.json();
      updatePortfolioCount();
      await loadCashBalance();
      return portfolioData;
    } catch (e) { console.error("loadPortfolio:", e); return null; }
  }

  async function loadCashBalance() {
    try {
      const res = await fetch("/api/portfolio/cash");
      if (!res.ok) return;
      const data = await res.json();
      const input = document.getElementById("input-cash-balance");
      if (input && data.amount > 0) input.value = data.amount;
    } catch (e) { console.error("loadCashBalance:", e); }
  }

  async function saveCashBalance() {
    const input = document.getElementById("input-cash-balance");
    if (!input) return;
    const amount = parseFloat(input.value) || 0;
    try {
      const res = await fetch("/api/portfolio/cash", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ amount }),
      });
      if (!res.ok) throw new Error("저장 실패");
      // 저장 후 portfolioData summary 갱신
      if (portfolioData && portfolioData.summary) {
        portfolioData.summary["예수금"] = amount;
        portfolioData.summary["총자산"] = (portfolioData.summary["총평가금액"] || 0) + amount;
        portfolioData["예수금"] = amount;
      }
      renderPortfolioSummary();
    } catch (e) {
      alert("예수금 저장 중 오류: " + e.message);
    }
  }

  function updatePortfolioCount() {
    const el = document.getElementById("cnt-portfolio");
    if (el && portfolioData) el.textContent = portfolioData.summary.종목수 || 0;
  }

  function renderPortfolioSummary() {
    const bar = document.getElementById("portfolio-summary-bar");
    if (!bar) return;
    if (currentScreen !== "portfolio") { bar.style.display = "none"; return; }
    bar.style.display = "";

    const statsEl = document.getElementById("portfolio-summary-stats");
    const sectorEl = document.getElementById("portfolio-sector-dist");
    if (!portfolioData || !portfolioData.items.length) {
      statsEl.innerHTML = '<span class="text-muted small">포트폴리오가 비어 있습니다. 종목을 추가해보세요.</span>';
      sectorEl.innerHTML = "";
      return;
    }
    const s = portfolioData.summary;
    const plClass = s.총수익금액 >= 0 ? "val-pos" : "val-neg";
    const plSign  = s.총수익금액 >= 0 ? "+" : "";
    const cash = s["예수금"] || 0;
    const totalAssets = s["총자산"] || s.총평가금액 || 0;
    const cashPct = totalAssets > 0 ? (cash / totalAssets * 100).toFixed(1) : "0.0";
    const cashHtml = cash > 0
      ? `<span class="mx-1 text-muted">|</span>
         <span class="text-muted small">💰 예수금 ${Math.round(cash).toLocaleString("ko-KR")}원 (${cashPct}%)</span>
         <span class="mx-1 text-muted">|</span>
         <span class="fw-bold text-secondary">총자산 ${Math.round(totalAssets).toLocaleString("ko-KR")}원</span>`
      : "";
    statsEl.innerHTML = `
      <span class="fw-bold">주식평가 <span class="fs-6">${Math.round(s.총평가금액).toLocaleString("ko-KR")}</span>원</span>
      <span class="mx-1 text-muted">|</span>
      <span class="${plClass} fw-bold">${plSign}${Math.round(s.총수익금액).toLocaleString("ko-KR")}원 (${plSign}${s.총수익률.toFixed(2)}%)</span>
      ${cashHtml}
      <span class="mx-1 text-muted">|</span>
      <span class="text-muted small">${s.종목수}종목</span>`;

    const sectors = portfolioData["섹터별"] || [];
    sectorEl.innerHTML = sectors.slice(0, 8).map(d =>
      `<span class="badge bg-light text-dark border" title="${(d.종목 || []).join(', ')}">${d.섹터} ${d.비중.toFixed(1)}%</span>`
    ).join("");
  }

  function renderPortfolioTable(items) {
    const cols = COLUMNS.portfolio;
    const headerEl = document.getElementById("table-header");
    const tbodyEl  = document.getElementById("stock-tbody");

    // colgroup으로 컬럼 너비 고정
    const tableEl = document.getElementById("stock-table");
    let colgroup = tableEl.querySelector("colgroup");
    if (!colgroup) { colgroup = document.createElement("colgroup"); tableEl.prepend(colgroup); }
    colgroup.innerHTML =
      `<col style="width:55px">` +
      cols.map(c => `<col style="width:${c.w || 'auto'}">`).join("");
    tableEl.style.tableLayout = "fixed";
    tableEl.style.minWidth = "";

    // 헤더 구성 (관리 컬럼 추가)
    headerEl.innerHTML =
      `<tr><th class="text-center">관리</th>` +
      cols.map(c => {
        const arrow = sortCol === c.key ? (sortOrder === "desc" ? " ↓" : " ↑") : "";
        const align = c.align === "left" ? " text-start" : "";
        return `<th data-col="${c.key}" class="${align}" style="cursor:pointer; user-select:none;">${c.label}<span class="sort-arrow text-muted small">${arrow}</span></th>`;
      }).join("") + `</tr>`;

    if (!items.length) {
      tbodyEl.innerHTML = `<tr><td colspan="${cols.length + 1}" class="text-center py-4 text-muted">
        포트폴리오가 비어 있습니다. '+ 종목 추가' 버튼으로 종목을 추가하세요.</td></tr>`;
      return;
    }

    const _gbnBadge = gbn => {
      if (!gbn || gbn === "보통주") return "";
      const map = { "ETF": "primary", "우선주": "secondary", "리츠": "success", "스팩": "warning" };
      const color = map[gbn] || "dark";
      return `<span class="badge bg-${color}" style="font-size:.65rem;">${gbn}</span>`;
    };

    tbodyEl.innerHTML = items.map(e => {
      const plClass = (e["수익률"] || 0) >= 0 ? "val-pos" : "val-neg";
      const actionBtns = `<td class="text-center p-1">
        <button class="btn btn-sm btn-outline-secondary pf-edit-btn" data-code="${e["종목코드"]}" title="편집" style="font-size:.7rem;padding:1px 5px;">✎</button>
        <button class="btn btn-sm btn-outline-danger pf-del-btn" data-code="${e["종목코드"]}" title="삭제" style="font-size:.7rem;padding:1px 5px;">✕</button>
      </td>`;
      const cells = cols.map(c => {
        let cls = c.align === "left" ? "text-start" : "";
        if (c.key === "수익률" || c.key === "수익금액") cls += " " + plClass;
        if (c.key === "종목구분") return `<td>${_gbnBadge(e["종목구분"])}</td>`;
        const rep = (c.key === "종목명") ? reportMap[e["종목코드"]] : null;
        const aiBadge = rep ? ` <span class="badge badge-ai" title="${rep.model} · ${rep.date}">AI</span>` : "";
        return `<td class="${cls.trim()}">${fmt(e[c.key], c.fmt)}${aiBadge}</td>`;
      }).join("");
      return `<tr data-code="${e["종목코드"]}">${actionBtns}${cells}</tr>`;
    }).join("");

    // 헤더 정렬 이벤트
    headerEl.querySelectorAll("th[data-col]").forEach(th => {
      th.addEventListener("click", () => {
        const col = th.dataset.col;
        if (sortCol === col) {
          sortOrder = sortOrder === "desc" ? "asc" : "desc";
        } else {
          sortCol = col; sortOrder = "desc";
        }
        // 클라이언트 정렬
        const sorted = [...items].sort((a, b) => {
          const va = a[sortCol] ?? -Infinity, vb = b[sortCol] ?? -Infinity;
          return sortOrder === "desc" ? (vb > va ? 1 : -1) : (va > vb ? 1 : -1);
        });
        renderPortfolioTable(sorted);
      });
    });

    // 수정 버튼
    tbodyEl.querySelectorAll(".pf-edit-btn").forEach(btn =>
      btn.addEventListener("click", e => { e.stopPropagation(); openPortfolioEdit(btn.dataset.code); })
    );
    // 삭제 버튼
    tbodyEl.querySelectorAll(".pf-del-btn").forEach(btn =>
      btn.addEventListener("click", e => {
        e.stopPropagation();
        if (confirm("이 종목을 포트폴리오에서 삭제하시겠습니까?")) deletePortfolioEntry(btn.dataset.code);
      })
    );
    // 행 클릭 → 상세 모달
    tbodyEl.querySelectorAll("tr[data-code]").forEach(tr =>
      tr.addEventListener("click", e => {
        if (e.target.closest(".pf-edit-btn, .pf-del-btn")) return;
        openDetail(tr.dataset.code);
      })
    );
  }

  function openPortfolioAdd(code, name) {
    document.getElementById("pf-code").value = code || "";
    document.getElementById("pf-code").readOnly = !!code;
    document.getElementById("pf-stock-name").textContent = name || "";
    document.getElementById("pf-qty").value = "";
    document.getElementById("pf-price").value = "";
    document.getElementById("pf-date").value = new Date().toISOString().slice(0, 10);
    document.getElementById("pf-memo").value = "";
    const noteEl = document.getElementById("pf-stock-note");
    if (noteEl) noteEl.innerHTML = "";
    document.getElementById("portfolio-modal-title").textContent = "포트폴리오 추가";
    new bootstrap.Modal(document.getElementById("portfolio-modal")).show();
  }

  function openPortfolioEdit(code) {
    if (!portfolioData) return;
    const entry = portfolioData.items.find(e => e["종목코드"] === code);
    if (!entry) return;
    document.getElementById("pf-code").value = entry["종목코드"];
    document.getElementById("pf-code").readOnly = true;
    document.getElementById("pf-stock-name").textContent = entry["종목명"] || "";
    document.getElementById("pf-qty").value = entry["수량"];
    document.getElementById("pf-price").value = entry["평균매입가"];
    document.getElementById("pf-date").value = entry["매입일"] || "";
    document.getElementById("pf-memo").value = entry["메모"] || "";
    const noteEl = document.getElementById("pf-stock-note");
    if (noteEl) {
      const gbn = entry["종목구분"] || "";
      if (gbn === "ETF") noteEl.innerHTML = '<span class="badge bg-primary me-1">ETF</span><span class="text-muted small">재무지표는 제공되지 않습니다.</span>';
      else if (gbn === "우선주") noteEl.innerHTML = '<span class="badge bg-secondary me-1">우선주</span>';
      else if (gbn === "리츠") noteEl.innerHTML = '<span class="badge bg-success me-1">리츠</span>';
      else noteEl.innerHTML = "";
    }
    document.getElementById("portfolio-modal-title").textContent = "포트폴리오 수정";
    new bootstrap.Modal(document.getElementById("portfolio-modal")).show();
  }

  async function savePortfolioEntry() {
    const code  = (document.getElementById("pf-code").value || "").trim().padStart(6, "0");
    const qty   = parseFloat(document.getElementById("pf-qty").value);
    const price = parseFloat(document.getElementById("pf-price").value);
    const date  = document.getElementById("pf-date").value;
    const memo  = document.getElementById("pf-memo").value.trim();
    if (!code || !qty || qty <= 0 || !price || price <= 0) {
      alert("수량과 매입가를 올바르게 입력하세요."); return;
    }
    const isEdit = document.getElementById("pf-code").readOnly;
    const url    = isEdit ? `/api/portfolio/${code}` : "/api/portfolio";
    const method = isEdit ? "PUT" : "POST";
    try {
      const res = await fetch(url, {
        method, headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ "종목코드": code, "수량": qty, "평균매입가": price, "매입일": date, "메모": memo }),
      });
      if (!res.ok) { const err = await res.json(); alert(err.error || "저장 실패"); return; }
      bootstrap.Modal.getInstance(document.getElementById("portfolio-modal"))?.hide();
      await loadPortfolio();
      if (currentScreen === "portfolio") loadStocks();
    } catch (e) { alert("저장 실패: " + e.message); }
  }

  async function deletePortfolioEntry(code) {
    try {
      const res = await fetch(`/api/portfolio/${code}`, { method: "DELETE" });
      if (!res.ok) { alert("삭제 실패"); return; }
      await loadPortfolio();
      if (currentScreen === "portfolio") loadStocks();
    } catch (e) { alert("삭제 실패: " + e.message); }
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
    setSelectedSectors([]);
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
    const hasFilter = snap.market || (snap.sectors && snap.sectors.length) || snap.search || Object.keys(snap.columnFilters).length;
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

  // 포트폴리오 저장 버튼
  document.getElementById("btn-portfolio-save")?.addEventListener("click", savePortfolioEntry);

  // 포트폴리오 추가 버튼 (세부 모달) - 상세 모달을 먼저 닫고 포트폴리오 모달 열기
  document.getElementById("btn-portfolio-detail")?.addEventListener("click", function () {
    if (currentDetailCode && currentDetailData) {
      const code = currentDetailCode;
      const name = currentDetailData["종목명"];
      const detailModal = bootstrap.Modal.getInstance(document.getElementById("detail-modal"));
      if (detailModal) {
        document.getElementById("detail-modal").addEventListener("hidden.bs.modal", function handler() {
          this.removeEventListener("hidden.bs.modal", handler);
          openPortfolioAdd(code, name);
        });
        detailModal.hide();
      } else {
        openPortfolioAdd(code, name);
      }
    }
  });

  // 포트폴리오 새 종목 추가 버튼 (요약 바)
  document.getElementById("btn-portfolio-add-new")?.addEventListener("click", () => {
    openPortfolioAdd("", "");
  });

  // 예수금 저장 버튼
  document.getElementById("btn-save-cash")?.addEventListener("click", saveCashBalance);

  // 예수금 입력창 엔터키
  document.getElementById("input-cash-balance")?.addEventListener("keydown", (e) => {
    if (e.key === "Enter") saveCashBalance();
  });

  // 포트폴리오 모달: 종목코드 입력 시 종목명/종목구분 자동 조회
  document.getElementById("pf-code")?.addEventListener("blur", async function () {
    const raw = this.value.trim();
    if (raw.length === 0 || raw.length > 6) return;
    const code = raw.padStart(6, "0");
    if (code === "000000") return;
    const noteEl = document.getElementById("pf-stock-note");
    try {
      const res = await fetch(`/api/stock-info/${code}`);
      if (res.ok) {
        const data = await res.json();
        document.getElementById("pf-stock-name").textContent = data["종목명"] || "";
        if (noteEl) {
          const gbn = data["종목구분"] || "";
          if (gbn === "ETF") {
            noteEl.innerHTML = '<span class="badge bg-primary me-1">ETF</span><span class="text-muted small">재무지표(PER/ROE 등)는 제공되지 않습니다.</span>';
          } else if (gbn === "우선주") {
            noteEl.innerHTML = '<span class="badge bg-secondary me-1">우선주</span><span class="text-muted small">재무지표는 보통주 기준으로 제공됩니다.</span>';
          } else if (gbn === "리츠") {
            noteEl.innerHTML = '<span class="badge bg-success me-1">리츠</span><span class="text-muted small">재무지표는 제공되지 않습니다.</span>';
          } else {
            noteEl.innerHTML = "";
          }
        }
      } else {
        document.getElementById("pf-stock-name").textContent = "";
        if (noteEl) noteEl.innerHTML = '<span class="text-danger small">종목을 찾을 수 없습니다. 파이프라인 실행 후 재시도하세요.</span>';
      }
    } catch (e) { /* ignore */ }
  });

  // ─── 포트폴리오 차트 렌더링 ────────────────────────────────────────
  let _pfCharts = [];

  function _renderPortfolioCharts(scores) {
    // 기존 차트 제거
    _pfCharts.forEach(c => { try { c.destroy(); } catch(e) {} });
    _pfCharts = [];

    if (!scores || typeof Chart === "undefined") return;

    // 1. 수익률 바 차트
    const returnCanvas = document.getElementById("pf-return-chart");
    if (returnCanvas && portfolioData && portfolioData.items) {
      const items = portfolioData.items.filter(i => i["수익률"] !== null && i["수익률"] !== undefined);
      if (items.length > 0) {
        const labels = items.map(i => i["종목명"] || i["종목코드"]);
        const values = items.map(i => i["수익률"] || 0);
        const colors = values.map(v => v >= 0 ? "rgba(39,174,96,0.8)" : "rgba(192,57,43,0.8)");
        _pfCharts.push(new Chart(returnCanvas, {
          type: "bar",
          data: {
            labels,
            datasets: [{ label: "수익률(%)", data: values, backgroundColor: colors, borderWidth: 0 }]
          },
          options: {
            indexAxis: "y",
            responsive: true,
            plugins: { legend: { display: false } },
            scales: {
              x: { grid: { color: "#eee" }, ticks: { callback: v => v + "%" } },
              y: { ticks: { font: { size: 11 } } }
            }
          }
        }));
      }
    }

    // 2. 섹터 도넛 차트
    const sectorCanvas = document.getElementById("pf-sector-chart");
    if (sectorCanvas && portfolioData && portfolioData["섹터별"]) {
      const sectors = portfolioData["섹터별"].filter(s => s["비중"] > 0);
      if (sectors.length > 0) {
        const palette = ["#2980b9","#27ae60","#e74c3c","#8e44ad","#f39c12",
                         "#16a085","#d35400","#2c3e50","#7f8c8d","#1abc9c"];
        _pfCharts.push(new Chart(sectorCanvas, {
          type: "doughnut",
          data: {
            labels: sectors.map(s => s["섹터"]),
            datasets: [{
              data: sectors.map(s => s["비중"]),
              backgroundColor: sectors.map((_, i) => palette[i % palette.length]),
              borderWidth: 1
            }]
          },
          options: {
            responsive: true,
            plugins: {
              legend: { position: "right", labels: { font: { size: 11 } } },
              tooltip: { callbacks: {
                label: ctx => ctx.label + ": " + ctx.parsed.toFixed(1) + "%",
                afterLabel: ctx => {
                  const stocks = sectors[ctx.dataIndex]["종목"];
                  return stocks && stocks.length ? stocks.join(", ") : "";
                }
              } }
            }
          }
        }));
      }
    }
  }

  // ─── 포트폴리오 AI 분석 ────────────────────────────────────────────
  let _pfAnalysisInProgress = false;

  async function loadPortfolioAnalysisHistory() {
    const selectEl = document.getElementById("pf-report-history-select");
    if (!selectEl) return;
    try {
      const res = await fetch("/api/portfolio/analysis/history");
      if (!res.ok) return;
      const { history } = await res.json();
      if (!history || history.length <= 1) {
        selectEl.style.display = "none";
        return;
      }
      selectEl.style.display = "";
      selectEl.innerHTML = history.map((h, i) =>
        `<option value="${h.id}">${i === 0 ? "최신 " : ""}${h.generated_date} (${h.model_used || "AI"})</option>`
      ).join("");
    } catch (e) {
      selectEl.style.display = "none";
    }
  }

  async function requestPortfolioAnalysis(forceRegenerate) {
    if (_pfAnalysisInProgress) {
      alert("포트폴리오 AI 분석이 이미 진행 중입니다. 완료 후 다시 시도해주세요.");
      return;
    }
    _pfAnalysisInProgress = true;
    const pfAnalysisBtn = document.getElementById("btn-portfolio-analysis");
    if (pfAnalysisBtn) pfAnalysisBtn.disabled = true;

    const modalEl = document.getElementById("portfolio-report-modal");
    const reportModal = bootstrap.Modal.getOrCreateInstance(modalEl);
    reportModal.show();
    loadPortfolioAnalysisHistory();

    const loadingEl   = document.getElementById("pf-report-loading");
    const contentEl   = document.getElementById("pf-report-content");
    const metaEl      = document.getElementById("pf-report-meta");
    const staleBanner = document.getElementById("pf-report-stale-banner");
    const loadingText = document.getElementById("pf-report-loading-text");

    loadingEl.style.display = "";
    contentEl.innerHTML = "";
    metaEl.textContent  = "";
    staleBanner.style.display = "none";

    try {
      let data;

      if (!forceRegenerate) {
        const getRes = await fetch("/api/portfolio/analysis");
        if (getRes.ok) {
          const cached = await getRes.json();
          if (cached.stale) {
            loadingEl.style.display = "none";
            staleBanner.style.display = "flex";
            contentEl.innerHTML = cached.report_html || "";
            metaEl.textContent = (cached.model || "") + " \u00b7 " + (cached.generated_date || "") + " (\uc774\uc804 \ubd84\uc11d)";
            _renderPortfolioCharts(cached.scores);
            return;
          }
          data = cached;
        }
      }

      if (!data) {
        // 단계별 프로그레스 정의 (누적 %, 경과 초, 메시지)
        const PF_STAGES = [
          { pct: 5,  sec: 2,   msg: "포트폴리오 데이터 준비 중..." },
          { pct: 12, sec: 8,   msg: "종목 재무·퀀트 데이터 수집 중..." },
          { pct: 20, sec: 15,  msg: "AI 모델에 분석 요청 전송 중..." },
          { pct: 30, sec: 30,  msg: "포트폴리오 구성 종합 분석 중..." },
          { pct: 45, sec: 60,  msg: "종목별 매수/보유/매도 판단 분석 중..." },
          { pct: 58, sec: 90,  msg: "섹터 배분 및 상관관계 분석 중..." },
          { pct: 68, sec: 120, msg: "리스크·촉매 요인 도출 중..." },
          { pct: 78, sec: 160, msg: "리밸런싱 실행 계획 작성 중..." },
          { pct: 86, sec: 210, msg: "보고서 구조화 중..." },
          { pct: 93, sec: 280, msg: "최종 검토 및 HTML 렌더링 중..." },
          { pct: 97, sec: 400, msg: "거의 완료됐습니다. 잠시만 기다려주세요..." },
        ];

        const progressBar  = document.getElementById("pf-progress-bar");
        const progressPct  = document.getElementById("pf-progress-pct");
        const progressStage = document.getElementById("pf-progress-stage");

        const startTime = Date.now();
        let stageIdx = 0;

        function updateProgress() {
          const elapsed = (Date.now() - startTime) / 1000;
          // 현재 단계 찾기
          while (stageIdx < PF_STAGES.length - 1 && elapsed >= PF_STAGES[stageIdx + 1].sec) {
            stageIdx++;
          }
          const cur = PF_STAGES[stageIdx];
          const next = PF_STAGES[Math.min(stageIdx + 1, PF_STAGES.length - 1)];
          // 단계 내 보간
          let pct = cur.pct;
          if (stageIdx < PF_STAGES.length - 1) {
            const segElapsed = elapsed - cur.sec;
            const segDur = next.sec - cur.sec;
            pct = cur.pct + (next.pct - cur.pct) * Math.min(segElapsed / segDur, 1);
          }
          pct = Math.min(pct, 97);
          if (progressBar)  progressBar.style.width = pct.toFixed(1) + "%";
          if (progressPct)  progressPct.textContent = Math.round(pct) + "%";
          if (progressStage) progressStage.textContent = cur.msg;
          loadingText.textContent = "포트폴리오 AI 분석 중... (" + Math.round(elapsed) + "초)";
        }

        const progressInterval = setInterval(updateProgress, 500);
        updateProgress();

        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 660000); // 11분 (서버 10분 + 여유)

        try {
          const watchlistCodes = [...getWatchlist()];
          const postRes = await fetch("/api/portfolio/analysis", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ watchlist_codes: watchlistCodes }),
            signal: controller.signal,
          });
          // 완료 시 100%로
          if (progressBar)  { progressBar.style.width = "100%"; progressBar.classList.remove("progress-bar-animated"); }
          if (progressPct)  progressPct.textContent = "100%";
          if (progressStage) progressStage.textContent = "분석 완료!";
          data = await postRes.json();
        } finally {
          clearInterval(progressInterval);
          clearTimeout(timeoutId);
        }
      }

      loadingEl.style.display = "none";
      staleBanner.style.display = "none";

      if (data.error) {
        contentEl.innerHTML = '<div class="alert alert-danger"><strong>\uc624\ub958:</strong> ' + data.error + '</div>';
      } else {
        contentEl.innerHTML = data.report_html || "";
        metaEl.textContent = (data.model || "") + " \u00b7 " + (data.generated_date || "");
        _renderPortfolioCharts(data.scores);
      }
    } catch (e) {
      loadingEl.style.display = "none";
      const msg = e.name === "AbortError"
        ? "\ubd84\uc11d \uc2dc\uac04\uc774 \ucd08\uacfc\ub418\uc5c8\uc2b5\ub2c8\ub2e4. \ub2e4\uc2dc \uc2dc\ub3c4\ud574\uc8fc\uc138\uc694."
        : e.message;
      contentEl.innerHTML = '<div class="alert alert-danger">\uc624\ub958: ' + msg + '</div>';
    } finally {
      _pfAnalysisInProgress = false;
      if (pfAnalysisBtn) pfAnalysisBtn.disabled = false;
      loadPortfolioAnalysisHistory();
    }
  }

  document.getElementById("pf-report-history-select")?.addEventListener("change", async function () {
    const reportId = this.value;
    if (!reportId) return;
    const contentEl = document.getElementById("pf-report-content");
    const metaEl = document.getElementById("pf-report-meta");
    try {
      const res = await fetch(`/api/portfolio/analysis/${reportId}`);
      if (!res.ok) return;
      const data = await res.json();
      contentEl.innerHTML = data.report_html || "";
      metaEl.textContent = (data.model || "") + " · " + (data.generated_date || "") + " (이력)";
      _renderPortfolioCharts(data.scores);
    } catch (e) {
      contentEl.innerHTML = '<div class="alert alert-danger">이력 보고서 로드 실패</div>';
    }
  });

  document.getElementById("btn-portfolio-analysis")?.addEventListener("click", () => {
    if (!portfolioData || !portfolioData.items || !portfolioData.items.length) {
      alert("\ud3ec\ud2b8\ud3f4\ub9ac\uc624\uac00 \ube44\uc5b4 \uc788\uc2b5\ub2c8\ub2e4. \uc885\ubaa9\uc744 \uba3c\uc800 \ucd94\uac00\ud558\uc138\uc694.");
      return;
    }
    requestPortfolioAnalysis(false);
  });

  document.getElementById("btn-pf-regenerate")?.addEventListener("click", () => {
    requestPortfolioAnalysis(true);
  });

  document.getElementById("btn-pf-pdf")?.addEventListener("click", () => {
    document.body.classList.add("printing-portfolio");
    window.print();
    document.body.classList.remove("printing-portfolio");
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
  document.getElementById("btn-analysis-claude").addEventListener("click", function () {
    requestAnalysis(this.dataset.code || currentDetailCode, "claude");
  });

  // 이전 보고서 선택
  document.getElementById("report-history-select").addEventListener("change", function () {
    const historyId = this.value;
    const code = document.getElementById("btn-regenerate").dataset.code;
    if (historyId && code) showHistoryReport(historyId, code);
  });

  // 보고서 재생성 / PDF
  document.getElementById("btn-regenerate").addEventListener("click", function () {
    if (this.dataset.code && this.dataset.mode) {
      requestAnalysis(this.dataset.code, this.dataset.mode, true);
    }
  });
  document.getElementById("btn-pdf").addEventListener("click", () => {
    document.body.classList.add("printing-report");
    window.print();
    document.body.classList.remove("printing-report");
  });

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
      const prevScreen = currentScreen;
      document.querySelectorAll("#screen-tabs .nav-link").forEach(x => x.classList.remove("active"));
      l.classList.add("active");
      currentScreen = l.dataset.screen;
      sortCol       = TAB_DEFAULT_SORT[currentScreen];
      sortOrder     = "desc";
      currentPage   = 1;

      // 포트폴리오 ↔ 일반 탭 전환 시 요약 바/마켓 요약 토글
      if (currentScreen === "portfolio") {
        document.getElementById("portfolio-summary-bar").style.display = "";
      } else {
        document.getElementById("portfolio-summary-bar").style.display = "none";
        if (prevScreen === "portfolio") loadMarketSummary();
      }

      // 포트폴리오 탭은 자체 헤더를 빌드하므로 buildHeader 스킵
      if (currentScreen !== "portfolio") buildHeader();
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

  // ── 섹터 멀티셀렉트 헬퍼 ────────────────────────────────────────────
  function getSelectedSectors() {
    const boxes = document.querySelectorAll("#f-sector-options input[type=checkbox]:checked");
    return Array.from(boxes).map(cb => cb.value);
  }

  function setSelectedSectors(arr) {
    const boxes = document.querySelectorAll("#f-sector-options input[type=checkbox]");
    boxes.forEach(cb => { cb.checked = arr.includes(cb.value); });
    _updateSectorBtn();
  }

  function _updateSectorBtn() {
    const btn = document.getElementById("f-sector-btn");
    const allCb = document.getElementById("f-sector-all");
    if (!btn) return;
    const selected = getSelectedSectors();
    if (!selected.length) {
      btn.textContent = "전체 섹터";
      if (allCb) allCb.checked = true;
    } else if (selected.length === 1) {
      btn.textContent = selected[0];
      if (allCb) allCb.checked = false;
    } else {
      btn.textContent = `${selected.length}개 섹터`;
      if (allCb) allCb.checked = false;
    }
  }

  // 섹터 드롭다운 초기화
  async function loadSectorOptions() {
    const optWrap = document.getElementById("f-sector-options");
    if (!optWrap) return;
    try {
      const res  = await fetch("/api/sectors");
      if (!res.ok) return;
      const data = await res.json();
      optWrap.innerHTML = "";
      data.forEach(({ 섹터: name, count }, idx) => {
        const lbl = document.createElement("label");
        lbl.className = "sector-item" + (idx === 0 ? " sector-divider" : "");
        const cb = document.createElement("input");
        cb.type = "checkbox";
        cb.value = name;
        cb.addEventListener("change", () => {
          document.getElementById("f-sector-all").checked = false;
          _updateSectorBtn();
          currentPage = 1; loadStocks();
        });
        lbl.appendChild(cb);
        lbl.appendChild(document.createTextNode(`${name} (${count})`));
        optWrap.appendChild(lbl);
      });
    } catch (e) { /* 섹터 API 실패 시 무시 */ }
  }

  // 전체선택 체크박스
  const sectorAllCb = document.getElementById("f-sector-all");
  if (sectorAllCb) {
    sectorAllCb.addEventListener("change", () => {
      if (sectorAllCb.checked) {
        setSelectedSectors([]);
        currentPage = 1; loadStocks();
      }
    });
  }

  // 버튼 클릭 → 드롭다운 토글
  const sectorBtn = document.getElementById("f-sector-btn");
  const sectorDropdown = document.getElementById("f-sector-dropdown");
  if (sectorBtn && sectorDropdown) {
    sectorBtn.addEventListener("click", (e) => {
      e.stopPropagation();
      const isOpen = sectorDropdown.style.display !== "none";
      sectorDropdown.style.display = isOpen ? "none" : "block";
    });
    // 외부 클릭 시 닫기
    document.addEventListener("click", (e) => {
      if (!document.getElementById("sector-multiselect-wrap")?.contains(e.target)) {
        sectorDropdown.style.display = "none";
      }
    });
  }

  // ─── 데이터 품질 정보 ─────────────────────────────────────────────────
  async function loadDataInfo() {
    try {
      const res  = await fetch("/api/info");
      if (!res.ok) return;
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
  loadPortfolio();
  updateWatchlistCount();
  // reportMap을 먼저 로드한 뒤 테이블 렌더 → AI 배지 누락 방지
  loadReportMap().then(() => loadStocks());

  const initDesc = STRATEGY_DESCRIPTIONS.all;
  document.getElementById("strategy-desc").innerHTML =
    `<strong>${initDesc.title}</strong>&nbsp;&nbsp;<small class="text-muted">${initDesc.criteria}</small>`;

})();
