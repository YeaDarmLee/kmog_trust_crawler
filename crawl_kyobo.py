# crawl_kyobotrust_to_sheets.py
# -*- coding: utf-8 -*-
# 교보자산신탁 공매정보(BBS_0005) → Google Sheets append
# - 리스트 구조: .boardList (a.row 내부에 number/narw/link/narw)
# - 주소 기준 중복은 "2건 이상일 때만" 부여(= 1건이면 빈칸 유지)
#   · append 시 두 번째부터 임시로 '중복2..N' 부여
#   · append 직후 정규화하여 첫 건을 '중복1'로 보정(2건 이상 그룹만)
# - resume: output/.kyobo_state.json (마지막 페이지, seen_urls)
# - address_only_regex.py(동일 디렉토리) 사용

import os, re, time, json, logging
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter, Retry
from bs4 import BeautifulSoup

import gspread
from google.oauth2.service_account import Credentials

from address_only_regex import (
  extract_address,
  extract_province_sgg,
  extract_building_name,
  extract_sale_content,
)

# ────────── Google Sheets 설정 ──────────
SPREADSHEET_ID   = "1BEoi3Q6pOoUBUcEDgdy1YF03Ehc1hY02KfKz31GMt7E"  # 동일 스프레드시트 사용
WORKSHEET_NAME   = "교보자산_신탁"
SERVICE_KEY_FILE = "service_account.json"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# 헤더/append 순서 일치(무궁화 포맷과 동일)
HEADER = [
  "번호",
  "신탁사명", "게시글 제목", "게시일",
  "주소 (지번)", "도시명", "건물명", "매각내용",
  "용도", "중복여부",  "원문 링크(URL)",
]

# ────────── 크롤링 설정 ──────────
BASE = "https://www.kyobotrust.co.kr"
LIST_PATH = "/front/bbsList.do"
BBS_ID = "BBS_0005"
TRUST_NAME = "교보자산신탁"

DATE_RE = re.compile(r"\d{4}[.\-]\d{2}[.\-]\d{2}")
NTT_RE  = re.compile(r"fnViewArticle\('(\d+)'\s*,\s*'BBS_0005'\)")

DELAY_SEC = 1.0

# 페이지 범위(검수 시 조절)
START_PAGE = 1
END_PAGE   = 587
STEP       = 1

RESUME = True
STATE_FILE = "output/.kyobo_state.json"

# ────────── 로깅/세션 ──────────
def setup_logging():
  os.makedirs("logs", exist_ok=True)
  ts = datetime.now().strftime("%Y%m%d_%H%M%S")
  logfile = f"logs/kyobo_sheets_{ts}.log"
  logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(logfile, encoding="utf-8"), logging.StreamHandler()],
  )
  logging.info("로그 시작: %s", logfile)

def make_session() -> requests.Session:
  s = requests.Session()
  s.headers.update({
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/127.0.0.0 Safari/537.36"),
    "Accept-Language": "ko,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Connection": "keep-alive",
    "Referer": BASE,
  })
  retries = Retry(
    total=3,
    backoff_factor=0.6,
    status_forcelist=[500, 502, 503, 504],
    allowed_methods=["HEAD", "GET", "OPTIONS"],
    raise_on_status=False,
  )
  adapter = HTTPAdapter(max_retries=retries)
  s.mount("http://", adapter)
  s.mount("https://", adapter)
  return s

# ────────── 헤더/컬럼 유연 매칭 유틸 ──────────
ADDRESS_COL_CANDIDATES  = ["address", "주소 (지번)", "주소", "Address"]
DUP_COL_CANDIDATES      = ["duplicate", "중복여부", "중복", "Duplicate"]
URL_COL_CANDIDATES      = ["url", "원문 링크(URL)", "링크", "URL"]

def _find_col_index(header: list, candidates: list) -> int:
  """header 리스트에서 candidates 중 먼저 매칭되는 컬럼의 인덱스(0-based) 반환. 없으면 -1"""
  name_to_idx = {name: i for i, name in enumerate(header)}
  for c in candidates:
    if c in name_to_idx:
      return name_to_idx[c]
  return -1

def _col_letter(n: int) -> str:
  s = ""
  while n:
    n, r = divmod(n - 1, 26)
    s = chr(r + 65) + s
  return s

# ────────── 시트 유틸 ──────────
def _open_sheet():
  creds = Credentials.from_service_account_file(SERVICE_KEY_FILE, scopes=SCOPES)
  logging.info("Using service account: %s", creds.service_account_email)
  gc = gspread.authorize(creds)
  sh = gc.open_by_key(SPREADSHEET_ID)
  try:
    ws = sh.worksheet(WORKSHEET_NAME)
  except gspread.WorksheetNotFound:
    ws = sh.add_worksheet(title=WORKSHEET_NAME, rows="1000", cols="20")
  existing = ws.get_all_values()
  if not existing:
    ws.append_row(HEADER)
  else:
    if existing[0] != HEADER:
      logging.warning("시트 헤더가 코드 정의와 다릅니다. (유연 매칭으로 진행)")
  return ws

def _normalize_duplicate_numbering(ws):
  """
  시트 전체를 훑어서,
  - 같은 주소가 2건 이상인 그룹만 '중복1..N'로 채움
  - 1건뿐인 주소는 duplicate 공란 유지
  """
  values = ws.get_all_values()
  if not values:
    return
  header = values[0]

  addr_idx = _find_col_index(header, ADDRESS_COL_CANDIDATES)
  dup_idx  = _find_col_index(header, DUP_COL_CANDIDATES)
  if addr_idx < 0 or dup_idx < 0:
    return

  groups: Dict[str, List[int]] = {}
  for r, row in enumerate(values[1:], start=2):
    addr = (row[addr_idx].strip() if addr_idx < len(row) else "")
    groups.setdefault(addr, []).append(r)

  # 업데이트할 중복열 구성(헤더행 + 데이터행 수 유지)
  dup_col = [header[dup_idx]] + [""] * (len(values) - 1)
  for addr, rows in groups.items():
    if not addr:
      continue
    if len(rows) >= 2:
      for k, r in enumerate(rows, start=1):
        dup_col[r - 1] = f"중복{k}"
    # 1건뿐이면 공란 유지

  col_letter = _col_letter(dup_idx + 1)
  ws.update(f"{col_letter}1:{col_letter}{len(dup_col)}",
            [[v] for v in dup_col],
            value_input_option="USER_ENTERED")

def _load_existing_from_sheet(ws) -> Tuple[set, Dict[str, int]]:
  """
  - seen_urls: 이미 저장된 URL 집합
  - address_counts: 주소별 현재 누계(정규화 이후 결과 기준)
  """
  values = ws.get_all_values()
  if not values:
    return set(), {}
  header = values[0]

  url_idx  = _find_col_index(header, URL_COL_CANDIDATES)
  addr_idx = _find_col_index(header, ADDRESS_COL_CANDIDATES)

  seen_urls = set()
  address_counts: Dict[str, int] = {}
  for row in values[1:]:
    url = (row[url_idx].strip() if url_idx >= 0 and url_idx < len(row) else "")
    addr = (row[addr_idx].strip() if addr_idx >= 0 and addr_idx < len(row) else "")
    if url:
      seen_urls.add(url)
    if addr:
      address_counts[addr] = address_counts.get(addr, 0) + 1
  return seen_urls, address_counts

def _append_rows(ws, rows: List[Dict[str, str]]):
  if not rows:
    return
  # 기본은 HEADER 순서로 append (시트 헤더가 달라도 컬럼 이름 유연 매칭으로 동작)
  values = []
  for r in rows:
    values.append([
      r.get("no", ""),
      r.get("trust_name", ""),
      r.get("title", ""),
      r.get("post_date", ""),
      r.get("address", ""),
      r.get("city", ""),
      r.get("building", ""),
      r.get("sale_content", ""),
      r.get("purpose", ""),
      r.get("duplicate", ""),
      r.get("url", ""),
    ])
  ws.append_rows(values, value_input_option="USER_ENTERED")

# ────────── 상태 파일 ──────────
def _load_state() -> Dict:
  try:
    if os.path.exists(STATE_FILE):
      with open(STATE_FILE, "r", encoding="utf-8") as f:
        return json.load(f)
  except Exception:
    pass
  return {"last_done_page": None, "seen_urls": []}

def _save_state(state: Dict):
  try:
    os.makedirs("output", exist_ok=True)
    with open(STATE_FILE, "w", encoding="utf-8") as f:
      json.dump(state, f, ensure_ascii=False)
  except Exception:
    pass

# ────────── 보조 파서 ──────────
def _normalize_date(s: str) -> str:
  s = (s or "").strip()
  m = DATE_RE.search(s)
  if not m:
    return ""
  return m.group(0).replace(".", "-")

def _detail_url_from_onclick(onclick: str) -> str:
  m = NTT_RE.search(onclick or "")
  if not m:
    return ""
  ntt_id = m.group(1)
  return f"{BASE}/front/viewAritcle.do?bbsId={BBS_ID}&nttId={ntt_id}"

def _extract_row_number_from_text(t: str) -> str:
  m = re.search(r"\d+", (t or ""))
  return m.group(0) if m else ""

def _make_purpose(title: str) -> str:
  return "오피스텔" if "오피스텔" in (title or "") else ""

# ────────── 리스트 파서(.boardList 전용) ──────────
def parse_list_page(html: str) -> List[Dict[str, str]]:
  """
  <div class="boardList">
    <div class="body">
      <a class="row" onclick="fnViewArticle('1268','BBS_0005');">
        <div class="number">9</div>
        <div class="narw">종료</div>
        <div class="link">제목</div>
        <div class="narw">2006.10.31</div>
      </a>
    </div>
  </div>
  """
  soup = BeautifulSoup(html, "lxml")
  items: List[Dict[str, str]] = []

  for a in soup.select(".boardList .body a.row"):
    onclick = a.get("onclick", "")
    detail_url = _detail_url_from_onclick(onclick)
    if not detail_url:
      continue

    no_el = a.select_one(".number")
    link_el = a.select_one(".link")
    narw_els = a.select(".narw")

    no = _extract_row_number_from_text(no_el.get_text(strip=True) if no_el else "")
    title = link_el.get_text(" ", strip=True) if link_el else ""
    # narw 2개: [진행상황, 등록일]
    post_date = _normalize_date(narw_els[-1].get_text(strip=True) if narw_els else "")

    address = extract_address(title) or ""
    city = extract_province_sgg(title, use_address_fallback=True) or ""
    building = extract_building_name(title) or ""
    sale_content = extract_sale_content(title) or ""
    purpose = _make_purpose(title)

    items.append({
      "no": no,
      "trust_name": TRUST_NAME,
      "title": title,
      "post_date": post_date,
      "url": detail_url,
      "address": address,
      "city": city,
      "building": building,
      "sale_content": sale_content,
      "purpose": purpose,
      # duplicate은 append 직전에 주소별 카운트로 채움
    })

  return items

# ────────── 크롤링 루프 ──────────
def crawl_all(ws, start_page: int, end_page: int, step: int, seen_urls: set, address_counts: Dict[str, int]):
  sess = make_session()
  state = _load_state()
  last_done = state.get("last_done_page")
  if RESUME and last_done is not None:
    start_page = last_done + (1 if step > 0 else -1)

  logging.info("크롤 범위: %s → %s (step=%s)", start_page, end_page, step)

  for page in range(start_page, end_page + (1 if step > 0 else -1), step):
    params = {"bbsId": BBS_ID, "pageIndex": page}
    url = urljoin(BASE, LIST_PATH)
    try:
      r = sess.get(url, params=params, timeout=15)
      if r.status_code != 200:
        logging.warning("[LIST] page=%s status=%s", page, r.status_code)
        time.sleep(DELAY_SEC)
        continue
      if not r.encoding or r.encoding.lower() in ("iso-8859-1", "ascii"):
        r.encoding = r.apparent_encoding or "utf-8"
      html = r.text
    except requests.RequestException as e:
      logging.error("[LIST] page=%s error=%s", page, e)
      time.sleep(DELAY_SEC)
      continue

    items = parse_list_page(html)

    # 중복 URL 제외 + duplicate(번호) 표기 + 배치 append
    batch: List[Dict[str, str]] = []
    for it in items:
      url = it["url"]
      if url in seen_urls:
        continue
      addr = (it.get("address") or "").strip()
      duplicate = ""
      if addr:
        prev = address_counts.get(addr, 0)
        # 첫 등장(0) → 공란 유지, 두 번째부터 '중복2..N' 임시 부여
        if prev >= 1:
          duplicate = f"중복{prev + 1}"
        address_counts[addr] = prev + 1
      it["duplicate"] = duplicate
      batch.append(it)
      seen_urls.add(url)

    if batch:
      _append_rows(ws, batch)
      logging.info("[LIST] page=%s appended rows=%d", page, len(batch))
      # 새로 들어온 행까지 포함해 '2건 이상' 그룹에만 '중복1..N' 재부여
      _normalize_duplicate_numbering(ws)

    # 상태 저장
    state["last_done_page"] = page
    state["seen_urls"] = list(seen_urls)
    _save_state(state)

    time.sleep(DELAY_SEC)

# ────────── 메인 ──────────
def main():
  setup_logging()
  ws = _open_sheet()

  # 1) 기존 시트의 duplicate 열 정규화(2건 이상만 '중복1..N', 1건은 공란)
  _normalize_duplicate_numbering(ws)

  # 2) 정규화된 결과를 기반으로 현재까지의 seen_urls / address_counts 로드
  seen_urls, address_counts = _load_existing_from_sheet(ws)

  # 3) 수집 실행
  crawl_all(
    ws=ws,
    start_page=START_PAGE,
    end_page=END_PAGE,
    step=STEP,
    seen_urls=set(seen_urls),
    address_counts=address_counts,
  )
  logging.info("수집 종료")

if __name__ == "__main__":
  main()
