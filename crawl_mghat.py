# -*- coding: utf-8 -*-
# crawl_mghat.py
# mghat.com(무궁화_신탁) 공매정보 → Google Sheets append
# 이 코드는 crawl_kyobo.py 구조를 그대로 가져와 mghat 사이트에 맞게 범용적으로 수정한 버전입니다.
# 원본: crawl_kyobo.py. 참조: :contentReference[oaicite:1]{index=1}

import os, re, time, json, logging
from datetime import datetime
from typing import Dict, List, Tuple
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter, Retry
from bs4 import BeautifulSoup

import gspread
from google.oauth2.service_account import Credentials

# 주소/건물명/도시명 파서가 동일한 위치에 있으면 그대로 import
try:
  from address_only_regex import (
    extract_address,
    extract_province_sgg,
    extract_building_name,
    extract_sale_content,
  )
except Exception:
  # 만약 해당 모듈이 없으면 빈 동작 파서로 대체(사용자가 제공한 모듈이 필요)
  def extract_address(title): return ""
  def extract_province_sgg(title, use_address_fallback=True): return ""
  def extract_building_name(title): return ""
  def extract_sale_content(title): return ""

# ────────── Google Sheets 설정 ──────────
SPREADSHEET_ID   = "1BEoi3Q6pOoUBUcEDgdy1YF03Ehc1hY02KfKz31GMt7E"  # 필요 시 변경
WORKSHEET_NAME   = "무궁화_신탁"  # 시트명 (원하면 변경)
SERVICE_KEY_FILE = "service_account.json"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

HEADER = [
  "번호","신탁사명","게시글 제목","게시일",
  "주소 (지번)","도시명","건물명","매각내용","용도",
  "중복여부","원문 링크(URL)"
]

# ────────── 크롤링 설정 (mghat 사이트용) ──────────
BASE = "http://mghat.com"
LIST_PATH = "/auction/disposal/list.do"
TRUST_NAME = "무궁화 신탁"

DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")

def _find_first_date_in_row(row) -> str:
  txt_cells = row.select("td.txt-gray")
  for td in txt_cells:
    t = td.get_text(" ", strip=True)
    m = DATE_RE.search(t)
    if m:
      return m.group(0)
  mview = row.select_one(".m-date-view span")
  if mview:
    m = DATE_RE.search(mview.get_text(" ", strip=True))
    if m:
      return m.group(0)
  return ""


DELAY_SEC = 1.0

# 기본: 현재→과거 (페이지 예시로 384 사용)
START_PAGE = 384
END_PAGE   = 1
STEP       = -1

RESUME = True
STATE_FILE = "output/.mghat_state.json"

# 컬럼 후보
ADDRESS_COL_CANDIDATES  = ["address", "주소 (지번)", "주소", "Address"]
DUP_COL_CANDIDATES      = ["duplicate", "중복여부", "중복", "Duplicate"]
URL_COL_CANDIDATES      = ["url", "원문 링크(URL)", "링크", "URL"]

def _find_col_index(header: list, candidates: list) -> int:
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

# ────────── 로깅/세션 ──────────
def setup_logging():
  os.makedirs("logs", exist_ok=True)
  ts = datetime.now().strftime("%Y%m%d_%H%M%S")
  logfile = f"logs/mghat_sheets_{ts}.log"
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

# ────────── 시트 유틸 (원본 로직 유지) ──────────
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

  dup_col = [header[dup_idx]] + [""] * (len(values) - 1)
  for addr, rows in groups.items():
    if not addr:
      continue
    if len(rows) >= 2:
      for k, r in enumerate(rows, start=1):
        dup_col[r - 1] = f"중복{k}"

  col_letter = _col_letter(dup_idx + 1)
  ws.update(f"{col_letter}1:{col_letter}{len(dup_col)}",
            [[v] for v in dup_col],
            value_input_option="USER_ENTERED")

def _load_existing_from_sheet(ws) -> Tuple[set, Dict[str, int]]:
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
    # 상태 저장 실패는 치명적이지 않음
    pass

# ────────── 보조 파서 ──────────
def _normalize_date(s: str) -> str:
  s = (s or "").strip()
  m = DATE_RE.search(s)
  if not m:
    return ""
  return m.group(0).replace(".", "-")

def _extract_row_number_from_text(t: str) -> str:
  m = re.search(r"\d+", (t or ""))
  return m.group(0) if m else ""

def _make_purpose(title: str) -> str:
  return "오피스텔" if "오피스텔" in (title or "") else ""

# ────────── 리스트 파서 (mghat 범용 버전) ──────────
def parse_list_page(html: str) -> List[Dict[str, str]]:
  soup = BeautifulSoup(html, "lxml")
  items: List[Dict[str, str]] = []

  # 여러 후보 셀렉터 시도: 실제 사이트 구조에 맞게 변형 가능
  CANDIDATE_SELECTORS = [
    ".board-lst table tbody tr",     # (대부분 테이블형 리스트)
    ".board_list li",                # ul/li 형
    ".boardList .body a.row",        # 교보 스타일의 후보
    ".list-area .list-wrap tr",      # 다른 변형
    ".boardList tr",                 # 일반 테이블
    "table.board tr",                # 또다른 후보
  ]

  list_nodes = []
  for sel in CANDIDATE_SELECTORS:
    nodes = soup.select(sel)
    if nodes:
      list_nodes = nodes
      logging.debug("parse_list_page: using selector %s (found=%d)", sel, len(nodes))
      break

  if not list_nodes:
    # fallback: 링크가 많은 영역에서 a 태그들을 긁어보기
    container = soup.select_one(".board-lst") or soup.select_one(".board")
    if container:
      list_nodes = container.select("a")
    else:
      list_nodes = soup.select("a")

  # 항목 추출: 각 노드에서 링크(상세), 제목, 날짜, 번호(가능하면) 탐색
  for node in list_nodes:
    # node가 <tr>라면 내부에서 a, td 등을 찾기
    a = node.select_one("a") if hasattr(node, "select_one") else None
    link = None
    title = ""
    post_date = ""
    no = ""

    # 후보 1: 노드 자체가 <a>인 경우
    if node.name == "a":
      a = node

    if a:
      href = a.get("href", "").strip()
      if href and not href.startswith("javascript"):
        link = urljoin(BASE, href)
      title = a.get_text(" ", strip=True)

    # 날짜 탐색: 강인한 추출 (td, span, .date 등)
    post_date = _find_first_date_in_row(node)

    # 번호 추출 시도 (첫 컬럼 등)
    num_el = node.select_one(".num") or node.select_one(".number") or node.select_one("td:first-child")
    if num_el:
      no = _extract_row_number_from_text(num_el.get_text(" ", strip=True))
    else:
      no = _extract_row_number_from_text(title)

    # 상세 링크가 자바스크립트로 구성된 경우(예: view?idx=123) 처리
    if not link:
      # 시도: onclick에 url 파라미터가 들어있다면 추출
      onclick = node.get("onclick") if hasattr(node, "get") else None
      if onclick:
        m = re.search(r"location\.href=['\"]([^'\"]+)['\"]", onclick)
        if m:
          link = urljoin(BASE, m.group(1))
        else:
          # 또 다른 패턴: view.do?seq=123 형태
          m2 = re.search(r"(/[^)'\"]+view[^)'\"\s]+)", onclick)
          if m2:
            link = urljoin(BASE, m2.group(1))

    # 보수적으로 링크 없으면 스킵
    if not link:
      continue

    # 제목이 비어있으면 링크 텍스트로 대체
    if not title:
      title = a.get("title", "") or link

    # 주소/도시/건물 추출 (제목 기반)
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
      "url": link,
      "address": address,
      "city": city,
      "building": building,
      "sale_content": sale_content,
      "purpose": purpose,
    })

  return items

# ────────── 메인 ──────────
def main():
  setup_logging()
  ws = _open_sheet()

  # 1) 기존 시트의 중복열 정규화
  _normalize_duplicate_numbering(ws)

  # 2) 시트/상태 기반으로 seen_urls / address_counts 복원
  seen_urls_sheet, address_counts = _load_existing_from_sheet(ws)
  state = _load_state()
  seen_urls = set(seen_urls_sheet) | set(state.get("seen_urls") or [])
  last_done = state.get("last_done_page")

  # 3) 재개 지점 계산
  start_page = START_PAGE
  if RESUME and last_done is not None:
    start_page = last_done + (1 if STEP > 0 else -1)

  logging.info("크롤 범위: %s → %s (step=%s)", start_page, END_PAGE, STEP)
  session = make_session()

  try:
    rng_end_inclusive = END_PAGE + (1 if STEP > 0 else -1)
    for page in range(start_page, rng_end_inclusive, STEP):
      params = {"page": page, "searchCount": 0, "field": "", "keyword": "", "type": ""}
      url = urljoin(BASE, LIST_PATH)

      try:
        r = session.get(url, params=params, timeout=20)
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

      # 사이트에 따라 리스트 항목 순서가 과거->현재일 수 있으므로 원본 로직과 동일하게 역전 처리
      items = list(reversed(items))

      # 중복 URL 제외 + 임시 duplicate('중복2..N') + 배치 append
      batch: List[Dict[str, str]] = []
      for it in items:
        url = it["url"]
        if url in seen_urls:
          continue

        addr = (it.get("address") or "").strip()
        duplicate = ""
        if addr:
          prev = address_counts.get(addr, 0)
          if prev >= 1:
            duplicate = f"중복{prev + 1}"  # 임시(첫 건은 공란)
          address_counts[addr] = prev + 1

        it["duplicate"] = duplicate
        batch.append(it)
        seen_urls.add(url)

      if batch:
        _append_rows(ws, batch)
        _normalize_duplicate_numbering(ws)
        logging.info("[LIST] page=%s appended rows=%d", page, len(batch))

      # 상태 저장
      state["last_done_page"] = page
      state["seen_urls"] = list(seen_urls)
      _save_state(state)

      time.sleep(DELAY_SEC)

  finally:
    logging.info("수집 종료")

if __name__ == "__main__":
  main()
