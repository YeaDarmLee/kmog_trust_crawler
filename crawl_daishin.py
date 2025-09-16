# -*- coding: utf-8 -*-
# crawl_daishin.py
# 대신자산신탁 공매/매각 게시판 → Google Sheets append
# - 교보 로직과 동일한 상태관리/중복 규칙 유지
# - 페이지 내 item 역순 처리로 과거→현재 업로드 정렬 보장(STEP=1일 때)
# - 재시작 시 상태 파일/시트로 seen_urls, address_counts 복원
#
# 참고: 교보 버전 로직 구조를 준수함 (시트 열 구성/중복 정규화 방식 동일)

import os, re, time, json, logging
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from urllib.parse import urljoin, urlparse, urlunparse, parse_qsl, urlencode

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
SPREADSHEET_ID   = "1BEoi3Q6pOoUBUcEDgdy1YF03Ehc1hY02KfKz31GMt7E"  # 동일 스프레드시트
WORKSHEET_NAME   = "대신자산_신탁"  # 워크시트 명만 변경
SERVICE_KEY_FILE = "service_account.json"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

HEADER = [
  "번호","신탁사명","게시글 제목","게시일",
  "주소 (지번)","도시명","건물명","매각내용","용도",
  "중복여부","원문 링크(URL)"
]

# ────────── 크롤링 설정 ──────────
BASE = "https://trust.daishin.com"
# 목록 기본 질의(q=keyword_type:all). page 파라미터만 바꾸면 됨.
LIST_BASE_URL = "https://trust.daishin.com/thing_1/?q=YToxOntzOjEyOiJrZXl3b3JkX3R5cGUiO3M6MzoiYWxsIjt9"

TRUST_NAME = "대신자산신탁"

DELAY_SEC = 1.0

# 기본: 현재→과거(예: START_PAGE=1, END_PAGE=400, STEP=1) 또는 과거→현재(STEP=-1)
# 진행 방향은 사이트 상황에 맞게 설정
START_PAGE = 36
END_PAGE   = 1
STEP       = -1

RESUME = True
STATE_FILE = "output/.daishin_state.json"

# ────────── 헤더/컬럼 유틸 ──────────
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
  logfile = f"logs/daishin_sheets_{ts}.log"
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
  같은 주소가 2건 이상인 그룹만 '중복1..N'로 채우고,
  1건뿐인 주소는 공란 유지.
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
DATE_RE = re.compile(r"\d{4}[.\-]\d{2}[.\-]\d{2}")

def _normalize_date(s: str) -> str:
  s = (s or "").strip()
  m = DATE_RE.search(s)
  if not m:
    return ""
  return m.group(0).replace(".", "-")

def _with_page(url: str, page: int) -> str:
  """
  LIST_BASE_URL에 page 파라미터를 세팅해 최종 목록 URL을 만든다.
  """
  u = urlparse(url)
  qs = dict(parse_qsl(u.query, keep_blank_values=True))
  qs["page"] = str(page)
  new_q = urlencode(qs, doseq=True)
  return urlunparse(u._replace(query=new_q))

def _make_purpose(title: str) -> str:
  return "오피스텔" if "오피스텔" in (title or "") else ""

# ────────── 리스트 파서 (대신자산신탁 전용) ──────────
def parse_list_page(html: str) -> List[Dict[str, str]]:
  """
  구조 예시:
  <div class="li_board">
    <ul class="li_body holder">
      <li class="count">355</li>
      <li class="category hidden-xs">...</li>
      <li class="tit ">
        <a class="list_text_title _fade_link" href="/thing_1/?...&bmode=view&idx=167711283&t=board">
          <span>제목</span>
        </a>
      </li>
      <li class="time" title="2025-09-12 15:38">2025-09-12</li>
      <li class="read">14</li>
    </ul>
    ...
  </div>
  """
  soup = BeautifulSoup(html, "lxml")
  items: List[Dict[str, str]] = []

  for ul in soup.select("div.li_board ul.li_body.holder"):
    no_el = ul.select_one("li.count")
    title_link = ul.select_one("li.tit a.list_text_title")
    date_el = ul.select_one("li.time")

    # 번호
    no = ""
    if no_el:
      m = re.search(r"\d+", no_el.get_text(strip=True))
      no = m.group(0) if m else ""

    # 제목/URL
    title = ""
    url = ""
    if title_link:
      # 텍스트는 내부 <span>에 있음
      title = title_link.get_text(" ", strip=True)
      href = title_link.get("href", "")
      if href:
        url = urljoin(BASE, href)

    # 게시일 (보이는 yyyy-mm-dd 혹은 title 속성의 yyyy-mm-dd HH:MM)
    post_date = ""
    if date_el:
      raw = (date_el.get("title") or date_el.get_text(strip=True) or "").strip()
      post_date = _normalize_date(raw)

    # 주소/도시/건물/매각내용/용도
    address = extract_address(title) or ""
    city = extract_province_sgg(title, use_address_fallback=True) or ""
    building = extract_building_name(title) or ""
    sale_content = extract_sale_content(title) or ""
    purpose = _make_purpose(title)

    if url and title:
      items.append({
        "no": no,
        "trust_name": TRUST_NAME,
        "title": title,
        "post_date": post_date,
        "url": url,
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

  # 1) 기존 시트의 중복열 정규화(2건 이상만 '중복1..N', 1건은 공란)
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
      list_url = _with_page(LIST_BASE_URL, page)

      try:
        r = session.get(list_url, timeout=15)
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

      # 페이지 내 역순 처리 (교보 로직과 동일)
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
