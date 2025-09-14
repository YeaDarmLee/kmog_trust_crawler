# -*- coding: utf-8 -*-
# crawl_mghat_to_sheets.py
# 무궁화신탁 매각 게시판 → Google Sheets append 수집
# - no(번호), address, city, building, sale_content (address_only_regex.py 사용)
# - purpose: title에 '오피스텔' 포함 시 '오피스텔'
# - duplicate: 동일 address가 시트/이번 배치에 이미 있으면 '중복'
# - resume: output/.mghat_state.json 로 마지막 수집 페이지와 seen_urls 관리

import os, re, time, json, logging
from datetime import datetime
from typing import Dict, List, Tuple, Optional

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
SPREADSHEET_ID   = "1BEoi3Q6pOoUBUcEDgdy1YF03Ehc1hY02KfKz31GMt7E"   # /d/<ID>/edit
WORKSHEET_NAME   = "무궁화_신탁"
SERVICE_KEY_FILE = "service_account.json"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

HEADER = [
  "no",        # ← 번호
  "trust_name", "title", "post_date", "url",
  "address", "city", "building", "sale_content",
  "purpose", "duplicate",
]

# ────────── 크롤링 설정 ──────────
BASE_CANDIDATES = [
  "http://mghat.com",
  "http://www.mghat.com",
  "https://mghat.com",
  "https://www.mghat.com",
]
LIST_PATH = "/auction/disposal/list.do"
DETAIL_HREF_RE = re.compile(r"^/auction/disposal/\d+/show\.do(\?.*)?$")
DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")

START_PAGE = 249
END_PAGE   = 1
STEP     = -1
DELAY_SEC  = 1.0

RESUME = True
STATE_FILE = "output/.mghat_state.json"

# ────────── 공통 유틸 ──────────
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

def _full_url(base: str, href: str) -> str:
  return href if href.startswith("http") else base.rstrip("/") + href

def _try_fetch(session: requests.Session, base: str, page: int) -> Optional[str]:
  params = {"searchCount": "0", "field": "", "keyword": "", "type": "", "page": str(page)}
  url = base.rstrip("/") + LIST_PATH
  try:
    r = session.get(url, params=params, timeout=15)
    if r.status_code != 200:
      logging.warning("page=%s base=%s status=%s", page, base, r.status_code)
      return None
    if not r.encoding or r.encoding.lower() in ("iso-8859-1", "ascii"):
      r.encoding = r.apparent_encoding or "utf-8"
    return r.text
  except requests.RequestException as e:
    logging.error("요청 오류(base=%s, page=%s): %s", base, page, e)
    return None

def fetch_list_page(session: requests.Session, page: int) -> Tuple[Optional[str], Optional[str]]:
  for base in BASE_CANDIDATES:
    html = _try_fetch(session, base, page)
    if html:
      return html, base
  return None, None

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

# ────────── 번호/파생 필드 ──────────
def _extract_row_number(row) -> str:
  """
  리스트 행의 '번호' 텍스트를 최대한 견고하게 추출.
  사이트 마크업 변화에 대비해 여러 후보 셀을 시도.
  """
  # 1) 흔한 클래스 패턴 우선 시도
  for sel in ["td.no", "td.num", "td.number", "th.no", "th.num", "th.number", "td:nth-of-type(1)", "th:nth-of-type(1)"]:
    el = row.select_one(sel)
    if el:
      t = el.get_text(strip=True)
      if re.fullmatch(r"\d+", t):
        return t
  # 2) 모든 셀 중 첫 셀의 숫자
  cells = row.find_all(["td", "th"])
  if cells:
    t0 = cells[0].get_text(strip=True)
    m = re.search(r"\d+", t0)
    if m:
      return m.group(0)
  return ""

def _make_purpose(title: str) -> str:
  return "오피스텔" if "오피스텔" in (title or "") else ""

def _enrich_row(it: Dict[str, str]) -> Dict[str, str]:
  title = it.get("title", "") or ""
  try:
    it["address"] = extract_address(title) or ""
  except Exception:
    it["address"] = ""
  try:
    it["city"] = extract_province_sgg(title, use_address_fallback=True) or ""
  except Exception:
    it["city"] = ""
  try:
    it["building"] = extract_building_name(title) or ""
  except Exception:
    it["building"] = ""
  try:
    it["sale_content"] = extract_sale_content(title) or ""
  except Exception:
    it["sale_content"] = ""
  it["purpose"] = _make_purpose(title)
  return it

def parse_list_items(html: str, base: str) -> List[Dict[str, str]]:
  soup = BeautifulSoup(html, "lxml")
  tbody = soup.select_one(".board-lst table tbody")
  items: List[Dict[str, str]] = []

  rows = tbody.select("tr") if tbody else []
  if not rows:
    # 구조 변경 대비: row 문맥이 없으므로 no는 빈값 처리
    links = soup.find_all("a", href=DETAIL_HREF_RE)
    for a in links:
      row = a.find_parent("tr") or a.find_parent("li") or a.parent
      title = a.get_text(strip=True)
      date_str = _find_first_date_in_row(row) if row else ""
      url = _full_url(base, a.get("href", "").strip())
      it = {"no": "", "trust_name": "무궁화신탁", "title": title, "post_date": date_str, "url": url}
      items.append(_enrich_row(it))
    return items

  for row in rows:
    a = row.select_one("td.tit a[href]")
    if not a:
      continue
    href = a.get("href", "").strip()
    if not DETAIL_HREF_RE.match(href):
      continue
    title = a.get_text(strip=True)
    date_str = _find_first_date_in_row(row)
    url = _full_url(base, href)
    no = _extract_row_number(row)
    it = {"no": no, "trust_name": "무궁화신탁", "title": title, "post_date": date_str, "url": url}
    items.append(_enrich_row(it))
  return items

# ────────── 상태/시트 유틸 ──────────
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

def _open_sheet():
  creds = Credentials.from_service_account_file(SERVICE_KEY_FILE, scopes=SCOPES)
  logging.info("Using service account: %s", creds.service_account_email)
  gc = gspread.authorize(creds)
  sh = gc.open_by_key(SPREADSHEET_ID)
  try:
    ws = sh.worksheet(WORKSHEET_NAME)
  except gspread.WorksheetNotFound:
    ws = sh.add_worksheet(title=WORKSHEET_NAME, rows="1000", cols="20")
  # 헤더 보장
  existing = ws.get_all_values()
  if not existing:
    ws.append_row(HEADER)
  else:
    # 헤더에 'no'가 없을 경우(기존 시트), 맨 앞에 'no' 컬럼을 추가하도록 안내
    if "no" not in existing[0]:
      # 안전을 위해 여기선 경고만 찍고 계속 진행(append는 HEADER 순서로 들어감)
      logging.warning("시트 헤더에 'no' 컬럼이 없습니다. 새로 append되는 행은 'no'가 첫 컬럼으로 추가됩니다.")
  return ws

def _load_existing_from_sheet(ws) -> Tuple[set, Dict[str, int]]:
  """
  시트에서 기존 URL/주소를 읽어:
  - seen_urls: 이미 저장된 URL 집합
  - address_counts: address별 빈도 (duplicate 표기를 위해)
  """
  values = ws.get_all_values()
  if not values:
    return set(), {}
  header = values[0]
  col_index = {name: i for i, name in enumerate(header)}
  seen_urls = set()
  address_counts: Dict[str, int] = {}
  for row in values[1:]:
    url = row[col_index.get("url", -1)] if col_index.get("url") is not None and col_index["url"] < len(row) else ""
    addr = row[col_index.get("address", -1)] if col_index.get("address") is not None and col_index["address"] < len(row) else ""
    if url:
      seen_urls.add(url.strip())
    if addr:
      address_counts[addr.strip()] = address_counts.get(addr.strip(), 0) + 1
  return seen_urls, address_counts

def _append_rows(ws, rows: List[Dict[str, str]]):
  if not rows:
    return
  values = []
  for r in rows:
    values.append([
      r.get("no", ""),         # ← 번호
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

# ────────── 메인 ──────────
def main():
  setup_logging()
  ws = _open_sheet()

  # 시트에서 과거 URL/주소 로드 (append 방식에서 중복 방지/표기용)
  seen_urls_sheet, address_counts = _load_existing_from_sheet(ws)

  # state(파일)에서도 URL resume (둘 다 사용)
  state = _load_state()
  seen_urls_state = set(state.get("seen_urls") or [])
  last_done = state.get("last_done_page")

  # 최종 seen_urls = 시트 + state
  seen_urls = set(seen_urls_sheet) | set(seen_urls_state)

  start_page = START_PAGE
  if RESUME and last_done is not None:
    start_page = last_done + STEP

  logging.info("크롤 범위: %d → %d (step=%d)", start_page, END_PAGE, STEP)

  session = make_session()

  try:
    for page in range(start_page, END_PAGE - (1 if STEP < 0 else -1), STEP):
      logging.info("[LIST] 페이지 수집 시작 page=%s", page)
      html, base_used = fetch_list_page(session, page)
      if not html or not base_used:
        logging.warning("[LIST] page=%s 응답 없음 → 다음", page)
        time.sleep(DELAY_SEC)
        continue

      items = parse_list_items(html, base_used)

      # 중복 URL 제외 + duplicate 표기 + 배치 append
      batch_to_append: List[Dict[str, str]] = []
      for it in items:
        url = it["url"]
        if url in seen_urls:
          continue

        addr = (it.get("address") or "").strip()
        duplicate = ""
        if addr:
          prev = address_counts.get(addr, 0)
          if prev >= 1:
            duplicate = "중복"
          address_counts[addr] = prev + 1
        it["duplicate"] = duplicate

        batch_to_append.append(it)
        seen_urls.add(url)

      if batch_to_append:
        _append_rows(ws, batch_to_append)
        logging.info("[LIST] page=%s 시트 append rows=%d", page, len(batch_to_append))

      # 상태 저장
      state["last_done_page"] = page
      state["seen_urls"] = list(seen_urls)
      _save_state(state)

      time.sleep(DELAY_SEC)

  finally:
    logging.info("수집 종료")

if __name__ == "__main__":
  main()
