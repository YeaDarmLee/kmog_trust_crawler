# -*- coding: utf-8 -*-
# 한국토지신탁(코레이트) 공매공고 수집 - SPA 렌더링 + 상세 URL 회수(클릭)
import os
import re
import time
import json
import logging
from datetime import datetime
from typing import Dict, List
from urllib.parse import urljoin

import gspread
from google.oauth2.service_account import Credentials

from bs4 import BeautifulSoup

# Selenium (headless Chrome)
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager

# === 프로젝트 유틸 (이미 프로젝트에 있는 함수들) ===
from address_only_regex import (
  extract_address,
  extract_province_sgg,
  extract_building_name,
  extract_sale_content,
)

# ====== 상수/설정 ======
SPREADSHEET_ID   = "1BEoi3Q6pOoUBUcEDgdy1YF03Ehc1hY02KfKz31GMt7E"
WORKSHEET_NAME   = "한국토지_신탁"
SERVICE_KEY_FILE = "service_account.json"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

HEADER = [
  "번호","신탁사명","게시글 제목","게시일",
  "주소 (지번)","도시명","건물명","매각내용","용도",
  "중복여부","원문 링크(URL)"
]

TRUST_NAME = "한국토지신탁"
BASE = "https://www.koreit.co.kr"
LIST_PATH = "/land-trust/sale/short-notice"   # ?num=<page>&Keyword=

DATE_RE = re.compile(r"\d{4}[.\-\.]\s*\d{2}[.\-\.]\s*\d{2}")
DELAY_SEC = 0.5

# 페이지 범위 (필요 시 조정)
START_PAGE = 10
END_PAGE   = 1
STEP     = -1

# 재시작(Resume) 상태 저장
RESUME   = True
STATE_FILE = "output/.koreit_state.json"

# ====== 로깅 ======
def setup_logging():
  os.makedirs("logs", exist_ok=True)
  ts = datetime.now().strftime("%Y%m%d_%H%M%S")
  logfile = f"logs/koreit_sheets_{ts}.log"
  logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(logfile, encoding="utf-8"), logging.StreamHandler()],
  )
  logging.info("로그 시작: %s", logfile)

# ====== 구글시트 ======
ADDRESS_COLS = ["address", "주소 (지번)", "주소", "Address"]
DUP_COLS   = ["duplicate", "중복여부", "중복", "Duplicate"]
URL_COLS   = ["url", "원문 링크(URL)", "링크", "URL"]

def _find_col_index(header: list, candidates: list) -> int:
  m = {name: i for i, name in enumerate(header)}
  for c in candidates:
    if c in m:
      return m[c]
  return -1

def _col_letter(n: int) -> str:
  s = ""
  while n:
    n, r = divmod(n-1, 26)
    s = chr(65+r) + s
  return s

def _open_sheet():
  creds = Credentials.from_service_account_file(SERVICE_KEY_FILE, scopes=SCOPES)
  logging.info("Using service account: %s", creds.service_account_email)
  gc = gspread.authorize(creds)
  sh = gc.open_by_key(SPREADSHEET_ID)
  try:
    ws = sh.worksheet(WORKSHEET_NAME)
  except gspread.WorksheetNotFound:
    ws = sh.add_worksheet(title=WORKSHEET_NAME, rows="2000", cols="20")
  vals = ws.get_all_values()
  if not vals:
    ws.append_row(HEADER)
  elif vals[0] != HEADER:
    logging.warning("시트 헤더가 코드 정의와 다릅니다. (유연 매칭으로 계속 진행)")
  return ws

def _normalize_duplicate_numbering(ws):
  vals = ws.get_all_values()
  if not vals:
    return
  header = vals[0]
  a_i = _find_col_index(header, ADDRESS_COLS)
  d_i = _find_col_index(header, DUP_COLS)
  if a_i < 0 or d_i < 0:
    return

  groups: Dict[str, List[int]] = {}
  for r, row in enumerate(vals[1:], start=2):
    addr = (row[a_i].strip() if a_i < len(row) else "")
    groups.setdefault(addr, []).append(r)

  dup_col = [header[d_i]] + [""] * (len(vals) - 1)
  for addr, rows in groups.items():
    if addr and len(rows) >= 2:
      for k, r in enumerate(rows, start=1):
        dup_col[r-1] = f"중복{k}"

  col = _col_letter(d_i + 1)
  ws.update(
    range_name=f"{col}1:{col}{len(dup_col)}",
    values=[[v] for v in dup_col],
    value_input_option="USER_ENTERED",
  )

def _load_existing_from_sheet(ws):
  vals = ws.get_all_values()
  if not vals:
    return set(), {}, (-1, -1, -1, -1)
  header = vals[0]
  url_i = _find_col_index(header, URL_COLS)
  adr_i = _find_col_index(header, ADDRESS_COLS)
  tit_i = _find_col_index(header, ["게시글 제목"])
  dat_i = _find_col_index(header, ["게시일"])
  seen = set()
  addr_counts: Dict[str, int] = {}
  for row in vals[1:]:
    url = (row[url_i].strip() if 0 <= url_i < len(row) else "")
    if url:
      seen.add(url)
    addr = (row[adr_i].strip() if 0 <= adr_i < len(row) else "")
    if addr:
      addr_counts[addr] = addr_counts.get(addr, 0) + 1
  return seen, addr_counts, (url_i, adr_i, tit_i, dat_i)

def _append_rows(ws, rows: List[Dict[str, str]]):
  if not rows:
    return
  values = []
  for r in rows:
    values.append([
      r.get("no",""), TRUST_NAME, r.get("title",""), r.get("post_date",""),
      r.get("address",""), r.get("city",""), r.get("building",""),
      r.get("sale_content",""), r.get("purpose",""),
      r.get("duplicate",""), r.get("url",""),
    ])
  ws.append_rows(values, value_input_option="USER_ENTERED")

# ====== 상태 ======
def _load_state() -> Dict:
  try:
    if os.path.exists(STATE_FILE):
      with open(STATE_FILE,"r",encoding="utf-8") as f:
        return json.load(f)
  except Exception:
    pass
  return {"last_done_page": None, "seen_keys": []}

def _save_state(state: Dict):
  try:
    os.makedirs("output", exist_ok=True)
    with open(STATE_FILE,"w",encoding="utf-8") as f:
      json.dump(state, f, ensure_ascii=False)
  except Exception:
    pass

# ====== 도우미 ======
def _normalize_date(s: str) -> str:
  s = (s or "").strip()
  m = DATE_RE.search(s)
  if not m:
    return ""
  d = m.group(0)
  d = d.replace(" ", "").replace(".", "-")
  if "--" in d:
    d = d.replace("--", "-")
  return d

def _purpose_flag(title: str) -> str:
  t = (title or "")
  return "오피스텔" if "오피스텔" in t else ""

# ====== 렌더링/파싱 ======
def make_driver():
  opts = Options()
  opts.add_argument("--headless=new")
  opts.add_argument("--no-sandbox")
  opts.add_argument("--disable-gpu")
  opts.add_argument("--disable-dev-shm-usage")
  opts.add_argument("--window-size=1366,900")
  opts.add_argument("--lang=ko-KR,ko")
  opts.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36")
  service = ChromeService(ChromeDriverManager().install())
  driver = webdriver.Chrome(service=service, options=opts)
  driver.set_page_load_timeout(30)
  return driver

def render_list_page(driver, page: int) -> str:
  url = urljoin(BASE, LIST_PATH) + f"?num={page}&Keyword="
  driver.get(url)
  try:
    WebDriverWait(driver, 15).until(
      EC.presence_of_element_located((By.CSS_SELECTOR, ".sub-board-wrap table"))
    )
    time.sleep(0.8)  # 행 렌더링 보정
  except TimeoutException:
    logging.warning("[WAIT] 리스트 테이블 로딩 타임아웃")
  html = driver.page_source
  os.makedirs("debug", exist_ok=True)
  with open(f"debug/koreit_p{page}.html", "w", encoding="utf-8") as f:
    f.write(html)
  return html

def parse_list_html(html: str, list_url: str) -> List[Dict[str,str]]:
  items: List[Dict[str,str]] = []
  soup = BeautifulSoup(html, "lxml")
  table = soup.select_one(".sub-board-wrap table") or soup.find("table")
  if not table:
    logging.warning("[PARSE] table 미발견")
    return items

  tbody = table.find("tbody") or table
  trs = tbody.find_all("tr")
  logging.info("[PARSE] rows=%d", len(trs))

  for idx, tr in enumerate(trs, start=1):
    # 번호
    no = ""
    no_el = tr.find("td", class_="td-number")
    if no_el:
      m = re.search(r"\d+", no_el.get_text(strip=True))
      no = m.group(0) if m else ""

    # 제목/URL
    title, url = "", ""
    subj = tr.find("td", class_="td-subject")
    if subj:
      a = subj.find("a")
      if a:
        title = a.get_text(" ", strip=True)
        href = a.get("href", "")
        if href and href.lower() != "javascript:void(0)":
          url = urljoin(list_url, href)
      else:
        title = subj.get_text(" ", strip=True)

    # 날짜
    post_date = ""
    for td in tr.find_all("td"):
      cand = td.get_text(" ", strip=True)
      if DATE_RE.search(cand):
        post_date = _normalize_date(cand)
        break

    if not title:
      continue

    address = extract_address(title) or ""
    city = extract_province_sgg(title, use_address_fallback=True) or ""
    building = extract_building_name(title) or ""
    sale_content = extract_sale_content(title) or ""
    purpose = _purpose_flag(title)

    items.append({
      "row_index": idx,  # 클릭-해결 매핑용
      "no": no, "title": title, "post_date": post_date, "url": url,
      "address": address, "city": city, "building": building,
      "sale_content": sale_content, "purpose": purpose,
    })
  return items

def resolve_detail_urls_by_click(driver, list_url: str) -> Dict[int, str]:
  """
  리스트 화면에서 href가 javascript:void(0)인 항목을 실제 클릭해 상세 URL을 회수.
  return: {row_index: detail_url}
  """
  detail_urls: Dict[int, str] = {}

  def back_to_list():
    # 뒤로가기 후 테이블 존재 대기. 실패 시 리스트 URL 재진입
    try:
      driver.back()
      WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, ".sub-board-wrap table"))
      )
    except Exception:
      driver.get(list_url)
      try:
        WebDriverWait(driver, 10).until(
          EC.presence_of_element_located((By.CSS_SELECTOR, ".sub-board-wrap table"))
        )
      except Exception:
        pass
    time.sleep(0.3)

  rows = driver.find_elements(By.CSS_SELECTOR, ".sub-board-wrap table tbody tr")
  for i in range(1, len(rows) + 1):
    a_sel = f".sub-board-wrap table tbody tr:nth-child({i}) td.td-subject a"
    try:
      a_el = driver.find_element(By.CSS_SELECTOR, a_sel)
    except WebDriverException:
      continue

    href = (a_el.get_attribute("href") or "").strip().lower()
    if href and href != "javascript:void(0)":
      detail_urls[i] = urljoin(list_url, href)
      continue

    # 클릭해서 라우터 이동
    before = driver.current_url
    try:
      driver.execute_script("arguments[0].click();", a_el)
    except WebDriverException:
      # 클릭 실패 시 스킵
      continue

    changed = False
    # 1) URL 변화를 우선 체크
    try:
      WebDriverWait(driver, 8).until(lambda d: d.current_url != before)
      changed = True
    except TimeoutException:
      # 2) URL이 안 바뀌면 상세뷰 컨테이너 로딩을 체크 (모달/동일URL 케이스)
      try:
        WebDriverWait(driver, 8).until(
          EC.presence_of_all_elements_located(
            (By.CSS_SELECTOR, ".view, .view-wrap, .sub-board-view, .detail, .board-view")
          )
        )
        changed = True
      except TimeoutException:
        pass

    if changed:
      # 새 탭으로 열렸을 수도 있으니 마지막 핸들로 이동 확인
      try:
        if len(driver.window_handles) > 1:
          driver.switch_to.window(driver.window_handles[-1])
      except Exception:
        pass

      detail_urls[i] = driver.current_url

      # 새 탭이면 닫고 복귀
      try:
        if len(driver.window_handles) > 1:
          driver.close()
          driver.switch_to.window(driver.window_handles[0])
      except Exception:
        pass

    # 리스트로 복귀
    back_to_list()

  return detail_urls

# ====== 메인 ======
def main():
  setup_logging()
  ws = _open_sheet()
  _normalize_duplicate_numbering(ws)

  seen_sheet, addr_counts, _ = _load_existing_from_sheet(ws)

  state = _load_state()
  seen_keys = set(seen_sheet) | set(state.get("seen_keys") or [])
  last_done = state.get("last_done_page")

  start_page = START_PAGE
  if RESUME and last_done is not None:
    start_page = last_done + (1 if STEP > 0 else -1)

  logging.info("크롤 범위: %s → %s (step=%s)", start_page, END_PAGE, STEP)

  driver = make_driver()
  try:
    rng_end_inclusive = END_PAGE + (1 if STEP > 0 else -1)
    for page in range(start_page, rng_end_inclusive, STEP):
      list_url = urljoin(BASE, LIST_PATH) + f"?num={page}&Keyword="
      html = render_list_page(driver, page)
      items = parse_list_html(html, urljoin(BASE, LIST_PATH))
      items = list(reversed(items))

      # 원문링크 비어있는 항목들 → 실제 클릭해서 URL 회수
      if any(not it.get("url") for it in items):
        detail_map = resolve_detail_urls_by_click(driver, list_url)
        # row_index로 1:1 매핑
        for it in items:
          if not it.get("url"):
            ridx = it.get("row_index")
            if ridx in detail_map:
              it["url"] = detail_map[ridx]

      logging.info("[LIST] page=%s items=%d", page, len(items))

      # 신규만 필터링 & 중복 번호 표기
      batch: List[Dict[str, str]] = []
      for it in items:
        key = it.get("url") or f"{it.get('title','')}|{it.get('post_date','')}"
        if not key or key in seen_keys:
          continue

        addr = (it.get("address") or "").strip()
        duplicate = ""
        if addr:
          prev = addr_counts.get(addr, 0)
          if prev >= 1:
            duplicate = f"중복{prev + 1}"
          addr_counts[addr] = prev + 1

        it["duplicate"] = duplicate
        batch.append(it)
        seen_keys.add(key)

      if batch:
        _append_rows(ws, batch)
        _normalize_duplicate_numbering(ws)
        logging.info("[LIST] page=%s appended rows=%d", page, len(batch))

      # 상태 저장
      state["last_done_page"] = page
      state["seen_keys"] = list(seen_keys)
      _save_state(state)

      time.sleep(DELAY_SEC)
  finally:
    try:
      driver.quit()
    except Exception:
      pass
    logging.info("수집 종료")

if __name__ == "__main__":
  main()
