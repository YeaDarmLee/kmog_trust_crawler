import time, subprocess, json, re
from typing import Dict, List, Tuple
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

from address_only_regex import (
  extract_address,
  extract_province_sgg,
  extract_building_name,
  extract_sale_content,
)

def _purpose_officetel_flag(title: str) -> str:
  # 기존 스키마 유지: 오피스텔 여부만 '용도'에 표기
  return "오피스텔" if "오피스텔" in (title or "") else ""

subprocess.Popen('C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe --remote-debugging-port=9222 --user-data-dir="C:\\chromeCookie\\kmong_Rohmin_nol"'.format("C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe"))

# Selenium 옵션 설정
options = Options()
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
options.add_argument('--ignore-certificate-errors')
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_argument('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3')
options.add_experimental_option("debuggerAddress", "127.0.0.1:9222")

# ChromeDriver 실행
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

# 사이트 진입
driver.get('https://kbret.co.kr/auction/7683')

idx = 1
items: List[Dict[str, str]] = []
TRUST_NAME = "KB부동산신탁"

while True:
  time.sleep(0.5)

  # 맨 마지막 페이지 이동
  title = driver.find_element(By.CSS_SELECTOR, "div.board_tit strong").text
  date = driver.find_element(By.XPATH, "//div[@class='info']//dl[dt[text()='등록일']]/dd").text
  current_url = driver.current_url

  address = extract_address(title) or ""
  city = extract_province_sgg(title, use_address_fallback=True) or ""
  building = extract_building_name(title) or ""
  sale_content = extract_sale_content(title) or ""
  purpose = _purpose_officetel_flag(title)

  items.append({
    "no": idx,
    "trust_name": TRUST_NAME,
    "title": title,
    "post_date": date,
    "url": current_url,
    "address": address,
    "city": city,
    "building": building,
    "sale_content": sale_content,
    "purpose": purpose,
  })
  
  idx += 1

  try:
    next_btn = driver.find_element(By.CSS_SELECTOR, "a.page.next")
    next_btn.click()
  except:
    break

# === ⬇️ 여기부터: Google Sheets 업로드 전용 최소 블록 추가 ⬇️ ===
import gspread
from google.oauth2.service_account import Credentials

SPREADSHEET_ID   = "1BEoi3Q6pOoUBUcEDgdy1YF03Ehc1hY02KfKz31GMt7E"  # 예: "1BEoi3Q6pOoUBUcEDgdy1YF03Ehc1hY02KfKz31GMt7E"
WORKSHEET_NAME   = "KB부동산_신탁"          # 시트 탭 이름
SERVICE_KEY_FILE = "service_account.json"     # 서비스키 파일 경로
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

HEADER = [
  "번호","신탁사명","게시글 제목","게시일",
  "주소 (지번)","도시명","건물명","매각내용","용도",
  "중복여부","원문 링크(URL)"
]

def _open_sheet():
  creds = Credentials.from_service_account_file(SERVICE_KEY_FILE, scopes=SCOPES)
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
    # 헤더가 다르면 그대로 두고 유연하게 진행
    pass
  return ws

def _find_col_index(header: list, name: str) -> int:
  try:
    return header.index(name)
  except ValueError:
    return -1

def _load_existing(ws):
  vals = ws.get_all_values()
  if not vals:
    return set(), {}
  header = vals[0]
  url_idx  = _find_col_index(header, "원문 링크(URL)")
  addr_idx = _find_col_index(header, "주소 (지번)")
  seen_urls = set()
  addr_counts = {}
  for row in vals[1:]:
    if 0 <= url_idx < len(row):
      u = row[url_idx].strip()
      if u:
        seen_urls.add(u)
    if 0 <= addr_idx < len(row):
      a = row[addr_idx].strip()
      if a:
        addr_counts[a] = addr_counts.get(a, 0) + 1
  return seen_urls, addr_counts

def _append_rows(ws, rows):
  if not rows: return
  ws.append_rows(rows, value_input_option="USER_ENTERED")

def _normalize_duplicate(ws):
  vals = ws.get_all_values()
  if not vals: return
  header = vals[0]
  try:
    dup_idx = header.index("중복여부")
    addr_idx = header.index("주소 (지번)")
  except ValueError:
    return
  # 주소별 그룹핑
  groups = {}
  for r, row in enumerate(vals[1:], start=2):
    addr = row[addr_idx].strip() if addr_idx < len(row) else ""
    groups.setdefault(addr, []).append(r)
  # 중복 라벨 재계산
  col = [header[dup_idx]] + [""] * (len(vals) - 1)
  for addr, rows in groups.items():
    if not addr: continue
    if len(rows) >= 2:
      for k, rr in enumerate(rows, start=1):
        col[rr - 1] = f"중복{k}"
  # 컬럼 갱신
  def _col_letter(n: int) -> str:
    s = ""
    while n:
      n, r = divmod(n - 1, 26)
      s = chr(r + 65) + s
    return s
  col_letter = _col_letter(dup_idx + 1)
  ws.update([[v] for v in col], range_name=f"{col_letter}1:{col_letter}{len(col)}", value_input_option="USER_ENTERED")

# ==== ⬇️ 루프가 끝난 뒤, 수집 결과 items를 시트에 업로드 ====
ws = _open_sheet()
seen_urls, addr_counts = _load_existing(ws)

# 이미 존재하는 URL은 스킵, '중복여부'는 기존 시트+이번 배치 합산으로 라벨링
rows_to_append = []
for it in items:
  url = (it.get("url") or "").strip()
  if not url or url in seen_urls:
    continue

  addr = (it.get("address") or "").strip()
  dup_label = ""
  if addr:
    prev = addr_counts.get(addr, 0)
    if prev >= 1:
      dup_label = f"중복{prev + 1}"
    addr_counts[addr] = prev + 1

  rows_to_append.append([
    it.get("no", ""),
    it.get("trust_name", ""),
    it.get("title", ""),
    it.get("post_date", ""),
    addr,
    it.get("city", ""),
    it.get("building", ""),
    it.get("sale_content", ""),
    it.get("purpose", ""),
    dup_label,
    url
  ])
  seen_urls.add(url)

# 시트에 추가 & 중복열 정돈
_append_rows(ws, rows_to_append)
_normalize_duplicate(ws)
print(f"[OK] appended: {len(rows_to_append)} rows")
# === ⬆️ 여기까지 붙이면 끝! ⬆️ ===
