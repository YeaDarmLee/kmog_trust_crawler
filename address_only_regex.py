# -*- coding: utf-8 -*-
# address_only_regex.py (v8)
# - 세종특별자치시 특수 처리: 시/군/구 없이도 매칭
# - 전북특별자치도/강원특별자치도 대응 및 표준화
# - 광역시/도 약칭(예: '인천', '부산') 대응
# - 주소에 '...지구/...구역' 포함
# - 건물명에서 순수 지번/수량 토큰 제외
# - 🔧 보강: '시/구 단독 시작' 패턴 지원 (예: '서초구 서초동 1445', '부산 덕천동 397-1', '마산시 합성동 125-5')

import re

# ─────────────────────────────────────────────────────────────
# 기본 토큰
PREFIX = r"(?:\d+\.\s*)?"

# 세종 전용 토큰 (광역명)
PROV_SEJONG = r"(?:세종특별자치시|세종시|세종)"

# 광역/도 목록 (세종 제외) — 광역시 약칭 포함
REG1_NO_SEJONG = (
  r"(?:" 
  r"서울특별시|서울시|서울|"
  r"부산광역시|부산시|부산|"
  r"대구광역시|대구시|대구|"
  r"인천광역시|인천시|인천|"
  r"광주광역시|광주시|광주|"
  r"대전광역시|대전시|대전|"
  r"울산광역시|울산시|울산|"
  r"경기도|경기|"
  r"강원특별자치도|강원도|강원|"
  r"충청북도|충북|"
  r"충청남도|충남|"
  r"전북특별자치도|전라북도|전북|"
  r"전라남도|전남|"
  r"경상북도|경북|"
  r"경상남도|경남|"
  r"제주특별자치도|제주도|제주"
  r")"
)

# 전체 광역/도 (세종 포함)
REG1 = rf"(?:{PROV_SEJONG}|{REG1_NO_SEJONG})"

# 시/군/구 — 뒤에 공백/끝 경계 요구(강구면이 '강구'로 잘리는 문제 방지)
CITY_GUN_GU = r"(?:[가-힣]+(?:시|군|구))(?=\s|$)"

# 읍/면/동/가/리
TOWN = r"(?:[가-힣0-9]+(?:읍|면|동|가|리))"

# 도로명
ROAD = r"(?:[가-힣0-9]+(?:로|길|대로|번길))"

# 지번/산
LOT = r"(?:산\s*)?\d+(?:-\d+)?(?:\s*(?:일원|번지))?"
LOT_LIST = rf"{LOT}(?:\s*,\s*\d+(?:-\d+)?)*"

# 도시계획/구획 명칭 — 지구/구역까지 주소에 포함
PLAN_DIST = r"(?:[가-힣0-9]+(?:지구|구역))"

# ─────────────────────────────────────────────────────────────
# 1) 전체 주소만 추출 — 세종 분기 + 일반 분기 + (약칭 시/구 단독)
PATTERN = rf"""
^(?P<addr>
  {PREFIX}
  (?:
  # ◇ 세종 특수: 시/군/구 없이 읍/면/동부터 가능
  (?P<prov_sj>{PROV_SEJONG})
  (?:\s+{TOWN}){{0,3}}
  (?:\s+{PLAN_DIST})?
  (?:\s+{ROAD})?
  (?:\s*{LOT_LIST})?

  |
  # ◇ 일반: 광역/도 + (시/군/구 1개 이상)
  (?P<prov_etc>{REG1_NO_SEJONG})
  (?:\s+{CITY_GUN_GU})+
  (?:\s+{TOWN}){{0,3}}
  (?:\s+{PLAN_DIST})?
  (?:\s+{ROAD})?
  (?:\s*{LOT_LIST})?

  |
  # ◇ 보조: 시/군/구 단독 시작 (예: '서초구 서초동 1445', '마산시 합성동 125-5', '인천 삼산동')
  (?P<sgg_only>{CITY_GUN_GU})
  (?:\s+{TOWN}){{0,3}}
  (?:\s+{PLAN_DIST})?
  (?:\s+{ROAD})?
  (?:\s*{LOT_LIST})?
  )
)
"""
RX_ADDR = re.compile(PATTERN, re.X)

def extract_address(title: str) -> str:
  """
  제목에서 '행정구역~지번/지구/구역'까지의 '주소만' 추출.
  - 세종 특수/일반(광역+시군구)/시군구 단독 시작 모두 처리
  """
  m = RX_ADDR.search(title or "")
  if not m:
    return ""
  return re.sub(r"^\d+\.\s*", "", m.group("addr")).strip()

# ─────────────────────────────────────────────────────────────
# 2) 시군구(도시명): "광역/도 + 시군구" (세종은 광역만)
PATTERN_SGG = rf"""
^{PREFIX}
(?P<prov>{REG1_NO_SEJONG})
(?:\s+(?P<sgg1>{CITY_GUN_GU}))
(?:\s+(?P<sgg2>{CITY_GUN_GU}))?
"""
RX_SGG = re.compile(PATTERN_SGG, re.X)

# 세종 전용: 시/군/구 없이도 OK
RX_SGJ = re.compile(rf"^{PREFIX}(?P<prov>{PROV_SEJONG})\b")

# 시/군/구 단독 시작 (예: '서초구', '마산시 합성동', '부산 북구' 등)
PATTERN_SGG_ONLY = rf"""
^{PREFIX}
(?P<sgg1>{CITY_GUN_GU})
(?:\s+(?P<sgg2>{CITY_GUN_GU}))?
"""
RX_SGG_ONLY = re.compile(PATTERN_SGG_ONLY, re.X)

# 표준화 매핑 — 약칭 → 정식 명칭(최근 행정명칭 기준)
_PROV_STD = {
  # 광역시: 시/약칭 → 공식
  "서울": "서울특별시", "서울시": "서울특별시",
  "부산": "부산광역시", "부산시": "부산광역시",
  "대구": "대구광역시", "대구시": "대구광역시",
  "인천": "인천광역시", "인천시": "인천광역시",
  "광주": "광주광역시", "광주시": "광주광역시",
  "대전": "대전광역시", "대전시": "대전광역시",
  "울산": "울산광역시", "울산시": "울산광역시",

  # 도: 약칭/옛 명칭 → 최신 공식
  "경기": "경기도",
  "강원": "강원특별자치도", "강원도": "강원특별자치도",
  "충북": "충청북도",
  "충남": "충청남도",
  "전라북도": "전북특별자치도", "전북": "전북특별자치도",
  "전남": "전라남도",
  "경북": "경상북도",
  "경남": "경상남도",
  "제주": "제주특별자치도", "제주도": "제주특별자치도",

  # 세종
  "세종": "세종특별자치시", "세종시": "세종특별자치시",
}

def _normalize_province(p: str) -> str:
  return _PROV_STD.get(p, p)

def extract_province_sgg(title_or_address: str, *, use_address_fallback: bool = True) -> str:
  """
  출력: '광역/도 + 시군구'
  - 세종은 '세종특별자치시'만 반환(시/군/구 자체가 없음)
  - 광역/도 없이 '시/군/구'만 시작하는 케이스는 '시군구(시퀀스)'만 반환
    예) '서초구 서초동 ...' → '서초구'
        '마산시 합성동 ...' → '마산시 합성동' (연속 시군구 2개까지)
  """
  s = (title_or_address or "").strip()
  s = re.sub(r"^\d+\.\s*", "", s)

  m = RX_SGG.search(s)
  if not m:
    # 세종 특수 케이스
    m2 = RX_SGJ.search(s)
    if not m2 and use_address_fallback:
      addr = extract_address(s)
      if addr:
        m = RX_SGG.search(addr)
        if not m:
          m2 = RX_SGJ.search(addr)
    if m2:
      prov = _normalize_province(m2.group("prov"))
      return prov  # 세종: 광역만

    # 시/군/구 단독 시작 케이스 처리
    m3 = RX_SGG_ONLY.search(s)
    if not m3 and use_address_fallback:
      addr = extract_address(s)
      if addr:
        m3 = RX_SGG_ONLY.search(addr)
    if m3:
      sgg1 = (m3.group("sgg1") or "").strip()
      sgg2 = (m3.group("sgg2") or "").strip()
      return f"{sgg1}{(' ' + sgg2) if sgg2 else ''}".strip()

    if not m:
      return ""

  prov = _normalize_province(m.group("prov") or "")
  sgg1 = m.group("sgg1") or ""
  sgg2 = m.group("sgg2") or ""
  return f"{prov} {sgg1}{(' ' + sgg2) if sgg2 else ''}".strip()

def extract_city_sgg(title_or_address: str, *, use_address_fallback: bool = True) -> str:
  """
  출력: '시군구'만
  - 세종은 시/군/구가 없으므로 빈 문자열 반환
  - 시/군/구 단독 시작 케이스는 해당 시군구 시퀀스 반환
  """
  s = (title_or_address or "").strip()
  s = re.sub(r"^\d+\.\s*", "", s)

  m = RX_SGG.search(s)
  if not m:
    m2 = RX_SGJ.search(s)
    if not m2 and use_address_fallback:
      addr = extract_address(s)
      if addr:
        m = RX_SGG.search(addr)
        if not m:
          m2 = RX_SGJ.search(addr)
    if m2 and not m:
      return ""  # 세종: 시군구 없음

    # 시/군/구 단독 시작 케이스
    m3 = RX_SGG_ONLY.search(s)
    if not m3 and use_address_fallback:
      addr = extract_address(s)
      if addr:
        m3 = RX_SGG_ONLY.search(addr)
    if m3:
      sgg1 = (m3.group("sgg1") or "").strip()
      sgg2 = (m3.group("sgg2") or "").strip()
      return (sgg1 + (" " + sgg2 if sgg2 else "")).strip()

    if not m:
      return ""

  sgg1 = m.group("sgg1") or ""
  sgg2 = m.group("sgg2") or ""
  return (sgg1 + (" " + sgg2 if sgg2 else "")).strip()

# ─────────────────────────────────────────────────────────────
# 3) 건물명 추출: 동/층/호·공고키워드 제거 후, '지번/수량' 토큰 제외
_BUILDING_STOP = re.compile(
  r"(?:일괄매각|개별매각|매각\s*공고|매각공고|재공매|재매각|후\s*개별수의계약\s*공고)"
)
_UNIT_TOKENS = re.compile(
  r"(?:제?[0-9A-Za-z\-]+동|제?[0-9A-Za-z\-]+층|제?[0-9A-Za-z\-]+호|[0-9]+동|[0-9]+층|[0-9]+호)"
)
_BRACKET = re.compile(r"[\[\(].*?[\]\)]")
_SEPARATORS = re.compile(r"[,\u00B7·/]|(?:\s+외\s+)")

# 건물명 접미사 힌트
_BUILDING_SUFFIX = (
  r"(?:타워|팰리스|캐슬|캐슬플러스|팰리움|밸리|스퀘어|시티|힐스|힐스테이트|"
  r"아이파크|자이|더힐|프라자|프라임|스카이|에버빌|해링턴타워|해링턴|아파트|"
  r"오피스텔|연립주택|주건축물|몰|빌라|빌리지|스포츠몰|블록|블럭|롯트|로트)"
)
_BUILDING_CORE = r"[가-힣A-Za-z0-9\-·]+"
_BUILDING_CANDIDATE = re.compile(rf"({_BUILDING_CORE}(?:{_BUILDING_SUFFIX})?)")

# 건물명에서 제외할 패턴: 순수 지번/수량 단위
_EXCLUDE_BUILDING = re.compile(
  r"""^
    (?:외\s*\d+\s*)?          # '외1' 등
    (?:
      \d+(?:-\d+)?(?:번지)?       # 지번(706-10, 850-3번지)
      |
      \d+\s*개(?:\s*(?:호|호실|세대|필지))?
      |
      (?:호|호실|세대|필지)$
    )
  """, re.X
)

def extract_building_name(title: str) -> str:
  s = (title or "").strip()

  # 주소 제거
  addr = extract_address(s)
  tail = s[len(addr):].strip() if addr and s.startswith(addr) else s

  # 괄호/동·층·호/공고 키워드 제거
  tail = _BRACKET.sub(" ", tail)
  tail = _UNIT_TOKENS.sub(" ", tail)
  mstop = _BUILDING_STOP.search(tail)
  if mstop:
    tail = tail[:mstop.start()].strip()

  parts = [p.strip() for p in _SEPARATORS.split(tail) if p.strip()]
  best = ""

  def score(x: str) -> tuple:
    suf = x.endswith((
      "타워","팰리스","캐슬","밸리","스퀘어","시티","힐스","아이파크","자이","더힐",
      "프라자","프라임","스카이","에버빌","해링턴타워","해링턴","아파트","오피스텔",
      "빌라","빌리지","몰","스포츠몰","블록","블럭","롯트","로트"
    ))
    return (1 if suf else 0, len(x))

  for p in parts:
    cands = []
    for mm in _BUILDING_CANDIDATE.finditer(p):
      token = mm.group(1).strip("-·").strip()
      if len(token) >= 3 and not _EXCLUDE_BUILDING.match(token):
        cands.append(token)
    if cands:
      pick = sorted(cands, key=score, reverse=True)[0]
      if score(pick) > score(best):
        best = pick
  return best

# ─────────────────────────────────────────────────────────────
# 4) 매각내용: 1)주소 2)도시명 3)건물명 제거 후 나머지
def extract_sale_content(title: str) -> str:
  s = (title or "").strip()

  # 1) 주소 제거
  addr = extract_address(s)
  if addr:
    s = s.replace(addr, "", 1).strip()

  # 2) 광역/도 + 시군구 제거 (세종은 광역만 / 시군구 단독도 제거)
  prov_sgg = extract_province_sgg(title, use_address_fallback=True)
  if prov_sgg:
    s = s.replace(prov_sgg, "", 1).strip()

  # 3) 건물명 제거
  bld = extract_building_name(title)
  if bld:
    s = s.replace(bld, "", 1).strip()

  # 선두 '외'류 정리
  s = re.sub(r"^외\s*\d+\s*", "", s)
  s = re.sub(r"^외\s+", "", s)

  # 공백/구두점 정리
  s = re.sub(r"\s+", " ", s)
  s = re.sub(r"\s*([,\u00B7·/])\s*", r"\1", s)
  s = re.sub(r"^[,·/]+", "", s)
  s = re.sub(r"[,·/]+$", "", s)
  return s.strip()
