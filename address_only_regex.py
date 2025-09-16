# -*- coding: utf-8 -*-
# address_only_regex.py (v10)
# - 세종특별자치시 특수 처리
# - 광역/도 약칭(서울/부산/인천/경기/충남/충북/전북/전남/경북/경남/제주 등) 대응
# - '시/군/구 단독 시작' + '광역 없이 시/군/구로 시작' + '광역 + 원시도시 + 동/읍/면' 대응
# - 주소는 행정구역~지번까지 최대 확장(예: '전주시 완산구 고사동 408-3')
# - 매각내용에서 '외 N개', 공고 키워드, 잔여 구두점 등을 정리

import re

# ─────────────────────────────────────────────────────────────
# 기본 토큰
PREFIX = r"(?:\d+\.\s*)?"

# 세종 전용 토큰 (광역명)
PROV_SEJONG = r"(?:세종특별자치시|세종시|세종)"

# 광역/도 목록 (세종 제외) — 광역시/도 약칭 포함
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

# 시/군/구 — 뒤에 공백/끝 경계 요구
CITY_GUN_GU = r"(?:[가-힣]+(?:시|군|구))(?=\s|$)"

# 광역 뒤에 붙는 '원시 도시명'(접미사 없음: 파주/포천/양양/홍성 등)
RAW_CITY = r"(?:[가-힣]{2,})"

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
# 1) 전체 주소만 추출 — 세종/일반/시군구 단독/보강 패턴
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
  # ◇ 보조1: 시/군/구 단독 시작 (예: '전주시 완산구 고사동 408-3', '서초구 서초동 1445')
  (?P<sgg_only>{CITY_GUN_GU})
  (?:\s+{CITY_GUN_GU})*
  (?:\s+{TOWN}){{0,3}}
  (?:\s+{PLAN_DIST})?
  (?:\s+{ROAD})?
  (?:\s*{LOT_LIST})?

  |
  # ◇ 보강2: 광역/도 + '원시 도시명' + 동/읍/면 (예: '경기 파주 야당동', '충남 홍성 오관리')
  (?P<prov_raw>{REG1_NO_SEJONG})
  \s+ (?P<rawcity>{RAW_CITY})
  (?:\s+{TOWN}){{1,3}}
  (?:\s+{PLAN_DIST})?
  (?:\s+{ROAD})?
  (?:\s*{LOT_LIST})?

  |
  # ◇ 보강3: 광역/도 + 동/읍/면 직결 (예: '인천 만수동', '제주 한림읍')
  (?P<prov_town>{REG1_NO_SEJONG})
  (?:\s+{TOWN}){{1,3}}
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
  """
  m = RX_ADDR.search(title or "")
  if not m:
    return ""
  return re.sub(r"^\d+\.\s*", "", m.group("addr")).strip()

# ─────────────────────────────────────────────────────────────
# 2) 시군구(도시명)
PATTERN_SGG = rf"""
^{PREFIX}
(?P<prov>{REG1_NO_SEJONG})
(?:\s+(?P<sgg1>{CITY_GUN_GU}))
(?:\s+(?P<sgg2>{CITY_GUN_GU}))?
"""
RX_SGG = re.compile(PATTERN_SGG, re.X)

# 세종 전용
RX_SGJ = re.compile(rf"^{PREFIX}(?P<prov>{PROV_SEJONG})\b")

# 시/군/구 단독 시작
PATTERN_SGG_ONLY = rf"""
^{PREFIX}
(?P<sgg1>{CITY_GUN_GU})
(?:\s+(?P<sgg2>{CITY_GUN_GU}))?
"""
RX_SGG_ONLY = re.compile(PATTERN_SGG_ONLY, re.X)

# 광역/도 + '원시 도시명' + 동/읍/면
PATTERN_PROV_RAWCITY = rf"""
^{PREFIX}
(?P<prov_raw2>{REG1_NO_SEJONG})
\s+(?P<rawcity2>{RAW_CITY})
\s+{TOWN}
"""
RX_PROV_RAWCITY = re.compile(PATTERN_PROV_RAWCITY, re.X)

# 광역/도 + 동/읍/면
PATTERN_PROV_TOWN = rf"""
^{PREFIX}
(?P<prov_town2>{REG1_NO_SEJONG})
\s+{TOWN}
"""
RX_PROV_TOWN = re.compile(PATTERN_PROV_TOWN, re.X)

# 표준화 매핑 — 약칭 → 정식 명칭
_PROV_STD = {
  "서울": "서울특별시", "서울시": "서울특별시",
  "부산": "부산광역시", "부산시": "부산광역시",
  "대구": "대구광역시", "대구시": "대구광역시",
  "인천": "인천광역시", "인천시": "인천광역시",
  "광주": "광주광역시", "광주시": "광주광역시",
  "대전": "대전광역시", "대전시": "대전광역시",
  "울산": "울산광역시", "울산시": "울산광역시",
  "경기": "경기도",
  "강원": "강원특별자치도", "강원도": "강원특별자치도",
  "충북": "충청북도",
  "충남": "충청남도",
  "전라북도": "전북특별자치도", "전북": "전북특별자치도",
  "전남": "전라남도",
  "경북": "경상북도",
  "경남": "경상남도",
  "제주": "제주특별자치도", "제주도": "제주특별자치도",
  "세종": "세종특별자치시", "세종시": "세종특별자치시",
}

def _normalize_province(p: str) -> str:
  return _PROV_STD.get(p, p)

def extract_province_sgg(title_or_address: str, *, use_address_fallback: bool = True) -> str:
  """
  출력: '광역/도 + 시군구'
  - 세종은 '세종특별자치시'만 반환
  - 시/군/구 단독 시작 → 해당 시군구 시퀀스 반환
  - 광역/도 + 원시도시 + 동/읍/면 → '광역/도 + 원시도시'
  - 광역/도 + 동/읍/면 → 광역/도만
  """
  s = (title_or_address or "").strip()
  s = re.sub(r"^\d+\.\s*", "", s)

  # 1) 정규 '광역/도 + 시군구'
  m = RX_SGG.search(s)
  if m:
    prov = _normalize_province(m.group("prov") or "")
    sgg1 = m.group("sgg1") or ""
    sgg2 = m.group("sgg2") or ""
    return f"{prov} {sgg1}{(' ' + sgg2) if sgg2 else ''}".strip()

  # 2) 세종 특수
  m2 = RX_SGJ.search(s)
  if not m2 and use_address_fallback:
    addr = extract_address(s)
    if addr:
      m = RX_SGG.search(addr)
      if m:
        prov = _normalize_province(m.group("prov") or "")
        sgg1 = m.group("sgg1") or ""
        sgg2 = m.group("sgg2") or ""
        return f"{prov} {sgg1}{(' ' + sgg2) if sgg2 else ''}".strip()
      m2 = RX_SGJ.search(addr)
  if m2:
    return _normalize_province(m2.group("prov"))

  # 3) 시/군/구 단독 시작
  m3 = RX_SGG_ONLY.search(s)
  if not m3 and use_address_fallback:
    addr = extract_address(s)
    if addr:
      m3 = RX_SGG_ONLY.search(addr)
  if m3:
    sgg1 = (m3.group("sgg1") or "").strip()
    sgg2 = (m3.group("sgg2") or "").strip()
    return f"{sgg1}{(' ' + sgg2) if sgg2 else ''}".strip()

  # 4) 광역/도 + 원시도시 + 동/읍/면
  m4 = RX_PROV_RAWCITY.search(s)
  if not m4 and use_address_fallback:
    addr = extract_address(s)
    if addr:
      m4 = RX_PROV_RAWCITY.search(addr)
  if m4:
    prov = _normalize_province(m4.group("prov_raw2") or "")
    rawc = (m4.group("rawcity2") or "").strip()
    return f"{prov} {rawc}".strip()

  # 5) 광역/도 + 동/읍/면 → 광역/도만
  m5 = RX_PROV_TOWN.search(s)
  if not m5 and use_address_fallback:
    addr = extract_address(s)
    if addr:
      m5 = RX_PROV_TOWN.search(addr)
  if m5:
    return _normalize_province(m5.group("prov_town2") or "")

  return ""

def extract_city_sgg(title_or_address: str, *, use_address_fallback: bool = True) -> str:
  """
  출력: '시군구'만
  - 세종은 빈 문자열
  - 시/군/구 단독 시작 → 해당 시군구 시퀀스
  - 광역/도 + 원시도시 + 동/읍/면 → 원시도시
  - 광역/도 + 동/읍/면 → 빈 문자열
  """
  s = (title_or_address or "").strip()
  s = re.sub(r"^\d+\.\s*", "", s)

  m = RX_SGG.search(s)
  if m:
    sgg1 = m.group("sgg1") or ""
    sgg2 = m.group("sgg2") or ""
    return (sgg1 + (" " + sgg2 if sgg2 else "")).strip()

  m2 = RX_SGJ.search(s)
  if not m2 and use_address_fallback:
    addr = extract_address(s)
    if addr:
      m = RX_SGG.search(addr)
      if m:
        sgg1 = m.group("sgg1") or ""
        sgg2 = m.group("sgg2") or ""
        return (sgg1 + (" " + sgg2 if sgg2 else "")).strip()
      m2 = RX_SGJ.search(addr)
  if m2:
    return ""

  m3 = RX_SGG_ONLY.search(s)
  if not m3 and use_address_fallback:
    addr = extract_address(s)
    if addr:
      m3 = RX_SGG_ONLY.search(addr)
  if m3:
    sgg1 = (m3.group("sgg1") or "").strip()
    sgg2 = (m3.group("sgg2") or "").strip()
    return (sgg1 + (" " + sgg2 if sgg2 else "")).strip()

  m4 = RX_PROV_RAWCITY.search(s)
  if not m4 and use_address_fallback:
    addr = extract_address(s)
    if addr:
      m4 = RX_PROV_RAWCITY.search(addr)
  if m4:
    return (m4.group("rawcity2") or "").strip()

  m5 = RX_PROV_TOWN.search(s)
  if not m5 and use_address_fallback:
    addr = extract_address(s)
    if addr:
      m5 = RX_PROV_TOWN.search(addr)
  if m5:
    return ""

  return ""

# ─────────────────────────────────────────────────────────────
# 3) 건물명 추출
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
    (?:외\s*\d+\s*)?
    (?:
      \d+(?:-\d+)?(?:번지)?
      |
      \d+\s*개(?:\s*(?:호|호실|세대|필지))?
      |
      (?:호|호실|세대|필지)$
    )
  """, re.X
)

def extract_building_name(title: str) -> str:
  s = (title or "").strip()
  addr = extract_address(s)
  tail = s[len(addr):].strip() if addr and s.startswith(addr) else s

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
# 4) 매각내용: 주소/도시명/건물명 제거 후 정리
_SALE_KEYWORDS = re.compile(r"(?:일괄매각|개별매각|매각\s*공고|매각공고|재공매|재매각)")
def extract_sale_content(title: str) -> str:
  s = (title or "").strip()

  # 1) 주소 제거
  addr = extract_address(s)
  if addr:
    s = s.replace(addr, "", 1).strip()

  # 2) '광역/도 + 시군구' 또는 '시/군/구 시퀀스' 제거(남아있을 수 있는 앞부분 방지)
  prov_sgg = extract_province_sgg(title, use_address_fallback=True)
  if prov_sgg:
    s = s.replace(prov_sgg, "", 1).strip()

  # 3) 건물명 제거
  bld = extract_building_name(title)
  if bld:
    s = s.replace(bld, "", 1).strip()

  # 4) 선두 '외 N개' 정리 + 공고 키워드 보존
  #   예: '외 8개 필지 및 공매' → '8개 필지 및 공매'
  s = re.sub(r"^외\s*(\d+)\s*", r"\1", s)

  # 5) 공백/구두점 정리
  s = re.sub(r"\s+", " ", s)
  s = re.sub(r"\s*([,\u00B7·/])\s*", r"\1", s)
  s = re.sub(r"^[,·/]+", "", s)
  s = re.sub(r"[,·/]+$", "", s)
  return s.strip()
