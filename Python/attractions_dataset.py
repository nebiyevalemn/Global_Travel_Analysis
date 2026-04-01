





"""
TripAdvisor Attraction Scraper - RESUME VERSION (SeleniumBase)
==============================================================
- Resume supported via scrape_progress.json
- Dedup via done_urls.txt
- Appends to CSV (resume-friendly)
"""

import pandas as pd
import time
import random
import re
import json
import os
from bs4 import BeautifulSoup
from seleniumbase import SB
from datetime import datetime
from urllib.parse import urlsplit, urlunsplit, quote_plus

# =============================================================================
# CONFIGURATION
# =============================================================================

TOP30_COUNTRIES_3_CITIES = [
    ("UK", ["London", "Manchester", "Edinburgh"]),
    ("France", ["Paris", "Nice", "Lyon"]),
    ("Germany", ["Frankfurt", "Munich", "Berlin"]),
    ("Italy", ["Rome", "Milan", "Venice"]),
    ("Spain", ["Barcelona", "Madrid", "Seville"]),
    ("Turkey", ["Istanbul", "Antalya", "Izmir"]),
    ("UAE", ["Dubai", "Abu Dhabi", "Sharjah"]),
    ("USA", ["New York", "Los Angeles", "Miami"]),
    ("Netherlands", ["Amsterdam", "Rotterdam", "Eindhoven"]),
    ("Japan", ["Tokyo", "Osaka", "Nagoya"]),
    ("Thailand", ["Bangkok", "Phuket", "Chiang Mai"]),
    ("Czechia", ["Prague", "Brno", "Ostrava"]),
    ("Greece", ["Athens", "Thessaloniki", "Heraklion (Crete)"]),
    ("Portugal", ["Lisbon", "Porto", "Faro"]),
    ("South Korea", ["Seoul", "Busan", "Jeju"]),
    ("Morocco", ["Marrakech", "Casablanca", "Fes"]),
    ("Mexico", ["Cancun", "Mexico City", "Guadalajara"]),
    ("Australia", ["Sydney", "Melbourne", "Brisbane"]),
    ("Switzerland", ["Zurich", "Geneva", "Basel"]),
    ("Austria", ["Vienna", "Salzburg", "Innsbruck"]),
    ("Malta", ["Valletta", "Sliema", "St. Julian's"]),
    ("Iceland", ["Reykjavik", "Akureyri", "Egilsstadir"]),
    ("Vietnam", ["Hanoi", "Ho Chi Minh City", "Da Nang"]),
    ("Denmark", ["Copenhagen", "Billund", "Aarhus"]),
    ("Canada", ["Toronto", "Vancouver", "Montreal"]),
    ("Egypt", ["Cairo", "Sharm El Sheikh", "Hurghada"]),
    ("India", ["Delhi", "Mumbai", "Bengaluru"]),
    ("Singapore", ["Singapore", "Singapore (Alt)", "Singapore (Alt2)"]),
    ("Brazil", ["Rio de Janeiro", "Sao Paulo", "Salvador"]),
    ("Indonesia", ["Bali (Denpasar)", "Jakarta", "Surabaya"]),
]

OUTPUT_FILE = "tripadvisor_attractions.csv"
PROGRESS_FILE = "scrape_progress.json"
DONE_URLS_FILE = "done_urls.txt"

BATCH_SIZE = 10
MAX_URLS_PER_CITY = 200

# Link filter (səndəki kimi saxladım)
URL_PATTERNS = [
    "/AttractionProductReview-",
    "/Attraction_Review-",
    "/AttractionProductDetail-",
    "/AttractionProduct-",
]

# =============================================================================
# HELPERS
# =============================================================================

def polite_sleep(min_s=1.5, max_s=4.0):
    time.sleep(random.uniform(min_s, max_s))

def normalize_url(u: str) -> str:
    """Remove querystring and fragment to reduce duplicates."""
    try:
        parts = urlsplit(u)
        return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))
    except:
        return u

def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"country_idx": 0, "city_idx": 0, "url_idx": 0}

def save_progress(country_idx: int, city_idx: int, url_idx: int):
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump(
            {"country_idx": country_idx, "city_idx": city_idx, "url_idx": url_idx},
            f,
            ensure_ascii=False,
            indent=2
        )

def load_done_urls() -> set:
    if not os.path.exists(DONE_URLS_FILE):
        return set()
    with open(DONE_URLS_FILE, "r", encoding="utf-8") as f:
        return set(line.strip() for line in f if line.strip())

def add_done_url(url: str):
    with open(DONE_URLS_FILE, "a", encoding="utf-8") as f:
        f.write(url + "\n")

def ensure_output_header():
    if not os.path.exists(OUTPUT_FILE):
        pd.DataFrame(columns=[
            'country', 'city', 'headline', 'price_raw', 'price_value', 'currency',
            'review_score', 'review_count', 'location_city', 'source_url', 'scraped_at_utc'
        ]).to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')

def save_to_csv(data_list):
    if not data_list:
        return
    df = pd.DataFrame(data_list)
    cols = [
        'country', 'city', 'headline', 'price_raw', 'price_value', 'currency',
        'review_score', 'review_count', 'location_city', 'source_url', 'scraped_at_utc'
    ]
    for c in cols:
        if c not in df.columns:
            df[c] = None
    df = df[cols]
    df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig', mode='a', header=False)

# =============================================================================
# PARSE DETAIL PAGE
# =============================================================================

def parse_attraction_details(html_content, url, city, country):
    soup = BeautifulSoup(html_content, 'html.parser')

    extracted = {
        'country': country,
        'city': city,
        'headline': None,
        'price_raw': None,
        'price_value': None,
        'currency': None,
        'review_score': None,
        'review_count': None,
        'location_city': None,
        'source_url': url,
        'scraped_at_utc': datetime.utcnow().isoformat()
    }

    # HEADLINE
    try:
        h1 = soup.find('h1', {'data-automation': 'mainH1'}) or soup.find('h1')
        if h1:
            extracted['headline'] = h1.get_text(strip=True)
    except:
        pass

    # PRICE
    try:
        price_div = soup.find(attrs={"data-automation": "commerce_module_visible_price"})
        if price_div:
            extracted['price_raw'] = price_div.get_text(" ", strip=True)

        if extracted['price_raw']:
            raw = extracted['price_raw']
            if '$' in raw:
                extracted['currency'] = '$'
            elif '€' in raw:
                extracted['currency'] = '€'
            elif '£' in raw:
                extracted['currency'] = '£'

            m = re.search(r'[\d,]+\.?\d*', raw)
            if m:
                extracted['price_value'] = float(m.group(0).replace(',', ''))
    except:
        pass

    # REVIEWS
    try:
        score_tag = soup.find(attrs={"data-automation": "bubbleRatingValue"})
        if score_tag:
            t = score_tag.get_text(" ", strip=True)
            m = re.search(r'(\d+(\.\d+)?)', t)
            if m:
                extracted['review_score'] = float(m.group(1))

        count_tag = soup.find(attrs={"data-automation": "bubbleReviewCount"})
        if count_tag:
            t = count_tag.get_text(" ", strip=True)
            m = re.search(r'([\d,]+)', t)
            if m:
                extracted['review_count'] = int(m.group(1).replace(',', ''))
    except:
        pass

    extracted['location_city'] = city
    return extracted

# =============================================================================
# COLLECT URLS (IMPROVED: Search -> Listing -> Scroll + Next)
# =============================================================================

def _find_listing_url_from_html(html: str) -> str | None:
    """
    Try to find a listing page like:
    /Attractions-g187323-Activities-Berlin.html
    """
    soup = BeautifulSoup(html, "html.parser")
    candidates = []

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith("/Attractions-g") and "-Activities" in href and href.endswith(".html"):
            candidates.append(href)

    # pick first candidate (usually best)
    if candidates:
        return "https://www.tripadvisor.com" + candidates[0]

    # regex fallback
    m = re.search(r'(\/Attractions\-g\d+\-Activities[^"\s<>]*\.html)', html)
    if m:
        return "https://www.tripadvisor.com" + m.group(1)

    return None

def collect_city_urls(sb, city):
    print(f"   🔎 Searching attractions for: {city}...")
    collected = set()

    # 1) Open SEARCH (encoded city is more stable)
    q = quote_plus(f"Things to do in {city}")
    search_url = f"https://www.tripadvisor.com/Search?q={q}"

    try:
        sb.uc_open_with_reconnect(search_url, 4)
        time.sleep(3)
    except Exception as e:
        print(f"   ⚠️ Search open error: {e}")
        return []

    search_html = sb.get_page_source()
    listing_url = _find_listing_url_from_html(search_html)

    if not listing_url:
        print("   ⚠️ Listing link tapılmadı. Search-dən yığmağa fallback (az ola bilər).")
        listing_url = search_url

    print(f"   👉 Listing URL: {listing_url}")

    # 2) Open LISTING
    try:
        sb.uc_open_with_reconnect(listing_url, 4)
        time.sleep(3)
    except Exception as e:
        print(f"   ⚠️ Listing open error: {e}")
        return []

    def scan_current_page():
        soup = BeautifulSoup(sb.get_page_source(), "html.parser")
        links = soup.find_all("a", href=True)

        before = len(collected)
        for a in links:
            href = a["href"]
            if any(p in href for p in URL_PATTERNS):
                full_url = "https://www.tripadvisor.com" + href if href.startswith("/") else href
                full_url = normalize_url(full_url)
                collected.add(full_url)
                if len(collected) >= MAX_URLS_PER_CITY:
                    break

        return len(collected) - before

    def scroll_focus(rounds=6):
        # Listing page-də daha çox kart yüklənsin deyə scroll edirik
        for _ in range(rounds):
            try:
                sb.scroll_to_bottom()
            except:
                sb.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            scan_current_page()
            if len(collected) >= MAX_URLS_PER_CITY:
                return

    def click_next():
        # Next linkini həm seleniumbase selector ilə, həm də href-dən tapmağa çalışırıq
        next_selectors = [
            'a[aria-label="Next page"]',
            'a[aria-label="Next"]',
            'a.ui_button.nav.next',
        ]
        for sel in next_selectors:
            try:
                if sb.is_element_visible(sel):
                    sb.click(sel)
                    time.sleep(3)
                    return True
            except:
                continue

        # HTML-dən rel/aria-label next axtar
        try:
            soup = BeautifulSoup(sb.get_page_source(), "html.parser")
            a = soup.find("a", attrs={"aria-label": re.compile("Next", re.I)}, href=True)
            if a:
                href = a["href"]
                next_url = "https://www.tripadvisor.com" + href if href.startswith("/") else href
                sb.open(next_url)
                time.sleep(3)
                return True
        except:
            pass

        return False

    page_no = 0
    stagnant_pages = 0

    while len(collected) < MAX_URLS_PER_CITY and page_no < 50 and stagnant_pages < 2:
        before = len(collected)

        # scroll + scan
        scroll_focus(rounds=7)
        added = len(collected) - before

        print(f"   📄 Listing page {page_no}: +{added} | total={len(collected)}")

        if added == 0:
            stagnant_pages += 1
        else:
            stagnant_pages = 0

        if len(collected) >= MAX_URLS_PER_CITY:
            break

        if not click_next():
            break

        page_no += 1

    return list(collected)

# =============================================================================
# MAIN (RESUME)
# =============================================================================

def main():
    print("\n" + "="*60)
    print("🎡 TripAdvisor Attraction Scraper (SELENIUMBASE UC) - RESUME")
    print(f"💾 Output: {OUTPUT_FILE}")
    print(f"🧠 Progress: {PROGRESS_FILE}")
    print("="*60)

    ensure_output_header()

    progress = load_progress()
    done_urls = load_done_urls()
    data_buffer = []

    # ✅ DÜZƏLİŞ #1: Chrome açılsın deyə headless=False
    with SB(uc=True, headless=False) as sb:

        for c_i, (country, cities) in enumerate(TOP30_COUNTRIES_3_CITIES):
            if c_i < progress["country_idx"]:
                continue

            for city_i, city in enumerate(cities):
                if c_i == progress["country_idx"] and city_i < progress["city_idx"]:
                    continue

                print(f"\n🌍 Processing: {city}, {country} (country_idx={c_i}, city_idx={city_i})")

                target_urls = collect_city_urls(sb, city)
                print(f"   🔗 Found {len(target_urls)} URLs (before dedupe).")

                start_url_idx = 0
                if c_i == progress["country_idx"] and city_i == progress["city_idx"]:
                    start_url_idx = progress["url_idx"]

                for u_i, url in enumerate(target_urls):
                    if u_i < start_url_idx:
                        continue

                    if url in done_urls:
                        save_progress(c_i, city_i, u_i + 1)
                        continue

                    print(f"   [{u_i+1}/{len(target_urls)}] Extracting: {url[:70]}...")

                    try:
                        sb.uc_open_with_reconnect(url, 3)
                        time.sleep(random.uniform(2, 4))

                        # ✅ DÜZƏLİŞ #3: detail page-də də yüngül scroll
                        sb.execute_script("window.scrollTo(0, 600);")
                        time.sleep(1)

                        html = sb.get_page_source()
                        item = parse_attraction_details(html, url, city, country)

                        valid = bool(item.get("headline")) and (
                            item.get("price_raw") or item.get("review_score") is not None or item.get("review_count") is not None
                        )

                        if valid:
                            print(f"      ✅ Got: {item['headline'][:40]}...")
                            data_buffer.append(item)
                            done_urls.add(url)
                            add_done_url(url)
                        else:
                            print("      ⚠️ Not valid (missing headline/price/reviews). Skipped but checkpoint saved.")

                        save_progress(c_i, city_i, u_i + 1)

                        if len(data_buffer) >= BATCH_SIZE:
                            print(f"      💾 Saving batch of {len(data_buffer)} rows...")
                            save_to_csv(data_buffer)
                            data_buffer = []

                    except Exception as e:
                        print(f"      ❌ Error: {e} (checkpoint saved, continue)")
                        save_progress(c_i, city_i, u_i + 1)
                        continue

                    polite_sleep(1.2, 3.0)

                save_progress(c_i, city_i + 1, 0)

        if data_buffer:
            print(f"\n💾 Final save: {len(data_buffer)} rows...")
            save_to_csv(data_buffer)

    # Optional cleanup
    try:
        if os.path.exists(PROGRESS_FILE):
            os.remove(PROGRESS_FILE)
    except:
        pass

    print("\n" + "="*60)
    print("✅ Completed!")
    print(f"📊 Results saved to {OUTPUT_FILE}")
    print("="*60 + "\n")

if __name__ == "__main__":
    main()