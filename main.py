#!/usr/bin/env python3
# ============================================
# CHAINABUSE SCRAPER - PRIVATE REPO OUTPUT
# Progress tracked via local progress.json
# Batch files pushed to private GitHub repo
# ============================================

import subprocess
import asyncio
from playwright.async_api import async_playwright
import json
from tqdm import tqdm
import time
import pandas as pd
from datetime import datetime
import random
import os
import requests
import base64

from config import (
    INPUT_CSV_COLUMN, INPUT_URL_FILTER,
    BATCH_SIZE, MAX_CONCURRENT, MAX_RETRIES,
    OUTPUT_DIR, BATCH_DIR, TOKEN, PROGRESS_FILE, PROCESSED_BATCH_DIR, PROGRESS_DIR
)
from source.data_loader import load_csv

# ============================================
# GITHUB PRIVATE REPO HELPERS
# ============================================

def get_github_headers():
    return {
        "Authorization": f"token {TOKEN}",
        "Accept": "application/vnd.github.v3+json",
        "Content-Type": "application/json"
    }

def push_file_to_repo(full_file_url: str, content: str, commit_message: str):
    headers = get_github_headers()

    sha = None
    check = requests.get(full_file_url, headers=headers)
    if check.status_code == 200:
        sha = check.json().get("sha")

    encoded = base64.b64encode(content.encode("utf-8")).decode("utf-8")
    payload = {"message": commit_message, "content": encoded}
    if sha:
        payload["sha"] = sha

    response = requests.put(full_file_url, headers=headers, json=payload)
    if response.status_code in (200, 201):
        print(f"✅ Pushed: {full_file_url}")
        return True
    else:
        print(f"⚠️  Failed: {response.status_code} {response.text[:200]}")
        return False

# ============================================
# PROGRESS FILE (LOCAL ONLY)
# ============================================

def load_progress():
    """Load local progress.json. Returns None if not found."""
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, 'r') as f:
            return json.load(f)
    return None

def save_progress(batch_num: int, url_index: int, current_url: str):
    data = {
        "latest_batch_number": batch_num,
        "next_url_index": url_index,
        "last_url": current_url,
        "last_updated": datetime.now().isoformat()
    }
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(data, f, indent=2)
    print(f"💾 progress.json updated → batch={batch_num}, next_index={url_index}, url={current_url}")

    # Push to chain-forensic repo via GitHub API
    full_url = f"{PROGRESS_DIR}{PROGRESS_FILE}"
    push_file_to_repo(full_url, json.dumps(data, indent=2), f"Update progress.json → batch={batch_num}, index={url_index}")

# ============================================
# SCRAPER
# ============================================

async def scrape_url(browser, url, semaphore, retry_count=0):
    """Scrape single URL - extract FULL details AFTER clicking each card"""
    async with semaphore:
        if retry_count > 0:
            await asyncio.sleep(random.uniform(2, 5))
        else:
            await asyncio.sleep(random.uniform(0.3, 0.8))

        context = None
        page = None

        try:
            context = await browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                viewport={'width': 1920, 'height': 1080}
            )
            page = await context.new_page()

            await page.set_extra_http_headers({
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
            })

            await page.goto(url, wait_until='domcontentloaded', timeout=60000)
            await asyncio.sleep(3)

            try:
                await page.wait_for_selector('.create-ScamReportCard', timeout=60000)
            except:
                return {
                    'url': url,
                    'reports': [],
                    'success': True,
                    'empty': True,
                    'note': 'No reports found',
                    'retry_count': retry_count
                }

            cards = page.locator('.create-ScamReportCard')
            card_count = await cards.count()
            print(f"📄 Found {card_count} cards on {url}")

            reports_data = []

            for i in range(card_count):
                try:
                    print(f"   🔍 Processing card {i+1}/{card_count}")

                    card = cards.nth(i)
                    await card.wait_for(state='attached', timeout=10000)

                    clickable = card.locator('[data-react-aria-pressable="true"][role="button"]').first
                    if await clickable.count() == 0:
                        print(f"⚠️  No clickable element in card {i}, skipping")
                        continue

                    report_url = None
                    try:
                        async with page.expect_navigation(wait_until='domcontentloaded', timeout=15000):
                            await clickable.click(timeout=10000)
                        report_url = page.url
                        await asyncio.sleep(2)
                    except Exception as nav_error:
                        print(f"⚠️  Navigation failed for card {i}: {str(nav_error)[:80]}")
                        if page.url != url:
                            await page.go_back(wait_until='domcontentloaded', timeout=10000)
                        continue

                    report_data = await page.evaluate('''() => {
                        const categoryEl = document.querySelector('.create-ScamReportDetails__category');
                        const category = categoryEl?.textContent?.trim() || null;

                        const descriptionEls = document.querySelectorAll('.create-LexicalViewer p');
                        const description = Array.from(descriptionEls)
                            .map(el => el.textContent?.trim())
                            .filter(text => text && text.length > 0)
                            .join(' ') || null;

                        const voteCountEl = document.querySelector('.create-BidirectionalVoting__vote-count');
                        const voteCount = parseInt(voteCountEl?.textContent?.trim() || '0');

                        let submittedBy = 'Anonymous';
                        let submittedTime = null;
                        const infoRow = document.querySelector('.create-ScamReportDetails__info-row');
                        if (infoRow) {
                            const labeledInfos = infoRow.querySelectorAll('.create-LabeledInfo');
                            labeledInfos.forEach((info, index) => {
                                const label = info.querySelector('label');
                                if (label && label.textContent.trim() === 'Submitted') {
                                    const textContent = info.textContent.replace('Submitted', '').trim();
                                    if (index === 0 && textContent) {
                                        submittedTime = textContent;
                                    } else if (index === 1) {
                                        const linkLabel = info.querySelector('.create-Link__label');
                                        if (linkLabel) {
                                            submittedBy = linkLabel.textContent.trim();
                                        }
                                    }
                                }
                            });
                        }

                        const lossesSection = document.querySelector('.create-LossesSection');
                        let lossAmount = null;
                        if (lossesSection) {
                            const lossPs = lossesSection.querySelectorAll('p.create-Text');
                            if (lossPs[1]) {
                                lossAmount = lossPs[1].textContent.trim();
                            }
                        }

                        const addressSections = document.querySelectorAll('.create-ReportedSection__address-section');
                        const addresses = [];
                        const domains = [];

                        addressSections.forEach(section => {
                            const addrText = section.querySelector('.create-ResponsiveAddress__text')?.textContent?.trim();
                            const chainImg = section.querySelector('img[alt*="logo"]');
                            const blockchain = chainImg?.alt?.replace(' logo', '') || null;
                            const badge = section.querySelector('.create-Badge span')?.textContent?.trim();

                            if (addrText) {
                                addresses.push({
                                    address: addrText,
                                    blockchain: blockchain,
                                    tag: badge || null
                                });
                            }

                            const domainText = section.querySelector('.create-ReportedSection__domain')?.textContent?.trim();
                            if (domainText) {
                                domains.push(domainText);
                            }
                        });

                        return {
                            category: category,
                            description: description,
                            submitted_by: submittedBy,
                            submitted_time: submittedTime,
                            vote_count: voteCount,
                            loss_amount: lossAmount,
                            addresses: addresses,
                            domains: domains,
                            total_addresses: addresses.length,
                            total_domains: domains.length
                        };
                    }''')

                    report_data['index'] = i
                    report_data['report_url'] = report_url
                    if '/report/' in report_url:
                        report_data['report_id'] = report_url.split('/report/')[-1].split('?')[0]
                    else:
                        report_data['report_id'] = None
                    report_data['source_url'] = url
                    report_data['scraped_at'] = datetime.now().isoformat()

                    reports_data.append(report_data)
                    print(f"      ✅ Card {i+1}: {report_data.get('category', 'No category')} ({len(report_data.get('addresses', []))} addr, {len(report_data.get('domains', []))} domains)")

                    await page.go_back(wait_until='domcontentloaded', timeout=15000)
                    await asyncio.sleep(1.5)

                    cards = page.locator('.create-ScamReportCard')

                except Exception as e:
                    print(f"⚠️  Error processing card {i}: {str(e)[:100]}")
                    if page.url != url:
                        try:
                            await page.goto(url, wait_until='domcontentloaded', timeout=15000)
                            await asyncio.sleep(2)
                        except:
                            pass
                    continue

            for report in reports_data:
                report.pop('index', None)

            return {
                'url': url,
                'reports': reports_data,
                'success': True,
                'empty': len(reports_data) == 0,
                'report_count': len(reports_data),
                'retry_count': retry_count
            }

        except Exception as e:
            error_msg = str(e)
            if 'Timeout' in error_msg or 'timeout' in error_msg.lower():
                error_type = 'Timeout'
            elif 'TargetClosed' in error_msg or 'closed' in error_msg.lower():
                error_type = 'Browser Closed'
            elif 'net::ERR' in error_msg:
                error_type = 'Network Error'
            elif '404' in error_msg:
                error_type = '404 Not Found'
            elif '429' in error_msg or 'Too Many Requests' in error_msg:
                error_type = '429 Rate Limit'
            else:
                error_type = 'Unknown Error'

            return {
                'url': url,
                'error': error_msg[:200],
                'error_type': error_type,
                'success': False,
                'retry_count': retry_count
            }

        finally:
            try:
                if page:
                    await page.close()
            except:
                pass
            try:
                if context:
                    await context.close()
            except:
                pass

async def scrape_batch(urls, max_concurrent=3, retry_count=0):
    """Process batch of URLs with progress bar"""
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage']
        )
        semaphore = asyncio.Semaphore(max_concurrent)
        tasks = [scrape_url(browser, url, semaphore, retry_count) for url in urls]
        results = []

        desc = "Retrying" if retry_count > 0 else "Scraping"
        for coro in tqdm(asyncio.as_completed(tasks), total=len(urls), desc=desc):
            result = await coro
            results.append(result)

        await browser.close()
        return results

# ============================================
# BATCH SAVE → PRIVATE REPO
# ============================================

def build_batch_payload(batch_num, url_start, url_end, urls_data):
    """Build batch JSON payload — same format as before."""
    successful_urls = []
    empty_urls = []
    failed_urls = []

    for data in urls_data:
        if not data['success']:
            failed_urls.append({
                'url': data['url'],
                'error_type': data.get('error_type', 'Unknown'),
                'error': data.get('error', ''),
                'retry_count': data.get('retry_count', 0)
            })
        elif data.get('empty', False):
            empty_urls.append({
                'url': data['url'],
                'note': 'No reports found'
            })
        else:
            successful_urls.append({
                'url': data['url'],
                'reports': data['reports'],
                'total_reports': len(data['reports']),
                'total_addresses': sum(len(r.get('addresses', [])) for r in data['reports']),
                'total_domains': sum(len(r.get('domains', [])) for r in data['reports'])
            })

    return {
        'batch_number': batch_num,
        'url_range': {
            'start': url_start,
            'end': url_end,
            'total': url_end - url_start
        },
        'created_at': datetime.now().isoformat(),
        'summary': {
            'total_urls': len(urls_data),
            'successful': len(successful_urls),
            'empty': len(empty_urls),
            'failed': len(failed_urls),
            'total_reports': sum(u['total_reports'] for u in successful_urls),
            'total_addresses': sum(u['total_addresses'] for u in successful_urls),
            'total_domains': sum(u['total_domains'] for u in successful_urls)
        },
        'successful_urls': successful_urls,
        'empty_urls': empty_urls,
        'failed_urls': failed_urls
    }

def push_batch_to_repo(batch_num, url_start, url_end, urls_data):
    payload = build_batch_payload(batch_num, url_start, url_end, urls_data)
    content = json.dumps(payload, indent=2)
    full_url = f"{BATCH_DIR}batch-{batch_num}.json"  # already a full GitHub API URL
    commit_msg = f"Add batch-{batch_num} (URLs {url_start}-{url_end})"
    return push_file_to_repo(full_url, content, commit_msg)


def process_and_push_batch(batch_num: int, batch_results_data: list):
    batch_reports = []
    batch_stats = {
        'total_processed': len(batch_results_data),
        'successful_urls': 0,
        'failed_urls': 0,
        'empty_pages': 0,
        'pages_with_reports': 0,
        'total_reports': 0,
        'total_addresses': 0,
        'total_domains': 0,
        'errors_by_type': {},
        'batch_number': batch_num,
        'created_at': datetime.now().isoformat()
    }

    permanently_failed = []

    for result in batch_results_data:
        if result['success']:
            batch_stats['successful_urls'] += 1
            if result.get('empty'):
                batch_stats['empty_pages'] += 1
            else:
                batch_stats['pages_with_reports'] += 1
                batch_reports.extend(result.get('reports', []))
        else:
            batch_stats['failed_urls'] += 1
            error_type = result.get('error_type', 'Unknown')
            batch_stats['errors_by_type'][error_type] = batch_stats['errors_by_type'].get(error_type, 0) + 1
            permanently_failed.append({
                'url': result['url'],
                'error': result.get('error', ''),
                'error_type': error_type,
                'retries': MAX_RETRIES,
                'failed_at': datetime.now().isoformat()
            })

    batch_stats['total_reports']   = len(batch_reports)
    batch_stats['total_addresses'] = sum(len(r.get('addresses', [])) for r in batch_reports)
    batch_stats['total_domains']   = sum(len(r.get('domains', [])) for r in batch_reports)

    processed_payload = {
        "reports":            batch_reports,
        "processed":          len(batch_results_data),
        "failed":             [],
        "permanently_failed": permanently_failed,
        "total_urls":         len(batch_results_data),
        "stats":              batch_stats,
        "timestamp":          datetime.now().isoformat()
    }

    content    = json.dumps(processed_payload, indent=2)
    full_url   = f"{PROCESSED_BATCH_DIR}processed_batch-{batch_num}.json"
    commit_msg = f"Add processed_batch-{batch_num} ({len(batch_reports)} reports)"

    success = push_file_to_repo(full_url, content, commit_msg)
    if success:
        print(f"✅ processed_batch-{batch_num}.json pushed ({len(batch_reports)} reports, {batch_stats['total_addresses']} addresses)")
    return success
                                
# ============================================
# MAIN SCRAPE LOOP
# ============================================

async def scrape_all(all_urls):
    """
    Main loop. Progress tracked via local progress.json.
    Batch files pushed to private repo after each batch.
    """
    start_index = 0

    # Resume from progress.json if it exists
    progress = load_progress()
    if progress:
        start_index = progress.get("next_url_index", 0)
        print(f"▶️  Resuming from index {start_index} (last batch: {progress.get('latest_batch_number')}, url: {progress.get('last_url')})")
    else:
        print("🆕 Starting fresh scrape")

    urls_to_process = all_urls[start_index:]
    print(f"📋 URLs remaining: {len(urls_to_process)}")

    for i in range(0, len(urls_to_process), BATCH_SIZE):
        current_index = start_index + i
        batch_urls = urls_to_process[i:i + BATCH_SIZE]
        # Batch number is 1-based, derived from absolute position
        batch_num = current_index // BATCH_SIZE + 1

        print(f"\n{'='*70}")
        print(f"📦 Batch {batch_num} | Absolute URLs: {current_index} → {current_index + len(batch_urls) - 1}")
        print(f"{'='*70}")

        batch_start_time = time.time()
        batch_results_data = []

        try:
            results = await scrape_batch(batch_urls, max_concurrent=MAX_CONCURRENT)

            permanently_failed_in_batch = []

            for result in results:
                batch_results_data.append(result)

            # Retry failures
            batch_failed = [r for r in batch_results_data if not r['success']]
            if batch_failed:
                print(f"\n🔄 Retrying {len(batch_failed)} failed URLs...")
                for retry_attempt in range(1, MAX_RETRIES + 1):
                    if not batch_failed:
                        break
                    await asyncio.sleep(5)
                    retry_urls = [r['url'] for r in batch_failed]
                    retry_results = await scrape_batch(retry_urls, max_concurrent=2, retry_count=retry_attempt)
                    still_failed = []

                    for result in retry_results:
                        # Replace original result with retry result
                        for idx, orig in enumerate(batch_results_data):
                            if orig['url'] == result['url']:
                                batch_results_data[idx] = result
                                break
                        if result['success']:
                            print(f"  ✅ Recovered: {result['url']}")
                        else:
                            still_failed.append(result)

                    batch_failed = still_failed

                for failed_result in batch_failed:
                    permanently_failed_in_batch.append(failed_result)

            # Stats summary
            successful = [r for r in batch_results_data if r['success'] and not r.get('empty')]
            empty = [r for r in batch_results_data if r['success'] and r.get('empty')]
            failed = [r for r in batch_results_data if not r['success']]
            total_reports = sum(len(r.get('reports', [])) for r in successful)
            batch_time = time.time() - batch_start_time

            print(f"\n📊 Batch {batch_num} → ✅{len(successful)} 📭{len(empty)} ❌{len(failed)} 📝{total_reports} | ⚡{len(batch_urls)/batch_time:.1f}/s")

            # Push batch to private repo
            push_batch_to_repo(batch_num, current_index, current_index + len(batch_urls), batch_results_data)

            # Push processed batch — this batch only, checkpoint format
            process_and_push_batch(batch_num, batch_results_data)

            # Update local progress.json — point to the NEXT unprocessed index
            next_index = current_index + len(batch_urls)
            last_url = batch_urls[-1] if batch_urls else ""
            save_progress(batch_num, next_index, last_url)

        except Exception as e:
            print(f"\n❌ Batch {batch_num} error: {e}")
            import traceback
            traceback.print_exc()
            # Save progress so we can resume from this batch
            save_progress(batch_num - 1, current_index, batch_urls[0] if batch_urls else "")
            raise

        await asyncio.sleep(2)

    print(f"\n🎉 All batches complete.")

# ============================================
# MAIN
# ============================================

async def main():
    
    if not TOKEN:
        raise ValueError("TOKEN environment variable not set")

    # Load URLs from private repo via data_loader
    df = load_csv()
    urls_list = df[INPUT_CSV_COLUMN].tolist()
    # urls_list = [url for url in urls_list if INPUT_URL_FILTER in url]  
    print(f"📋 Total URLs after filter: {len(urls_list)}")

    start_time = time.time()

    try:
        await scrape_all(urls_list)
    except KeyboardInterrupt:
        print("\n⚠️  Interrupted by user. progress.json saved, safe to resume.")
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        elapsed = time.time() - start_time
        print(f"\n⏱️  Total elapsed: {elapsed/60:.2f} min")


if __name__ == "__main__":
    asyncio.run(main())
