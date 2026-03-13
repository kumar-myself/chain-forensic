#!/usr/bin/env python3
# ============================================
# CHAINABUSE SCRAPER FOR GITHUB ACTIONS
# Saves data directly to repository - FULL REPORT DETAILS VERSION
# ============================================

import asyncio
from playwright.async_api import async_playwright
import json
from tqdm import tqdm
import time
import pandas as pd
from datetime import datetime
import random
import os
import subprocess

# Configuration
output_dir = 'chainabuse_data-sitemap-0-with-unique-id-v1/'
os.makedirs(output_dir, exist_ok=True)
print(f"✅ Output directory: {output_dir}")

PROGRESS_FILE = f'{output_dir}progress.json'

def save_progress(batch_num, url_index, url):
    """Save current progress so we can resume from next batch"""
    progress = {
        'last_batch_num': batch_num,
        'last_url_index': url_index,
        'last_url': url,
        'saved_at': datetime.now().isoformat()
    }
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(progress, f, indent=2)
    print(f"📌 Progress saved: batch-{batch_num}, URL index {url_index}")

def load_progress():
    """Load progress to resume from last saved batch"""
    if not os.path.exists(PROGRESS_FILE):
        return None
    try:
        with open(PROGRESS_FILE, 'r') as f:
            progress = json.load(f)
        print(f"📂 Resuming from batch-{progress['last_batch_num']}, URL index {progress['last_url_index']} ({progress['last_url']})")
        return progress
    except Exception as e:
        print(f"⚠️  Could not load progress: {e}")
        return None

def git_commit_and_push(message):
    """Commit and push changes to GitHub"""
    try:
        subprocess.run(['git', 'config', '--global', 'user.name', 'GitHub Actions Bot'], check=True)
        subprocess.run(['git', 'config', '--global', 'user.email', 'actions@github.com'], check=True)
        subprocess.run(['git', 'add', output_dir], check=True)
        subprocess.run(['git', 'commit', '-m', message], check=True)
        subprocess.run(['git', 'push'], check=True)
        print(f"✅ Pushed to GitHub: {message}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"⚠️  Git push failed: {e}")
        return False

async def scrape_url(browser, url, semaphore, retry_count=0):
    """Scrape single URL - extract FULL details AFTER clicking each card"""
    async with semaphore:
        # Smart sleep
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
            
            # Set extra headers
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
            
            # Get basic card count and positions first
            cards = page.locator('.create-ScamReportCard')
            card_count = await cards.count()
            print(f"📄 Found {card_count} cards on {url}")
            
            reports_data = []
            
            # Click EACH card and extract FULL details from report page
            for i in range(card_count):
                try:
                    print(f"   🔍 Processing card {i+1}/{card_count}")
                    
                    # Get fresh card reference
                    card = cards.nth(i)
                    await card.wait_for(state='attached', timeout=10000)
                    
                    # Find clickable element
                    clickable = card.locator('[data-react-aria-pressable="true"][role="button"]').first
                    if await clickable.count() == 0:
                        print(f"⚠️  No clickable element in card {i}, skipping")
                        continue
                    
                    # Navigate to report page
                    report_url = None
                    try:
                        async with page.expect_navigation(wait_until='domcontentloaded', timeout=15000):
                            await clickable.click(timeout=10000)
                        report_url = page.url
                        await asyncio.sleep(2)  # Let page fully load
                    except Exception as nav_error:
                        print(f"⚠️  Navigation failed for card {i}: {str(nav_error)[:80]}")
                        if page.url != url:
                            await page.go_back(wait_until='domcontentloaded', timeout=10000)
                        continue
                    
                    # EXTRACT FULL DETAILS from report page using HTML structure
                    report_data = await page.evaluate('''() => {
                        // Category/Title
                        const categoryEl = document.querySelector('.create-ScamReportDetails__category');
                        const category = categoryEl?.textContent?.trim() || null;

                        // Full description from Lexical editor
                        const descriptionEls = document.querySelectorAll('.create-LexicalViewer p');
                        const description = Array.from(descriptionEls)
                            .map(el => el.textContent?.trim())
                            .filter(text => text && text.length > 0)
                            .join(' ') || null;

                        // Vote count
                        const voteCountEl = document.querySelector('.create-BidirectionalVoting__vote-count');
                        const voteCount = parseInt(voteCountEl?.textContent?.trim() || '0');

                        // Submitted info - PERFECT MATCH FOR YOUR HTML
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

                        // Loss amount - EXACT MATCH
                        const lossesSection = document.querySelector('.create-LossesSection');
                        let lossAmount = null;
                        if (lossesSection) {
                            const lossPs = lossesSection.querySelectorAll('p.create-Text');
                            if (lossPs[1]) {
                                lossAmount = lossPs[1].textContent.trim();
                            }
                        }

                        // Addresses and Domains from ReportedSection
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
                    
                    # Add metadata
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
                    
                    # Go back to list page
                    await page.go_back(wait_until='domcontentloaded', timeout=15000)
                    await asyncio.sleep(1.5)
                    
                    # Refresh card locator after navigation
                    cards = page.locator('.create-ScamReportCard')
                
                except Exception as e:
                    print(f"⚠️  Error processing card {i}: {str(e)[:100]}")
                    # Try to recover by going back
                    if page.url != url:
                        try:
                            await page.goto(url, wait_until='domcontentloaded', timeout=15000)
                            await asyncio.sleep(2)
                        except:
                            pass
                    continue
            
            # Remove index before returning
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

def save_batch_file(batch_num, url_start, url_end, urls_data, batch_dir):
    """Save batch of URLs with all their data and push to GitHub"""
    
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
    
    batch_data = {
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
    
    batch_file = f'{batch_dir}batch-{batch_num}.json'
    with open(batch_file, 'w') as f:
        json.dump(batch_data, f, indent=2)
    
    print(f"💾 Saved batch-{batch_num}.json (URLs {url_start}-{url_end}: ✅{len(successful_urls)} 📭{len(empty_urls)} ❌{len(failed_urls)})")
    
    return batch_file

def flatten_reports(reports):
    """Flatten reports for CSV"""
    csv_data = []
    for report in reports:
        base = {
            'report_id': report.get('report_id', ''),
            'report_url': report.get('report_url', ''),
            'source_url': report.get('source_url', ''),
            'scraped_at': report.get('scraped_at', ''),
            'category': report.get('category'),
            'description': report.get('description'),
            'submitted_by': report.get('submitted_by'),
            'submitted_time': report.get('submitted_time'),
            'vote_count': report.get('vote_count'),
            'loss_amount': report.get('loss_amount', ''),
        }
        
        if report.get('addresses'):
            for addr in report['addresses']:
                row = base.copy()
                row.update({
                    'address': addr.get('address'),
                    'blockchain': addr.get('blockchain'),
                    'tag': addr.get('tag'),
                    'domain': ', '.join(report.get('domains', []))
                })
                csv_data.append(row)
        elif report.get('domains'):
            for domain in report['domains']:
                row = base.copy()
                row.update({'address': '', 'blockchain': '', 'tag': '', 'domain': domain})
                csv_data.append(row)
        else:
            row = base.copy()
            row.update({'address': '', 'blockchain': '', 'tag': '', 'domain': ''})
            csv_data.append(row)
    
    return csv_data

async def scrape_all_github(all_urls, batch_size=50, max_concurrent=3, max_retries=2):
    """Scrape with batch saving and GitHub push every 50 URLs. Resumes from last saved batch."""
    
    permanently_failed = []
    start_index = 0
    next_batch_num = 1

    # Create batch directory
    batch_dir = f'{output_dir}url_batches/'
    os.makedirs(batch_dir, exist_ok=True)

    # Resume from progress if available
    progress = load_progress()
    if progress:
        # last_url_index is the last URL index that was fully processed (end of last batch)
        start_index = progress['last_url_index']
        next_batch_num = progress['last_batch_num'] + 1
        print(f"✅ Resuming: skipping {start_index} URLs, starting at batch-{next_batch_num}")
    
    urls_to_process = all_urls[start_index:]
    
    stats = {
        'total_processed': 0,
        'successful_urls': 0,
        'failed_urls': 0,
        'empty_pages': 0,
        'pages_with_reports': 0,
        'total_reports': 0,
        'total_addresses': 0,
        'total_domains': 0,
        'start_time': datetime.now().isoformat(),
        'last_update': datetime.now().isoformat()
    }

    try:
        for i in range(0, len(urls_to_process), batch_size):
            batch_num = next_batch_num + (i // batch_size)
            batch_urls = urls_to_process[i:i + batch_size]
            current_index = start_index + i  # absolute index into all_urls

            print(f"\n{'='*70}")
            print(f"📦 Batch {batch_num} | URLs index: {current_index} → {current_index + len(batch_urls) - 1}")
            print(f"   First URL: {batch_urls[0]}")
            print(f"{'='*70}")

            batch_start_time = time.time()
            batch_results_data = []

            try:
                results = await scrape_batch(batch_urls, max_concurrent=max_concurrent)

                batch_stats = {'successful': 0, 'failed': 0, 'empty': 0, 'with_reports': 0, 'reports': 0}
                batch_failed = []

                for result in results:
                    stats['total_processed'] += 1
                    batch_results_data.append(result)

                    if result['success']:
                        stats['successful_urls'] += 1
                        batch_stats['successful'] += 1

                        if result.get('empty', False):
                            stats['empty_pages'] += 1
                            batch_stats['empty'] += 1
                        else:
                            stats['pages_with_reports'] += 1
                            batch_stats['with_reports'] += 1
                            batch_stats['reports'] += len(result['reports'])
                            stats['total_reports'] += len(result['reports'])
                            stats['total_addresses'] += sum(len(r.get('addresses', [])) for r in result['reports'])
                            stats['total_domains'] += sum(len(r.get('domains', [])) for r in result['reports'])
                    else:
                        stats['failed_urls'] += 1
                        batch_stats['failed'] += 1
                        batch_failed.append(result)

                stats['last_update'] = datetime.now().isoformat()
                batch_time = time.time() - batch_start_time
                print(f"\n📊 ✅{batch_stats['successful']} ❌{batch_stats['failed']} 📭{batch_stats['empty']} 📝{batch_stats['reports']} | ⚡{len(batch_urls)/batch_time:.1f} URL/s")

                # Retry failed URLs
                if batch_failed:
                    print(f"\n🔄 Retrying {len(batch_failed)} failed URLs...")
                    for retry_attempt in range(1, max_retries + 1):
                        if not batch_failed:
                            break
                        await asyncio.sleep(5)
                        retry_urls = [r['url'] for r in batch_failed]
                        retry_results = await scrape_batch(retry_urls, max_concurrent=2, retry_count=retry_attempt)
                        still_failed = []

                        for result in retry_results:
                            for idx, orig in enumerate(batch_results_data):
                                if orig['url'] == result['url']:
                                    batch_results_data[idx] = result
                                    break

                            if result['success']:
                                stats['total_reports'] += len(result['reports'])
                                stats['total_addresses'] += sum(len(r.get('addresses', [])) for r in result['reports'])
                                print(f"  ✅ Recovered: {result['url']}")
                            else:
                                still_failed.append(result)

                        batch_failed = still_failed

                    for failed_result in batch_failed:
                        permanently_failed.append({
                            'url': failed_result['url'],
                            'error': failed_result.get('error', ''),
                            'error_type': failed_result.get('error_type', 'Unknown'),
                            'retries': max_retries,
                            'failed_at': datetime.now().isoformat()
                        })

                # Save batch JSON
                save_batch_file(
                    batch_num,
                    current_index,
                    current_index + len(batch_urls),
                    batch_results_data,
                    batch_dir
                )

                # Update progress AFTER batch is saved
                last_url_index = current_index + len(batch_urls)
                last_url = batch_urls[-1]
                save_progress(batch_num, last_url_index, last_url)

                # Push batch + progress to GitHub
                git_commit_and_push(f"Add batch-{batch_num} (URLs {current_index}–{current_index + len(batch_urls) - 1})")

            except Exception as e:
                print(f"\n❌ Batch {batch_num} error: {e}")
                # Don't update progress — next run will retry this batch
                raise

            await asyncio.sleep(2)

    except KeyboardInterrupt:
        print(f"\n⚠️  Interrupted! Progress saved up to last completed batch.")
        raise

    return permanently_failed, stats, batch_dir


# ============================================
# MAIN EXECUTION
# ============================================

async def main():
    df = pd.read_csv('sitemap-0.csv')
    urls_list = df['loc'].tolist()
    urls_list = [url for url in urls_list if '/address/' in url]

    print(f"📋 Total URLs: {len(urls_list)}")

    start_time = time.time()

    try:
        permanently_failed, stats, batch_dir = await scrape_all_github(
            all_urls=urls_list,
            batch_size=50,
            max_concurrent=2,
            max_retries=2
        )

        elapsed = time.time() - start_time
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        if permanently_failed:
            failed_csv = f'{output_dir}permanently_failed_{timestamp}.csv'
            pd.DataFrame(permanently_failed).to_csv(failed_csv, index=False)
            print(f"⚠️  Permanently failed URLs saved: {failed_csv}")

        stats['end_time'] = datetime.now().isoformat()
        stats['total_elapsed_seconds'] = elapsed
        stats['urls_per_hour'] = len(urls_list) / (elapsed / 3600)

        stats_file = f'{output_dir}final_stats_{timestamp}.json'
        with open(stats_file, 'w') as f:
            json.dump(stats, f, indent=2)

        git_commit_and_push(f"Complete scraping - final stats saved")

        print(f"\n{'='*70}")
        print(f"✅ COMPLETE")
        print(f"{'='*70}")
        print(f"🔗 Addresses: {stats['total_addresses']}")
        print(f"✅ Successful URLs: {stats['successful_urls']} | ❌ Permanently failed: {len(permanently_failed)}")
        print(f"⏱️  {elapsed/60:.2f} min | {stats['urls_per_hour']:.1f} URLs/hr")
        print(f"🎉 Done!")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())
