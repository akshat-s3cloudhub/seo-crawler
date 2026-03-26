import { chromium } from 'playwright';
import * as cheerio from 'cheerio';
import { Pool } from 'pg';
import * as dotenv from 'dotenv';
import pLimit from 'p-limit';
import express from 'express';

dotenv.config();

// ── Database ──
const db = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
  max: 3,
});

db.on('error', (err) => console.error('Unexpected DB error:', err));

// ── Server ──
const app = express();
const PORT = parseInt(process.env.PORT || '3000', 10);

app.use(express.json());

// ── Helpers ──
function sleep(ms: number) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function assignBand(score: number): string {
  if (score >= 50) return 'Elite';
  if (score >= 40) return 'Strong';
  if (score >= 30) return 'Average';
  if (score >= 20) return 'Weak';
  return 'Critical';
}

function assignPriorityAction(band: string): string {
  const map: Record<string, string> = {
    'Elite': 'Protect & Replicate',
    'Strong': 'Minor Enhancements',
    'Average': 'Structured Improvement',
    'Weak': 'Full Rewrite + Restructure',
    'Critical': 'Immediate Remediation',
  };
  return map[band] || 'Review Required';
}

function scheduleNextRun(): Date {
  const now = new Date();
  const next = new Date(now);
  next.setUTCHours(2, 0, 0, 0);
  if (next <= now) next.setDate(next.getDate() + 1);
  return next;
}

// ── Scoring Logic ──
async function crawlAndScore(url: string) {
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage();

  const startTime = Date.now();
  await page.goto(url, { waitUntil: 'networkidle', timeout: 30000 });
  const loadTime = Date.now() - startTime;

  const html = await page.content();
  await browser.close();

  const $ = cheerio.load(html);

  // ── CATEGORY A: Content Depth & Quality ──
  const bodyText = $('body').text().replace(/\s+/g, ' ').trim();
  const wordCount = bodyText.split(' ').filter(w => w.length > 0).length;
  const a1 = wordCount >= 1000 ? 2 : wordCount >= 500 ? 1 : 0;

  const metaDescription = $('meta[name="description"]').attr('content') || '';
  const hasDate = $('time, [class*="date"], [class*="published"]').length > 0;
  const a2 = hasDate ? 2 : metaDescription.length > 0 ? 1 : 0;

  const hasStats = /\d+%|\$\d+|\d+x|\d+ (million|billion|thousand)/i.test(bodyText);
  const a3 = hasStats ? 2 : 0;

  const headings = $('h2, h3, h4').length;
  const paragraphs = $('p').length;
  const a4 = headings >= 4 && paragraphs >= 6 ? 2 : headings >= 2 || paragraphs >= 3 ? 1 : 0;

  const avgSentenceLength = bodyText.split('.').reduce((a, s) => a + s.split(' ').length, 0) / Math.max(bodyText.split('.').length, 1);
  const a5 = avgSentenceLength < 20 ? 2 : avgSentenceLength < 30 ? 1 : 0;

  const hasAuthor = $('[class*="author"], [rel="author"], [itemprop="author"]').length > 0;
  const a6 = hasAuthor ? 2 : 0;

  const hasReadTime = /\d+\s*min(ute)?\s*read/i.test(bodyText);
  const a7 = hasReadTime ? 2 : 0;

  const cat_a = a1 + a2 + a3 + a4 + a5 + a6 + a7;

  // ── CATEGORY B: On-Page SEO ──
  const h1 = $('h1').first().text().trim();
  const b1 = h1.length > 0 ? 2 : 0;

  const titleTag = $('title').text().trim();
  const b2 = titleTag.length >= 50 && titleTag.length <= 65 ? 2 : titleTag.length >= 30 ? 1 : 0;

  const b3 = metaDescription.length >= 130 && metaDescription.length <= 170 ? 2 : metaDescription.length >= 80 ? 1 : 0;

  const slug = url.split('/').filter(Boolean).pop() || '';
  const b4 = slug.length > 3 && slug.includes('-') ? 2 : slug.length > 3 ? 1 : 0;

  const internalLinks = $('a[href]').filter((_, el) => {
    const href = $(el).attr('href') || '';
    return href.startsWith('/') || href.includes('wns.com');
  }).length;
  const b5 = internalLinks >= 8 ? 2 : internalLinks >= 3 ? 1 : 0;

  const hasSchema = $('script[type="application/ld+json"]').length > 0;
  const b6 = hasSchema ? 2 : 0;

  const cat_b = b1 + b2 + b3 + b4 + b5 + b6;

  // ── CATEGORY C: AEO Structure ──
  const hasDefinition = $('dl, [class*="definition"], [class*="glossary"]').length > 0 ||
    /^(what is|[a-z]+ is a|[a-z]+ refers to)/im.test(bodyText);
  const c1 = hasDefinition ? 2 : 0;

  const hasFAQ = $('[class*="faq"], [itemtype*="FAQPage"]').length > 0 ||
    $('script[type="application/ld+json"]').text().includes('FAQPage');
  const c2 = hasFAQ ? 2 : 0;

  const lists = $('ul, ol').length;
  const tables = $('table').length;
  const c3 = lists + tables >= 4 ? 2 : lists + tables >= 1 ? 1 : 0;

  const hasDirectAnswers = /^(yes|no|the answer is|in short|to summarize|the key)/im.test(bodyText);
  const c4 = hasDirectAnswers ? 2 : headings >= 3 ? 1 : 0;

  const namedEntityPattern = /\b(Gartner|Forrester|McKinsey|Deloitte|Accenture|IBM|SAP|Oracle|Microsoft|Google)\b/gi;
  const entityMatches = bodyText.match(namedEntityPattern) || [];
  const c5 = entityMatches.length >= 3 ? 2 : entityMatches.length >= 1 ? 1 : 0;

  const hasSpeakable = $('script[type="application/ld+json"]').text().includes('speakable');
  const c6 = hasSpeakable ? 2 : 0;

  const imagesWithAlt = $('img[alt]').filter((_, el) => ($(el).attr('alt') || '').length > 3).length;
  const c7 = imagesWithAlt >= 3 ? 2 : imagesWithAlt >= 1 ? 1 : 0;

  const cat_c = c1 + c2 + c3 + c4 + c5 + c6 + c7;

  // ── CATEGORY D: Authority & Trust ──
  const thirdPartyPattern = /\b(Gartner|Forrester|IDC|McKinsey|Deloitte|Harvard|MIT|Stanford|according to|cited by|research by)\b/gi;
  const thirdPartyMatches = bodyText.match(thirdPartyPattern) || [];
  const d1 = thirdPartyMatches.length >= 2 ? 2 : thirdPartyMatches.length >= 1 ? 1 : 0;

  const clientProofPattern = /\b(\d+%|\d+x|saved|reduced|increased|improved|delivered|achieved)\b/gi;
  const clientMatches = bodyText.match(clientProofPattern) || [];
  const d2 = clientMatches.length >= 3 ? 2 : clientMatches.length >= 1 ? 1 : 0;

  const regulatoryPattern = /\b(GDPR|HIPAA|SOX|ISO|compliance|regulatory|regulation|CCPA|PCI)\b/gi;
  const regulatoryMatches = bodyText.match(regulatoryPattern) || [];
  const d3 = regulatoryMatches.length >= 2 ? 2 : regulatoryMatches.length >= 1 ? 1 : 0;

  const industryTermPattern = /\b(digital transformation|automation|analytics|AI|machine learning|cloud|outsourcing|BPO|procurement|supply chain)\b/gi;
  const industryMatches = bodyText.match(industryTermPattern) || [];
  const d4 = industryMatches.length >= 5 ? 2 : industryMatches.length >= 2 ? 1 : 0;

  const cat_d = d1 + d2 + d3 + d4;

  // ── CATEGORY E: UX & Engagement ──
  const ctaText = $('a, button').map((_, el) => $(el).text().toLowerCase()).get().join(' ');
  const hasSpecificCTA = /(get started|contact us|request demo|download|speak to|schedule|learn more|explore)/i.test(ctaText);
  const e1 = hasSpecificCTA ? 2 : ctaText.length > 0 ? 1 : 0;

  const relatedLinks = $('[class*="related"], [class*="recommended"], [class*="more"]').find('a').length;
  const e2 = relatedLinks >= 3 ? 2 : relatedLinks >= 1 ? 1 : 0;

  const hasViewport = $('meta[name="viewport"]').length > 0;
  const e3 = hasViewport ? 2 : 0;

  const cat_e = e1 + e2 + e3;

  // ── CATEGORY F: WNS Brand Signals ──
  const wnsFrameworkPattern = /\b(WNS [A-Z][a-zA-Z]+|our framework|our methodology|our approach|WNS model)\b/gi;
  const f1 = wnsFrameworkPattern.test(bodyText) ? 2 : /\bWNS\b/.test(bodyText) ? 1 : 0;

  const quantifiedOutcomePattern = /\b(WNS (helped|enabled|delivered|saved|reduced|achieved|improved)|\d+% (improvement|reduction|increase|savings))\b/gi;
  const f2 = quantifiedOutcomePattern.test(bodyText) ? 2 : 0;

  const wnsToolPattern = /\b(DecisionPoint|WNS TRAC|WNS Verilytics|WNS Auto|WNS platform|WNS tool)\b/gi;
  const f3 = wnsToolPattern.test(bodyText) ? 2 : 0;

  const cat_f = f1 + f2 + f3;

  // ── TOTALS ──
  const totalScore = cat_a + cat_b + cat_c + cat_d + cat_e + cat_f;
  const band = assignBand(totalScore);
  const priorityAction = assignPriorityAction(band);

  return {
    a1, a2, a3, a4, a5, a6, a7,
    b1, b2, b3, b4, b5, b6,
    c1, c2, c3, c4, c5, c6, c7,
    d1, d2, d3, d4,
    e1, e2, e3,
    f1, f2, f3,
    cat_a, cat_b, cat_c, cat_d, cat_e, cat_f,
    totalScore, band, priorityAction, loadTime
  };
}

// ── Core Crawl Job ──
async function runCrawl() {
  console.log(`[${new Date().toISOString()}] 🚀 Starting WNS SEO Crawler...`);

  let { rows } = await db.query(`
    SELECT url FROM wns_seo_content_assets
    WHERE crawler_status = 'pending'
    ORDER BY no ASC
  `);

  if (rows.length === 0) {
    const result = await db.query(`
      SELECT url FROM wns_seo_content_assets
      WHERE crawler_status = 'crawled'
      ORDER BY last_crawled_at ASC NULLS FIRST
      LIMIT 50
    `);
    rows = result.rows;
    if (rows.length === 0) {
      console.log('No URLs found in database. Waiting for next scheduled run.');
      return;
    }
    console.log(`No pending URLs. Will recrawl ${rows.length} previously crawled pages.`);
  }

  console.log(`📋 Found ${rows.length} pages to crawl`);

  const limit = pLimit(3);
  let count = 0;
  let failed = 0;

  await Promise.all(
    rows.map(({ url }: { url: string }) =>
      limit(async () => {
        try {
          console.log(`🔍 Crawling: ${url}`);
          const result = await crawlAndScore(url);

          await db.query(`
            UPDATE wns_seo_content_assets SET
              a1_word_count = $1, a2_freshness = $2, a3_original_data = $3,
              a4_depth_of_insight = $4, a5_reading_level = $5, a6_author_byline = $6,
              a7_read_time_shown = $7,
              b1_h1_tag = $8, b2_meta_title = $9, b3_meta_description = $10,
              b4_url_slug = $11, b5_internal_links = $12, b6_schema_markup = $13,
              c1_definition_block = $14, c2_faq_section = $15, c3_lists_tables = $16,
              c4_answer_ready_passages = $17, c5_named_entities = $18,
              c6_speakable_content = $19, c7_multimedia = $20,
              d1_third_party_validation = $21, d2_client_proof_points = $22,
              d3_regulatory_refs = $23, d4_industry_depth = $24,
              e1_cta_quality = $25, e2_related_content = $26, e3_mobile_page_exp = $27,
              f1_wns_framework = $28, f2_quantified_outcome = $29, f3_wns_tool_platform = $30,
              cat_a_score = $31, cat_b_score = $32, cat_c_score = $33,
              cat_d_score = $34, cat_e_score = $35, cat_f_score = $36,
              total_score = $37, band = $38, priority_action = $39,
              crawler_status = 'crawled', last_crawled_at = NOW(),
              load_time_ms = $40
            WHERE url = $41
          `, [
            result.a1, result.a2, result.a3, result.a4, result.a5, result.a6, result.a7,
            result.b1, result.b2, result.b3, result.b4, result.b5, result.b6,
            result.c1, result.c2, result.c3, result.c4, result.c5, result.c6, result.c7,
            result.d1, result.d2, result.d3, result.d4,
            result.e1, result.e2, result.e3,
            result.f1, result.f2, result.f3,
            result.cat_a, result.cat_b, result.cat_c, result.cat_d, result.cat_e, result.cat_f,
            result.totalScore, result.band, result.priorityAction,
            result.loadTime, url
          ]);

          count++;
          console.log(`✅ [${count}/${rows.length}] ${result.band} (${result.totalScore}/60) — ${url}`);

          await sleep(1500);
        } catch (err) {
          failed++;
          console.error(`❌ Failed: ${url}`, err);

          await db.query(`
            UPDATE wns_seo_content_assets
            SET crawler_status = 'failed', last_crawled_at = NOW()
            WHERE url = $1
          `, [url]);
        }
      })
    )
  );

  console.log(`\n🎉 Crawler finished! ✅ ${count} crawled, ❌ ${failed} failed`);
}

// ── Scheduler ──
let isRunning = false;

async function scheduler() {
  console.log('📅 Scheduler started. Next run:', scheduleNextRun().toISOString());

  // Run immediately on startup
  if (!isRunning) {
    isRunning = true;
    try {
      await runCrawl();
    } finally {
      isRunning = false;
    }
  }

  // Then run every 24 hours
  setInterval(async () => {
    if (!isRunning) {
      isRunning = true;
      try {
        await runCrawl();
      } finally {
        isRunning = false;
      }
    } else {
      console.log('⏭️  Crawl already in progress, skipping scheduled run.');
    }
  }, 24 * 60 * 60 * 1000);
}

// ── Routes ──

// Health check — Railway probes this
app.get('/health', (req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString(), crawling: isRunning });
});

// Manual trigger
app.post('/trigger', (req, res) => {
  if (isRunning) {
    return res.status(409).json({ error: 'Crawl already in progress' });
  }
  res.json({ message: 'Crawl triggered!' });
  isRunning = true;
  runCrawl().finally(() => { isRunning = false; });
});

// Get crawl status
app.get('/status', async (req, res) => {
  try {
    const { rows } = await db.query(`
      SELECT
        COUNT(*) FILTER (WHERE crawler_status = 'crawled') as crawled,
        COUNT(*) FILTER (WHERE crawler_status = 'pending') as pending,
        COUNT(*) FILTER (WHERE crawler_status = 'failed') as failed,
        COUNT(*) as total,
        MAX(last_crawled_at) as last_run
      FROM wns_seo_content_assets
    `);
    res.json({
      crawling: isRunning,
      nextScheduledRun: scheduleNextRun().toISOString(),
      database: rows[0]
    });
  } catch (err) {
    res.status(500).json({ error: 'Database error', details: String(err) });
  }
});

// ── Start ──
app.listen(PORT, () => {
  console.log(`🌐 Server running on port ${PORT}`);
  scheduler();
});
