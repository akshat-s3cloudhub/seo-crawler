// ─────────────────────────────────────────────
// WNS SEO/AEO Crawler — Complete Ready-to-Run Script
// Save as: crawler.mjs  |  Node.js 20+ required
// ─────────────────────────────────────────────

import { chromium } from "playwright";
import * as cheerio from "cheerio";
import pg from "pg";
import { createHash } from "crypto";

const { Pool } = pg;

// ── CONFIG ──────────────────────────────────
const DB_URL   = process.env.DATABASE_URL || "postgresql://USER:PASSWORD@HOST:5432/DBNAME";
const CONCURRENCY = 3;   // pages crawled at once (be polite to wns.com)
const DELAY_MS    = 1500; // ms between requests
// ─────────────────────────────────────────────

const db = new Pool({ connectionString: DB_URL });

const WNS_FRAMEWORKS  = ["TRIANGE","RADAR","Evora","Precizon","Collections Maturity Model","DecisionPoint","ClaimEdge","Vuram"];
const WNS_PRODUCTS    = ["Triange","Precizon","Evora","DecisionPoint","ClaimEdge","Vuram"];
const ANALYST_SOURCES = ["Gartner","Forrester","IDC","McKinsey","Deloitte","Everest Group","HfS","Nelson Hall","ISG"];
const REGULATIONS     = ["GDPR","HIPAA","SOX","PCI DSS","IFRS","Basel","Solvency II","CCPA","FCA","DPDP"];

async function main() {
  console.log("🚀 WNS Crawler starting...");
  const { rows } = await db.query(
    "SELECT no, url FROM wns_seo_content_assets WHERE last_crawled_at IS NULL OR last_crawled_at < NOW() - INTERVAL '7 days' ORDER BY no ASC"
  );
  console.log(`📋 ${rows.length} URLs to crawl`);

  const { rows: jobRows } = await db.query(
    "INSERT INTO wns_seo_crawl_jobs (job_type,status,started_at,total_urls,triggered_by) VALUES ('full_crawl','running',NOW(),$1,'manual') RETURNING id",
    [rows.length]
  );
  const jobId = jobRows[0].id;

  let crawled = 0, failed = 0;
  const queue = [...rows];

  async function worker() {
    while (queue.length > 0) {
      const row = queue.shift();
      if (!row) break;
      try {
        console.log(`[${row.no}/${rows.length}] ${row.url.split("/").pop()}`);
        await crawlPage(row.url);
        crawled++;
        if (crawled % 20 === 0) {
          await db.query("UPDATE wns_seo_crawl_jobs SET crawled_urls=$1 WHERE id=$2",[crawled,jobId]);
          console.log(`  ✅ ${crawled} done, ${queue.length} remaining`);
        }
      } catch(e) {
        failed++;
        console.error(`  ❌ Failed: ${row.url} — ${e.message}`);
      }
      await sleep(DELAY_MS);
    }
  }

  await Promise.all(Array(CONCURRENCY).fill(null).map(worker));
  await db.query("UPDATE wns_seo_crawl_jobs SET status='completed',completed_at=NOW(),crawled_urls=$1,failed_urls=$2 WHERE id=$3",[crawled,failed,jobId]);
  console.log(`\n🎉 Done! Crawled: ${crawled} | Failed: ${failed}`);
  await db.end();
}

async function crawlPage(url) {
  const browser = await chromium.launch({ headless: true });
  const page    = await browser.newPage({ userAgent: "RYVRBot/1.0 (+https://wns.com)" });
  let statusCode = 200;
  page.on("response", r => { if (r.url() === url) statusCode = r.status(); });
  const start = Date.now();
  await page.goto(url, { waitUntil: "domcontentloaded", timeout: 25000 });
  const html    = await page.content();
  const finalUrl = page.url();
  const loadMs  = Date.now() - start;
  await browser.close();

  const params = extract(url, html);
  const scores = score(params);
  const hash   = createHash("sha256").update(html.slice(0,8000)).digest("hex");

  const prev = await db.query("SELECT total_score,band,content_hash FROM wns_seo_content_assets WHERE url=$1",[url]);
  const prevRow = prev.rows[0];

  await db.query(`UPDATE wns_seo_content_assets SET
    a1_word_count=$1,a2_freshness=$2,a3_original_data=$3,a4_depth_of_insight=$4,
    a5_reading_level=$5,a6_author_byline=$6,a7_read_time_shown=$7,
    b1_h1_tag=$8,b2_meta_title=$9,b3_meta_description=$10,b4_url_slug=$11,b5_internal_links=$12,b6_schema_markup=$13,
    c1_definition_block=$14,c2_faq_section=$15,c3_lists_tables=$16,c4_answer_ready_passages=$17,
    c5_named_entities=$18,c6_speakable_content=$19,c7_multimedia=$20,
    d1_third_party_validation=$21,d2_client_proof_points=$22,d3_regulatory_refs=$23,d4_industry_depth=$24,
    e1_cta_quality=$25,e2_related_content=$26,e3_mobile_page_exp=$27,
    f1_wns_framework=$28,f2_quantified_outcome=$29,f3_wns_tool_platform=$30,
    cat_a_score=$31,cat_b_score=$32,cat_c_score=$33,cat_d_score=$34,cat_e_score=$35,cat_f_score=$36,
    total_score=$37,band=$38,priority_action=$39,
    http_status=$40,final_url=$41,last_crawled_at=NOW(),content_hash=$42,load_time_ms=$43,
    crawler_status='crawled',audit_date=CURRENT_DATE,auditor='crawler-v1'
    WHERE url=$44`,
  [scores.a1,scores.a2,scores.a3,scores.a4,scores.a5,scores.a6,scores.a7,
   scores.b1,scores.b2,scores.b3,scores.b4,scores.b5,scores.b6,
   scores.c1,scores.c2,scores.c3,scores.c4,scores.c5,scores.c6,scores.c7,
   scores.d1,scores.d2,scores.d3,scores.d4,scores.e1,scores.e2,scores.e3,
   scores.f1,scores.f2,scores.f3,scores.catA,scores.catB,scores.catC,scores.catD,scores.catE,scores.catF,
   scores.total,scores.band,scores.action,statusCode,finalUrl,hash,loadMs,url]);

  await db.query(`INSERT INTO wns_seo_crawl_history
    (url,http_status,load_time_ms,content_hash,a1_word_count,a2_freshness,a3_original_data,a4_depth_of_insight,a5_reading_level,a6_author_byline,a7_read_time_shown,b1_h1_tag,b2_meta_title,b3_meta_description,b4_url_slug,b5_internal_links,b6_schema_markup,c1_definition_block,c2_faq_section,c3_lists_tables,c4_answer_ready_passages,c5_named_entities,c6_speakable_content,c7_multimedia,d1_third_party_validation,d2_client_proof_points,d3_regulatory_refs,d4_industry_depth,e1_cta_quality,e2_related_content,e3_mobile_page_exp,f1_wns_framework,f2_quantified_outcome,f3_wns_tool_platform,cat_a_score,cat_b_score,cat_c_score,cat_d_score,cat_e_score,cat_f_score,total_score,band,priority_action)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40,$41,$42,$43)`,
  [url,statusCode,loadMs,hash,scores.a1,scores.a2,scores.a3,scores.a4,scores.a5,scores.a6,scores.a7,scores.b1,scores.b2,scores.b3,scores.b4,scores.b5,scores.b6,scores.c1,scores.c2,scores.c3,scores.c4,scores.c5,scores.c6,scores.c7,scores.d1,scores.d2,scores.d3,scores.d4,scores.e1,scores.e2,scores.e3,scores.f1,scores.f2,scores.f3,scores.catA,scores.catB,scores.catC,scores.catD,scores.catE,scores.catF,scores.total,scores.band,scores.action]);

  if (prevRow) {
    const delta = scores.total - prevRow.total_score;
    if (delta !== 0) await db.query("INSERT INTO wns_seo_change_log (url,change_type,field_name,old_value,new_value,score_delta,severity) VALUES ($1,$2,'total_score',$3,$4,$5,$6)",
      [url, delta>0?"seo_improvement":"seo_regression", String(prevRow.total_score), String(scores.total), delta, Math.abs(delta)>=10?"critical":Math.abs(delta)>=5?"warning":"info"]);
    if (prevRow.band !== scores.band) await db.query("INSERT INTO wns_seo_change_log (url,change_type,field_name,old_value,new_value,score_delta,severity) VALUES ($1,'band_change','band',$2,$3,$4,$5)",
      [url, prevRow.band, scores.band, delta, scores.band==="Critical"?"critical":scores.band==="Elite"?"info":"warning"]);
  }
}

function extract(url, html) {
  const $ = cheerio.load(html);
  const body = $("body").text().replace(/\s+/g," ").trim();
  const words = body.split(/\s+/);
  const paras = $("p").map((_,el)=>$(el).text().trim()).get().filter(t=>t.length>50);
  const first100 = words.slice(0,100).join(" ");

  const pubMeta = $('meta[property="article:published_time"]').attr("content") || $("time[datetime]").first().attr("datetime") || null;
  const modMeta = $('meta[property="article:modified_time"]').attr("content") || null;
  const pubDate = pubMeta ? new Date(pubMeta) : null;
  const modDate = modMeta ? new Date(modMeta) : null;

  const authorEl = $('[rel="author"]').text() || $('[class*="author"]').first().text() || $('meta[name="author"]').attr("content") || "";
  const host = new URL(url).host;
  const slug = new URL(url).pathname;
  const intLinks = $("a[href]").filter((_,el)=>{ const h=$(el).attr("href")||""; return h.startsWith("/")||h.includes(host); });
  const schemaTypes = $('script[type="application/ld+json"]').map((_,el)=>{ try{return JSON.parse($(el).html()||"{}")["@type"]||"";}catch{return "";} }).get().filter(Boolean);
  const faqQ = $("h2,h3,h4").map((_,el)=>$(el).text()).get().filter(t=>/\?$/.test(t.trim()));
  const sentences = body.split(/[.!?]/).filter(s=>s.trim().length>5);
  const avgWPS = words.length / (body.split(/[.!?]+/).length||1);
  const industryTerms = ["vertical","sector","underwriting","actuarial","procurement","claims","regulatory","supply chain"];
  const ctaBtns = $("a,button").map((_,el)=>$(el).text().trim()).get().filter(t=>t.length>1&&t.length<60);
  const contextualCTAs = ["download","request a demo","get assessment","schedule","watch now"];

  return {
    wc: words.length,
    pubDate, modDate,
    hasWNS: WNS_FRAMEWORKS.some(f=>body.includes(f)),
    has3P: ANALYST_SOURCES.some(s=>body.includes(s)),
    hasMulti: /challenge|approach|solution|outcome|result/gi.test(body) && paras.length>5,
    hasEx: /for example|case study|client|reduced|improved|achieved/gi.test(body),
    rdg: avgWPS>20&&words.length>800?"expert":avgWPS>14?"generic":"shallow",
    hasAuth: authorEl.trim().length>2,
    hasAuthCred: /senior|director|VP|head of|manager|analyst/gi.test(authorEl),
    hasAuthLink: $('[rel="author"][href]').length>0,
    hasRT: $('[class*="read-time"],[class*="reading-time"]').length>0,
    rtAcc: /\d+\s*(min|minute)/i.test($('[class*="read-time"]').text()),
    h1: $("h1").first().text().trim(),
    h1kw: (() => { const h1=$("h1").first().text().toLowerCase(); return slug.replace(/[-/]/g," ").split(/\s+/).some(w=>w.length>4&&h1.includes(w.toLowerCase())); })(),
    mtLen: $("title").text().trim().length,
    mt: $("title").text().trim(),
    mdLen: ($('meta[name="description"]').attr("content")||"").length,
    urlClean: /^\/[a-z0-9-/]+\/?$/.test(slug)&&slug.length<80,
    intCt: intLinks.length,
    intDesc: intLinks.map((_,el)=>$(el).text().trim()).get().some(t=>t.length>10&&!/click here|read more|here/i.test(t)),
    schema: [...new Set(schemaTypes)],
    hasDef: /is a |refers to |defined as |means /gi.test(first100),
    hasFAQ: schemaTypes.includes("FAQPage")||faqQ.length>=2,
    faqSelf: faqQ.length>=4,
    listCt: $("ul,ol").length,
    tblCt: $("table").length,
    ansPas: paras.filter(p=>{const wc=p.split(/\s+/).length;return wc>=40&&wc<=200;}).length,
    entCt: new Set([...WNS_FRAMEWORKS,...WNS_PRODUCTS,...ANALYST_SOURCES,...REGULATIONS].filter(n=>body.includes(n))).size,
    spkCt: sentences.filter(s=>{const wc=s.trim().split(/\s+/).length;return wc<=30&&wc>=5;}).length,
    hasVid: $("video,iframe[src*='youtube'],iframe[src*='vimeo']").length>0,
    hasInfog: $("img").map((_,el)=>$(el).attr("alt")||"").get().some(a=>/infographic|diagram|chart|framework/i.test(a)),
    alts: $("img").map((_,el)=>$(el).attr("alt")||"").get().filter(Boolean),
    extCt: $("a[href^='http']").not(`a[href*="${host}"]`).length,
    hasReport: ANALYST_SOURCES.some(s=>body.includes(s)),
    hasClient: /reduced|improved|saved|increased|achieved|delivered/gi.test(body),
    hasQtf: /\d+%|\$\d+[MBK]?|\d+x /gi.test(body),
    regDepth: (()=>{ const r=REGULATIONS.find(r=>body.includes(r)); return !r?"none":(body.includes("compliance")||body.includes("approach"))?"explained":"named"; })(),
    indScore: industryTerms.filter(t=>body.toLowerCase().includes(t)).length,
    ctaType: ctaBtns.some(t=>contextualCTAs.some(c=>t.toLowerCase().includes(c)))?"contextual":ctaBtns.some(t=>["contact us","learn more"].some(c=>t.toLowerCase().includes(c)))?"generic":"none",
    relCt: $('[class*="related"],[class*="recommended"]').find("a").length,
    noVp: $('meta[name="viewport"]').length===0,
    wnsFw: WNS_FRAMEWORKS.filter(f=>body.includes(f)),
    wnsProd: WNS_PRODUCTS.filter(p=>body.includes(p)),
    wnsLink: WNS_PRODUCTS.some(p=>body.includes(p))&&$("a[href]").map((_,el)=>$(el).attr("href")||"").get().some(h=>WNS_PRODUCTS.some(p=>h.toLowerCase().includes(p.toLowerCase()))),
  };
}

function score(p) {
  const now = Date.now();
  const age = (d: Date | null) => (d ? (now - d.getTime()) / (1000 * 60 * 60 * 24 * 30) : 999);
  const eff = p.modDate||p.pubDate;

  const a1=p.wc>=1000?2:p.wc>=400?1:0;
  const a2=age(eff)<=12?2:age(eff)<=24?1:0;
  const a3=p.hasWNS?2:p.has3P?1:0;
  const a4=p.hasMulti&&p.hasEx?2:p.hasEx?1:0;
  const a5=p.rdg==="expert"?2:p.rdg==="generic"?1:0;
  const a6=p.hasAuth&&(p.hasAuthCred||p.hasAuthLink)?2:p.hasAuth?1:0;
  const a7=p.rtAcc?2:p.hasRT?1:0;
  const b1=p.h1kw?2:p.h1.length>0?1:0;
  const b2=p.mtLen>0&&p.mtLen<=60?2:p.mtLen>0?1:0;
  const b3=p.mdLen>=140&&p.mdLen<=155?2:p.mdLen>0?1:0;
  const b4=p.urlClean?2:1;
  const b5=p.intCt>=3&&p.intDesc?2:p.intCt>=1?1:0;
  const rich=["FAQPage","HowTo","SpeakableSpecification"];
  const b6=p.schema.length>0&&rich.some(s=>p.schema.includes(s))?2:p.schema.length>0?1:0;
  const c1=p.hasDef?2:0;
  const c2=p.hasFAQ&&p.faqSelf?2:p.hasFAQ?1:0;
  const c3=p.listCt>=2&&p.tblCt>=1?2:p.listCt>=1||p.tblCt>=1?1:0;
  const c4=p.ansPas>=3?2:p.ansPas>=1?1:0;
  const c5=p.entCt>=5?2:p.entCt>=1?1:0;
  const c6=p.spkCt>=2?2:p.spkCt>=1?1:0;
  const c7=(p.hasVid||p.hasInfog)&&p.alts.length>0?2:p.alts.length>0?1:0;
  const d1=p.hasReport?2:p.extCt>0?1:0;
  const d2=p.hasQtf&&p.hasClient?2:p.hasClient?1:0;
  const d3=p.regDepth==="explained"?2:p.regDepth==="named"?1:0;
  const d4=p.indScore>=4?2:p.indScore>=1?1:0;
  const e1=p.ctaType==="contextual"?2:p.ctaType==="generic"?1:0;
  const e2=p.relCt>=2?2:p.relCt>=1?1:0;
  const e3=p.noVp?0:2;
  const f1=p.wnsFw.length>0?2:0;
  const f2=p.hasQtf&&p.hasWNS?2:p.hasQtf?1:0;
  const f3=p.wnsProd.length>0&&p.wnsLink?2:p.wnsProd.length>0?1:0;

  const catA=a1+a2+a3+a4+a5+a6+a7;
  const catB=b1+b2+b3+b4+b5+b6;
  const catC=c1+c2+c3+c4+c5+c6+c7;
  const catD=d1+d2+d3+d4;
  const catE=e1+e2+e3;
  const catF=f1+f2+f3;
  const total=catA+catB+catC+catD+catE+catF;
  const band=total>=50?"Elite":total>=40?"Strong":total>=28?"Average":total>=15?"Weak":"Critical";
  const action=band==="Elite"?"No action":band==="Strong"?"Targeted refresh":band==="Average"?"Content + SEO refresh":band==="Weak"?"Full rewrite + restructure":"Archive or redirect";

  return {a1,a2,a3,a4,a5,a6,a7,b1,b2,b3,b4,b5,b6,c1,c2,c3,c4,c5,c6,c7,d1,d2,d3,d4,e1,e2,e3,f1,f2,f3,catA,catB,catC,catD,catE,catF,total,band,action};
}

function sleep(ms) { return new Promise(r=>setTimeout(r,ms)); }

main().catch(e => { console.error(e); process.exit(1); });
