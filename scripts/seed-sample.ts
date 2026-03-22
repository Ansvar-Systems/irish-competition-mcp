/**
 * Seed the CCPC (Ireland) database with sample decisions, mergers, and sectors.
 *
 * Usage:
 *   npx tsx scripts/seed-sample.ts
 *   npx tsx scripts/seed-sample.ts --force
 */

import Database from "better-sqlite3";
import { existsSync, mkdirSync, unlinkSync } from "node:fs";
import { dirname } from "node:path";
import { SCHEMA_SQL } from "../src/db.js";

const DB_PATH = process.env["CCPC_DB_PATH"] ?? "data/ccpc.db";
const force = process.argv.includes("--force");

const dir = dirname(DB_PATH);
if (!existsSync(dir)) { mkdirSync(dir, { recursive: true }); }
if (force && existsSync(DB_PATH)) { unlinkSync(DB_PATH); console.log(`Deleted existing database at ${DB_PATH}`); }

const db = new Database(DB_PATH);
db.pragma("journal_mode = WAL");
db.pragma("foreign_keys = ON");
db.exec(SCHEMA_SQL);
console.log(`Database initialised at ${DB_PATH}`);

interface SectorRow { id: string; name: string; name_en: string; description: string; decision_count: number; merger_count: number; }

const sectors: SectorRow[] = [
  { id: "grocery", name: "Grocery Retail", name_en: "Grocery Retail",
    description: "Grocery retail, supermarkets, convenience stores, and food supply chains in Ireland.", decision_count: 2, merger_count: 1 },
  { id: "financial_services", name: "Financial Services", name_en: "Financial Services",
    description: "Banking, insurance, payment services, and financial market infrastructure in Ireland.", decision_count: 1, merger_count: 1 },
  { id: "telecommunications", name: "Telecommunications", name_en: "Telecommunications",
    description: "Mobile, broadband, fixed-line, and telecommunications infrastructure in Ireland.", decision_count: 1, merger_count: 2 },
  { id: "healthcare", name: "Healthcare", name_en: "Healthcare",
    description: "Hospitals, pharmaceuticals, medical devices, and health insurance in Ireland.", decision_count: 1, merger_count: 1 },
  { id: "digital_economy", name: "Digital Economy", name_en: "Digital Economy",
    description: "Online platforms, digital marketplaces, and technology services in Ireland.", decision_count: 2, merger_count: 1 },
  { id: "energy", name: "Energy", name_en: "Energy",
    description: "Electricity, gas, and renewable energy generation, transmission, and supply in Ireland.", decision_count: 1, merger_count: 1 },
  { id: "media", name: "Media", name_en: "Media",
    description: "Broadcasting, print, digital media, and news services in Ireland.", decision_count: 1, merger_count: 0 },
  { id: "construction", name: "Construction", name_en: "Construction",
    description: "Construction materials, building services, and property development in Ireland.", decision_count: 1, merger_count: 0 },
];

const insertSector = db.prepare("INSERT OR IGNORE INTO sectors (id, name, name_en, description, decision_count, merger_count) VALUES (?, ?, ?, ?, ?, ?)");
for (const s of sectors) { insertSector.run(s.id, s.name, s.name_en, s.description, s.decision_count, s.merger_count); }
console.log(`Inserted ${sectors.length} sectors`);

interface DecisionRow { case_number: string; title: string; date: string; type: string; sector: string; parties: string; summary: string; full_text: string; outcome: string; fine_amount: number | null; gwb_articles: string; status: string; }

const decisions: DecisionRow[] = [
  {
    case_number: "CCPC/M/22/001",
    title: "Insurance Ireland — Cartel investigation into motor insurance information exchange",
    date: "2022-06-15", type: "cartel", sector: "financial_services",
    parties: JSON.stringify(["Insurance Ireland", "Multiple motor insurance providers"]),
    summary: "The CCPC investigated Insurance Ireland and its members for participation in an information exchange arrangement concerning motor insurance data that could facilitate coordination on pricing. The investigation followed a referral from the Central Bank of Ireland.",
    full_text: "The Competition and Consumer Protection Commission (CCPC) conducted an investigation under Section 4 of the Competition Act 2002 into Insurance Ireland, the representative body for insurance companies in Ireland, and a number of its member undertakings. The investigation concerned an information exchange arrangement, specifically the Insurance Link database, through which member insurers shared detailed claims and motor insurance data. The CCPC examined whether this data exchange facilitated coordination or harmonisation of pricing decisions among competing insurers in the Irish motor insurance market. The Irish motor insurance market is concentrated, with a small number of insurers holding significant market shares. The CCPC found that certain aspects of the Insurance Link system involved the sharing of competitively sensitive information. Insurance Ireland agreed to implement modifications to its data sharing arrangements. The case reflects broader European enforcement trends around algorithmic pricing and data-facilitated coordination in the insurance sector.",
    outcome: "cleared_with_conditions", fine_amount: null,
    gwb_articles: JSON.stringify(["Section 4 Competition Act 2002", "Article 101 TFEU"]), status: "final",
  },
  {
    case_number: "CCPC/M/23/002",
    title: "Grocery sector — Retail price signalling investigation",
    date: "2023-03-20", type: "abuse_of_dominance", sector: "grocery",
    parties: JSON.stringify(["Major Irish grocery retailer"]),
    summary: "The CCPC examined price signalling practices in the Irish grocery retail sector following consumer complaints about coordinated pricing behaviour. The investigation assessed whether publicly announced price commitments constituted anticompetitive coordination.",
    full_text: "The CCPC initiated an investigation into pricing practices in the Irish grocery retail sector following an increase in consumer complaints and media coverage of apparent price coordination among major grocery retailers. The CCPC examined whether public price announcements and commitments by major grocery chains constituted anticompetitive price signalling under Section 4 of the Competition Act 2002 and Article 101 TFEU. The Irish grocery market is an oligopoly dominated by Dunnes Stores, Tesco Ireland, Lidl Ireland, Aldi Ireland, and SuperValu. The CCPC assessed whether public statements about pricing strategies, particularly during the cost of living crisis period in 2022-2023, could have served as signals to competitors rather than genuine consumer commitments. The investigation also examined loyalty programme data sharing and category management arrangements with suppliers. The CCPC issued guidance to the grocery sector on permissible competitor information exchange and committed to ongoing monitoring of the sector.",
    outcome: "cleared", fine_amount: null,
    gwb_articles: JSON.stringify(["Section 4 Competition Act 2002", "Article 101 TFEU"]), status: "final",
  },
  {
    case_number: "CCPC/M/22/003",
    title: "Technology sector — Dominance in online advertising markets",
    date: "2022-11-08", type: "abuse_of_dominance", sector: "digital_economy",
    parties: JSON.stringify(["Online advertising platform operator"]),
    summary: "The CCPC opened a market study into digital advertising markets in Ireland, including the intermediation between online publishers and advertisers. The study examined market structure, barriers to entry, and data advantages of dominant platforms.",
    full_text: "The Competition and Consumer Protection Commission initiated a market study into digital advertising markets in Ireland under Section 10 of the Competition and Consumer Protection Act 2014. Ireland is a significant hub for digital advertising given the European headquarters of major technology companies in Dublin. The study focused on programmatic advertising, the real-time bidding ecosystem, and the role of intermediaries including ad servers, ad exchanges, supply-side platforms (SSPs), and demand-side platforms (DSPs). Key findings: (1) The market is characterised by significant vertical integration of the leading platform — controlling both publisher-facing and advertiser-facing intermediation tools. (2) Advertisers and publishers face high switching costs and lack of transparency in fee structures and auction dynamics. (3) Data advantages of integrated platforms create barriers to entry for independent intermediaries. (4) The complexity of the supply chain makes it difficult for publishers to verify charges and understand where revenue is lost. The CCPC published its findings and recommended enhanced transparency obligations, interoperability requirements, and data access rights for competitors. The study informed Ireland's approach to enforcement under the EU Digital Markets Act.",
    outcome: "cleared", fine_amount: null,
    gwb_articles: JSON.stringify(["Section 10 CCPA 2014", "Digital Markets Act"]), status: "final",
  },
  {
    case_number: "CCPC/M/21/004",
    title: "Construction materials — Cement price coordination",
    date: "2021-09-14", type: "cartel", sector: "construction",
    parties: JSON.stringify(["Irish Cement Ltd", "Cement Roadstone Holdings"]),
    summary: "The CCPC imposed fines following an investigation into price coordination between cement suppliers in Ireland. The investigation found evidence of communications between competitors concerning pricing decisions in the commercial construction sector.",
    full_text: "The Competition and Consumer Protection Commission concluded an investigation into price coordination in the Irish cement market. Following an inspection under Section 37 of the Competition and Consumer Protection Act 2014, the CCPC found evidence that senior representatives of the two main cement suppliers in Ireland had engaged in communications concerning pricing intentions for commercial and industrial construction customers. The Irish cement market is highly concentrated due to the capital-intensive nature of cement production and the geographic barriers to importing bulk cement. The two investigated parties hold market shares that together exceed 90% of the domestic Irish cement market. The CCPC found that the communications concerned future pricing decisions for large commercial customers — a clear violation of Section 4 of the Competition Act 2002 and Article 101 TFEU. Financial penalties were imposed. The parties were also required to implement compliance programmes including competition law training for commercial and executive staff. The case underlines the CCPC's focus on construction sector markets, which are significant for Irish infrastructure investment.",
    outcome: "fine", fine_amount: 4_000_000,
    gwb_articles: JSON.stringify(["Section 4 Competition Act 2002", "Article 101 TFEU"]), status: "final",
  },
  {
    case_number: "CCPC/M/24/001",
    title: "Digital healthcare — Market study in online pharmacy and health platforms",
    date: "2024-01-25", type: "sector_inquiry", sector: "healthcare",
    parties: JSON.stringify(["Online pharmacy operators", "Digital health platforms"]),
    summary: "The CCPC launched a sector inquiry into digital healthcare markets in Ireland, covering online pharmacies, digital health consultations, and health data aggregation platforms. The inquiry assesses competition dynamics and consumer protection implications.",
    full_text: "The Competition and Consumer Protection Commission launched a sector inquiry into digital healthcare markets in Ireland. The inquiry covers: (1) Online pharmacy services — the growth of online-only pharmacies and hybrid pharmacy models, and implications for competition with traditional pharmacies. (2) Digital health consultation platforms — the regulation and competition dynamics of telemedicine and online GP services. (3) Health data aggregation — the role of platforms that aggregate patient data and health metrics, and whether data advantages create market power. The inquiry is relevant to both competition law (market structure, barriers to entry, vertical integration) and consumer protection law (data practices, price transparency, health claims). Ireland is a significant location for digital health companies with European operations. The CCPC is working with the Health Products Regulatory Authority (HPRA) and the Data Protection Commission (DPC) on aspects of the inquiry touching on pharmaceutical regulation and data protection.",
    outcome: "cleared", fine_amount: null,
    gwb_articles: JSON.stringify(["Section 10 CCPA 2014"]), status: "ongoing",
  },
];

const insertDecision = db.prepare("INSERT OR IGNORE INTO decisions (case_number, title, date, type, sector, parties, summary, full_text, outcome, fine_amount, gwb_articles, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
const insertDecisionsAll = db.transaction(() => { for (const d of decisions) { insertDecision.run(d.case_number, d.title, d.date, d.type, d.sector, d.parties, d.summary, d.full_text, d.outcome, d.fine_amount, d.gwb_articles, d.status); } });
insertDecisionsAll();
console.log(`Inserted ${decisions.length} decisions`);

interface MergerRow { case_number: string; title: string; date: string; sector: string; acquiring_party: string; target: string; summary: string; full_text: string; outcome: string; turnover: number | null; }

const mergers: MergerRow[] = [
  {
    case_number: "CCPC/M/22/M/001",
    title: "Eir / Emerald Communications — Telecommunications merger",
    date: "2022-04-12", sector: "telecommunications",
    acquiring_party: "Eir", target: "Emerald Communications (Virgin Media Business commercial customers)",
    summary: "The CCPC approved the acquisition by Eir of selected commercial customer contracts from Virgin Media Business. Phase 1 clearance was granted following an assessment of the Irish business telecommunications market.",
    full_text: "The Competition and Consumer Protection Commission reviewed the proposed acquisition by Eir of a portfolio of commercial customer contracts from Virgin Media Business. Eir is Ireland's largest telecommunications operator, providing mobile, broadband, and fixed-line services to both residential and business customers. The transaction concerned the transfer of selected small and medium enterprise (SME) business telecommunications contracts. The CCPC assessed the transaction in the context of the Irish business telecommunications market. Key considerations: (1) The business telecommunications market in Ireland includes Eir, Virgin Media Business, Vodafone, BT Ireland, and several managed service providers. (2) The acquired customer base represents a limited share of the total market. (3) Competition from alternative providers remains sufficient in the relevant market segments. The CCPC cleared the transaction in Phase 1 without conditions, finding that it would not substantially lessen competition in any market for goods or services in the State.",
    outcome: "cleared_phase1", turnover: 800_000_000,
  },
  {
    case_number: "CCPC/M/23/M/002",
    title: "Musgrave Group / Centra franchise expansion — Grocery sector consolidation",
    date: "2023-07-18", sector: "grocery",
    acquiring_party: "Musgrave Group", target: "Independent Centra franchisee stores",
    summary: "The CCPC reviewed Musgrave Group's acquisition of independent Centra franchise stores in multiple locations. The transaction was cleared with conditions requiring divestiture of stores in local markets where combined market shares raised competition concerns.",
    full_text: "The Competition and Consumer Protection Commission reviewed the proposed acquisition by Musgrave Group plc of a number of independently operated Centra franchise convenience stores across multiple Irish locations. Musgrave Group operates the Centra, SuperValu, and Daybreak franchise networks in Ireland and is one of the largest grocery wholesale and retail operators in the State. The CCPC assessed the transaction using grocery retail market definitions that are local in nature — convenience grocery retail is typically defined on a geographic basis of a 5-10 minute drive time. In several local markets, the acquisition of independently operated stores would result in Musgrave having control of multiple competing outlets, raising horizontal overlap concerns. The CCPC applied the Substantial Lessening of Competition (SLC) test as required by the Competition Act 2002. Following detailed analysis, the CCPC approved the transaction subject to conditions: Musgrave was required to divest three stores in specific locations where local market concentration was found to be problematic. The divestiture conditions ensure ongoing competition in those local convenience grocery markets.",
    outcome: "cleared_with_conditions", turnover: 4_500_000_000,
  },
  {
    case_number: "CCPC/M/23/M/003",
    title: "Blackstone / Hilton Dublin Hotels — Hospitality sector",
    date: "2023-10-05", sector: "financial_services",
    acquiring_party: "Blackstone Real Estate", target: "Hilton Dublin Collection (3 properties)",
    summary: "The CCPC approved Blackstone's acquisition of three Hilton-branded hotel properties in Dublin. Phase 1 clearance was granted as the hospitality market in Dublin has sufficient alternative accommodation providers.",
    full_text: "The Competition and Consumer Protection Commission reviewed the proposed acquisition by Blackstone Real Estate Partners of a portfolio of three Hilton-branded hotel properties in Dublin. Blackstone is a global investment firm with a significant portfolio of hospitality assets in Europe. The three properties concerned are the Hilton Dublin, Hilton Dublin Kilmainham, and Doubletree by Hilton Dublin Burlington Road, comprising approximately 1,200 rooms in total. The CCPC assessed the transaction in the Dublin hotel accommodation market. Dublin has a concentrated supply of premium hotel rooms in certain locations, with significant capacity constraints. However, the CCPC found that the market includes a broad range of accommodation providers including hotels, serviced apartments, and short-term rental platforms. The transaction does not create horizontal overlaps as Blackstone did not previously own competing hotel properties in the same segments in Dublin. The CCPC cleared the transaction in Phase 1 without conditions.",
    outcome: "cleared_phase1", turnover: 2_000_000_000,
  },
];

const insertMerger = db.prepare("INSERT OR IGNORE INTO mergers (case_number, title, date, sector, acquiring_party, target, summary, full_text, outcome, turnover) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
const insertMergersAll = db.transaction(() => { for (const m of mergers) { insertMerger.run(m.case_number, m.title, m.date, m.sector, m.acquiring_party, m.target, m.summary, m.full_text, m.outcome, m.turnover); } });
insertMergersAll();
console.log(`Inserted ${mergers.length} mergers`);

const decisionCount = (db.prepare("SELECT count(*) as cnt FROM decisions").get() as { cnt: number }).cnt;
const mergerCount = (db.prepare("SELECT count(*) as cnt FROM mergers").get() as { cnt: number }).cnt;
const sectorCount = (db.prepare("SELECT count(*) as cnt FROM sectors").get() as { cnt: number }).cnt;
console.log("\nDatabase summary:");
console.log(`  Sectors:    ${sectorCount}`);
console.log(`  Decisions:  ${decisionCount}`);
console.log(`  Mergers:    ${mergerCount}`);
console.log(`\nDone. Database ready at ${DB_PATH}`);
db.close();
