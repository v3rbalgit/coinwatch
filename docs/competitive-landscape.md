# Competitive landscape and positioning

> Research snapshot: 2026-08-27. Product capabilities and prices change quickly. Facts below come
> from public product pages and official documentation; strategic assessments are explicitly
> separated from vendor claims.

## Purpose

Coinwatch is a Hyperliquid-first, single-user mission-control screener. Its intended workflow is:

1. Express a repeatable opportunity as a structured signal.
2. Understand why markets do or do not match it now.
3. Validate past matched states causally on the chart.
4. Activate a completed-candle, false-to-true alert.
5. Eventually publish, inspect, fork, and follow trustworthy signal definitions.

The competitive question is not whether other products have charts, screeners, or alerts. Many do.
The question is whether another product combines explainable no-code signal authoring, current
cross-market diagnostics, causal historical validation, reliable alert lifecycle semantics, and
social collaboration around versioned definitions. The reviewed market is fragmented across those
jobs.

## Executive assessment

TradingView is the strongest horizontal competitor and the product-quality benchmark. Buildix is
the strongest Hyperliquid-native breadth and value competitor. Hyperdash owns wallet discovery,
copy trading, and execution. Velo, CoinGlass, Laevitas, Hyblock, and ASXN primarily compete for
analytics attention. Altrady is a useful no-code scanner benchmark even though Hyperliquid support
was not established by the reviewed public material.

No reviewed product clearly owns **inspectable, versioned personal signals that move through one
consistent author → diagnose → validate → alert → share workflow**. That is Coinwatch's best
opportunity. It should not attempt to win by copying every order-flow panel or becoming a smaller
TradingView.

## Market map

| Segment                                   | Representative products            | Principal job                                                                    |
| ----------------------------------------- | ---------------------------------- | -------------------------------------------------------------------------------- |
| Universal charting and scripting          | TradingView                        | Chart any market, write indicators, screen watchlists, run cloud alerts          |
| Hyperliquid order-flow terminal           | Buildix                            | Scan order flow, whales, liquidations, funding, and cross-exchange metrics       |
| Hyperliquid wallet and execution terminal | Hyperdash                          | Discover traders, track wallets, copy trades, inspect liquidation zones, execute |
| Derivatives analytics                     | Velo, CoinGlass, Laevitas, Hyblock | Funding, OI, liquidations, order books, positioning, dashboards and APIs         |
| Hyperliquid ecosystem analytics           | ASXN Hyperscreener                 | Protocol, market, builder, trader, and ecosystem monitoring                      |
| No-code multi-exchange scanning           | Altrady                            | Build technical conditions, scan connected exchanges, alert, then execute        |
| Explainable personal-signal workflow      | Coinwatch opportunity              | Define, diagnose, validate, alert, version, publish, and fork signals            |

## Competitor profiles

### TradingView

#### Validated capabilities

- TradingView added Hyperliquid and Trade[XYZ] market feeds on 2026-07-02. Hyperliquid markets are
  discoverable with the `HYPERLIQUID:` prefix.
- Pine Screener scans a custom watchlist or index using one built-in, personal, invite-only, or
  community Pine indicator.
- Pine Screener supports common intraday through monthly timeframes, up to five `request.*()` calls,
  and calculations over the last 500 bars.
- TradingView alerts support price, indicators, strategies, drawings, and watchlists. Delivery can
  use app notifications, desktop notifications, email, sound, SMS routing, or webhooks.
- Alert instances retain the indicator settings captured when the alert was created.
- TradingView has an established public script and social-ideas ecosystem.

#### Strengths

- Best-in-class charting polish, market breadth, mobile reach, and distribution.
- Pine is substantially more expressive than a constrained visual builder.
- Mature cloud alert infrastructure and community content.
- A user can approximate much of Coinwatch's technical-analysis workflow with sufficient Pine
  skill and several TradingView surfaces.

#### Weaknesses and Coinwatch opportunity

- Bespoke logic generally requires Pine rather than a guided, domain-aware visual model.
- Screening, chart inspection, script authoring, and alert management are related but separate
  workflows.
- Pine Screener's one-script, request-count, and 500-bar constraints shape complex multi-timeframe
  scans.
- TradingView is market-general rather than opinionated around Hyperliquid data health, completed-bar
  causality, signal diagnostics, or false-to-true lifecycle state.

**Competitive status:** direct horizontal competitor; benchmark its usability and reliability, not
its total scope.

### Buildix

#### Validated public claims

Buildix's public product page advertises:

- 530+ Hyperliquid pairs and 149+ HIP-3 markets.
- A free market screener; paid plans starting at $9/month.
- Pair-level panels including CVD, OBI, OFI, VPIN, footprints, volume/TPO profiles, order-book and
  liquidation heatmaps, smart-money activity, funding, OI, and cross-exchange comparisons.
- Six prebuilt order-flow entry-signal logics, signal alerts, and Telegram/email delivery.
- A custom alert engine with AND/OR filters over metrics such as VPIN, volume, funding, and OBI.
- Wallet tracking, liquidation maps, carry analytics, AI querying, APIs, and webhooks on higher tiers.

These are vendor claims and were not independently benchmarked for latency, correctness, coverage,
or long-term availability.

#### Strengths

- Exceptional feature breadth for its price.
- Hyperliquid-native order-flow depth rather than generic exchange adaptation.
- Strong free acquisition surface and inexpensive upgrades.
- Integrates screeners, pair analysis, wallets, liquidations, alerts, education, and APIs.
- Makes specialist microstructure metrics accessible without requiring users to build the data
  pipeline themselves.

#### Weaknesses and Coinwatch opportunity

- Breadth and information density can make the product cognitively expensive; this must be tested
  hands-on rather than assumed from screenshots.
- Public positioning emphasizes order-flow analytics and prebuilt signal logic more than transparent,
  user-owned definition versioning and condition-by-condition diagnostics.
- Public material reviewed here does not prominently document a social model for publishing,
  inspecting, forking, and forward-tracking user-authored definitions.
- Competing feature-for-feature or on price would be strategically weak for Coinwatch.

**Competitive status:** strongest direct Hyperliquid product threat and the most important hands-on
benchmark.

### Hyperdash

#### Validated capabilities

- Hyperliquid-specific whale tracking, liquidation heatmaps, smart-money flows, trader rankings,
  portfolio analytics, and advanced order execution.
- Copy trading and counter-trading around ranked wallets.
- Whale, wallet-activity, and liquidation-zone alerts.
- Trader discovery by PnL, equity, win rate, Sharpe ratio, drawdown, leverage, trading style, and
  traded assets.
- Tracked-wallet lists can be shared by link and cloned by another user.

#### Strengths

- Clear identity around observing and acting on other traders' behavior.
- Deep wallet/transparency advantage from Hyperliquid's public state.
- Strong execution loop and compelling copy-trading acquisition hook.
- Shareable wallet lists are a useful example of a lightweight social artifact.

#### Weaknesses and Coinwatch opportunity

- The core object is a trader or wallet, not a user-authored technical signal.
- Its alerts concern wallet actions, whales, and liquidation zones rather than explainable
  multi-timeframe condition trees.
- Copy-trading incentives and risks differ materially from decision-support signal authoring.

**Competitive status:** competes for the same trader's desktop, but occupies a different primary
workflow.

### Velo

#### Validated capabilities

Velo's official documentation lists:

- Charting for 3,400+ symbols across exchanges including Hyperliquid and Trade[XYZ].
- Open-interest and funding indicators.
- Order-book heatmaps and Hyperliquid TP/SL/liquidation heatmaps.
- Alerts for price, open interest, volume, and liquidations.
- Hyperliquid and Bybit execution plus SDK/API access.

#### Strengths

- Broad derivatives coverage with a relatively clean data-first presentation.
- Useful metric alerts and direct Hyperliquid support.
- API and SDK make it useful as both an application and data source.

#### Weaknesses and Coinwatch opportunity

- The reviewed documentation presents metric dashboards and alerts, not a rich personal-signal
  authoring and historical-evidence workflow.
- Its identity is less immediately legible than the focused narratives of Buildix or Hyperdash.
- It is best treated as a derivatives-data and alerting benchmark rather than the exact product to
  emulate.

**Competitive status:** adjacent analytics competitor with partial alerting overlap.

### Altrady

#### Validated capabilities

- A no-code technical-analysis scanner with 100+ indicators.
- Custom AND/OR rules, preset strategies, market-wide monitoring, and real-time alerts.
- On-screen, sound, and push notification delivery.
- Chart-to-bracket-order workflows, bots, paper trading, portfolios, and multi-exchange execution.
- Public pricing starts around €28/month before annual discounts.

The reviewed product page advertises 19+ exchanges but does not establish native Hyperliquid support,
so this document does not claim it.

#### Strengths

- Strong proof that traders value visual condition builders and continuous technical scanning.
- Large indicator catalog and integrated execution.
- Useful benchmark for onboarding, templates, alert controls, and conversion from signal to order.

#### Weaknesses and Coinwatch opportunity

- Broad multi-exchange trading-suite positioning rather than a Hyperliquid-native mission-control
  workflow.
- The reviewed material emphasizes trigger speed and execution more than causal historical
  validation or explainable non-match diagnostics.
- Its breadth introduces portfolio, bot, and exchange-management complexity that Coinwatch need not
  copy.

**Competitive status:** workflow benchmark and substitute for no-code users, not yet a verified
Hyperliquid-direct competitor.

### ASXN Hyperscreener

#### Validated capabilities

Hyperliquid's official tools directory identifies ASXN as its most comprehensive analytics tool. Its
public description covers perps, spot, HIP-3 markets, HyperEVM, protocol revenue, builder codes, and
top traders.

#### Strengths

- Broad Hyperliquid ecosystem visibility.
- Strong protocol-level and market-structure context.
- Useful benchmark for universe coverage and ecosystem navigation.

#### Weaknesses and Coinwatch opportunity

- Public positioning is analytics and monitoring rather than personal signal creation.
- It answers “what is happening across Hyperliquid?” more directly than “does my repeatable setup
  hold, why, and should it notify me?”

**Competitive status:** adjacent ecosystem intelligence.

### CoinGlass, Laevitas, and Hyblock

These products overlap but are better understood as a derivatives-intelligence category:

- **CoinGlass** provides cross-market funding, OI, liquidation, long/short, order-flow, and
  Hyperliquid whale/wallet monitoring. Its Hyperliquid pages expose large positions, activity,
  liquidations, ratios, and wallet analytics.
- **Laevitas** covers real-time and historical perpetuals, futures, options, funding, OI,
  liquidations, order books, dashboards, and institutional APIs across major venues including
  Hyperliquid.
- **Hyblock** markets customizable terminals, liquidation and liquidity analytics, screeners,
  statistical analysis, alerts, and backtesting tools.

#### Shared strengths

- Much broader derivatives data than Coinwatch currently owns.
- Strong cross-exchange context and specialist datasets.
- Existing APIs could be more economical than building every data source internally.

#### Shared weaknesses and Coinwatch opportunity

- Their primary object is a metric, dashboard, or dataset rather than a versioned personal signal.
- Generic derivatives breadth can weaken Hyperliquid-specific workflow coherence.
- Coinwatch can consume selected data concepts without becoming another dashboard collection.

**Competitive status:** data competitors, possible suppliers, and feature benchmarks.

## Coinwatch's current advantages

Coinwatch already has a coherent technical foundation for a differentiated product:

- Hyperliquid-first universe, metadata, candle health, and persistent local history.
- Structured no-code definitions with domain compatibility and plain-language previews.
- Current universe/watchlist scans and causal historical validation through the same evaluator.
- Exact completed-candle and divergence-confirmation semantics.
- Condition evidence, three-valued data failures, and honest closest-candidate diagnostics.
- One inspectable definition artifact instead of opaque precomputed calls.
- Local-first privacy and ownership for private signals.

These are not yet a market moat. They become one when paired with reliable standing alerts,
excellent UX, useful additional data, and a network of inspectable signal artifacts.

## Product strategy

### Do not compete on dashboard breadth

Buildix already demonstrates how hard breadth-plus-price is to beat. Coinwatch should add a data
source only when it can participate in the complete workflow:

```text
chart → condition builder → current scan → historical validation → standing alert → shared evidence
```

A heatmap that cannot become a condition or explain an alert is lower priority than a smaller data
source integrated end to end.

### Close the private workflow first

The next milestone remains completed-candle standing alerts. Before social expansion, the private
experience should make this loop excellent:

1. Create or adapt a signal quickly.
2. Understand every operand and timeframe.
3. Diagnose non-matches without guesswork.
4. Validate expected historical occurrences.
5. Activate an alert snapshot confidently.
6. Return from a notification to the exact market and evidence.

UX polish is strategic, not cosmetic. Dense competitors create room for a calmer hierarchy, better
language, progressive disclosure, strong defaults, and fewer context switches.

### Add data in signal-compatible order

Recommended sequence:

1. Historical funding rates.
2. Historical open interest.
3. Liquidations and liquidation intensity.
4. CVD and aggressive buy/sell flow.
5. Order-book imbalance and liquidity changes.
6. Wallet or cohort activity.

Each addition needs stable history semantics, exact warm-up requirements where applicable, chart
representation, evidence formatting, validation behavior, and alert scheduling. Avoid repeating the
VWAP/OBV mistake of exposing a source before its history anchor is trustworthy.

### Build social around artifacts, not a generic feed

The market is not devoid of social features: TradingView has a mature community, and Hyperdash has
shareable wallet lists. The narrower gap is collaboration around **transparent signal definitions**.

The durable social object should be an immutable signal version containing:

- Structured definition and plain-language preview.
- Author, creation time, source version, and attribution.
- Historical validation context clearly separated from live results.
- Forward-only trigger records created after publication.
- Condition evidence for each public trigger.
- Fork lineage and explicit version updates.
- Public, unlisted, and private visibility.

Useful social features can then grow around that object:

1. Share or import an unlisted definition.
2. Publish an immutable version.
3. Fork/remix with attribution.
4. Curate collections or “signal packs.”
5. Follow authors or definitions.
6. Discover signals by market, timeframe, concept, or data source.
7. Comment only after the underlying artifact and provenance are trustworthy.

A generic post feed should come late, if at all. It is easy to build and difficult to keep useful.

### Make trust the network effect

Public trading ideas are vulnerable to hindsight edits, cherry-picked ranges, overfitting, and vague
claims. Coinwatch should distinguish:

- **Historically validated:** replayed against data that already existed.
- **Published:** immutable definition version created at a known time.
- **Live observed:** trigger recorded after publication.
- **Forked:** derived transparently from another published version.

Do not rank authors primarily by simulated return. Prefer transparent provenance, live observation
history, alert reliability, definition clarity, and user curation. Any eventual performance metric
requires explicit execution assumptions and strong anti-gaming design.

## Positioning

### Category

A useful category description is:

> **Hyperliquid-native personal signal and alerting workspace**

A stronger product promise is:

> Define it. Understand it. Validate it. Let Coinwatch watch it.

Possible supporting language:

- Explainable signals, not black-box calls.
- Your setup, across the whole Hyperliquid market.
- Completed-candle alerts you can inspect and trust.
- Publish definitions people can verify, version, and fork.

Coinwatch should not position itself as a smaller TradingView, a cheaper Buildix, a copy-trading
terminal, or an automated trading system.

### Initial audience

The most plausible initial user is a discretionary Hyperliquid trader who:

- Repeats technical or positioning-based setups.
- Watches several markets or timeframes.
- Does not want to maintain Pine scripts or bot infrastructure.
- Wants evidence and control rather than opaque signal calls.
- Values alerts but still makes the final decision manually.

A later audience is the analyst, educator, or community contributor who wants to publish a precise,
inspectable setup rather than a screenshot and vague caption.

### Eventual go-to-market, not a current milestone

Marketing should begin only after alerts are reliable, onboarding is polished, and a shared signal
link produces a credible first experience. Possible future channels:

- Shareable signal and live-trigger pages as the organic growth loop.
- Carefully documented built-in signal templates.
- Technical explainers about completed-candle causality and honest validation.
- Hyperliquid community participation on X, Discord, and relevant forums.
- Partnerships with analysts who publish transparent definitions rather than calls.
- Public product comparisons based on reproducible workflows, not unsupported superiority claims.

A freemium model is plausible but undecided: private local workflows could remain generous, while
hosted alerting, longer retention, premium data, and social publishing could fund server costs. Do
not anchor pricing to undercut Buildix; compete on trust, workflow, and community value.

### Naming

“Coinwatch” is adequate while the product remains private and exploratory. Naming should be revisited
only after the category and social object are proven. Renaming before product-market clarity creates
cost without differentiation.

## Risks

- **Feature sprawl:** copying competitors can bury the coherent author-to-alert workflow.
- **Cold start:** social discovery is empty without strong seed definitions and authors.
- **Trust abuse:** hindsight edits, copied work, misleading claims, and spam need provenance and
  moderation.
- **Infrastructure jump:** accounts, hosted evaluation, Postgres, moderation, and multi-device
  notifications materially change the local architecture.
- **Data cost and semantics:** more sources introduce licensing, retention, alignment, and causality
  concerns.
- **Security and regulatory surface:** execution, custody, copy trading, and performance marketing
  introduce obligations that decision support avoids.
- **Local/cloud tension:** social and always-on alerts should not destroy private-by-default local
  ownership.

## Recommended roadmap influence

This research does not replace the product roadmap. It sharpens priorities:

1. Complete and polish standing alerts.
2. Add funding/OI history as first-class signal sources.
3. Ship private/unlisted signal sharing with immutable versions.
4. Add forward trigger records and fork attribution.
5. Add public profiles, collections, follows, and discovery.
6. Expand order-flow and wallet data only where it integrates end to end.

## Research method and limitations

- Reviewed official product pages, official documentation, and Hyperliquid's tools directory on
  2026-08-27.
- Vendor capability, scale, and pricing claims are recorded as vendor claims unless independently
  verifiable from documentation.
- No paid plan was purchased and no systematic latency, correctness, usability, or data-quality
  benchmark was performed.
- Absence from public documentation is not proof that a feature does not exist.
- Competitive review should be refreshed before pricing, launch, or a major hosted-platform
  investment.

## Sources

### TradingView

- [Hyperliquid data feed announcement](https://www.tradingview.com/blog/en/trade-xyz-and-hyperliquid-59210/)
- [Pine Screener features and requirements](https://www.tradingview.com/support/solutions/43000742436-tradingview-pine-screener-key-features-and-requirements/)
- [TradingView alerts](https://www.tradingview.com/support/solutions/43000520149-introduction-to-tradingview-alerts/)

### Hyperliquid ecosystem

- [Hyperliquid official HyperCore tools directory](https://hyperliquid.gitbook.io/hyperliquid-docs/builder-tools/hypercore-tools)
- [ASXN Hyperscreener](https://hyperscreener.asxn.xyz/)

### Hyperliquid-native and crypto platforms

- [Buildix product and pricing](https://www.buildix.trade/)
- [Hyperdash product overview](https://hyperdash.com/start)
- [Hyperdash trader discovery documentation](https://docs.hyperdash.com/hyperdash/wallet-explore)
- [Velo documentation](https://docs.velo.xyz/)
- [CoinGlass Hyperliquid monitoring](https://www.coinglass.com/hyperliquid)
- [Laevitas](https://www.laevitas.ch/)
- [Hyblock terminal](https://hyblockcapital.com/features)
- [Altrady technical-analysis screener](https://www.altrady.com/features/crypto-technical-analysis-screener)
