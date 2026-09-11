# gorch — Roadmap verso la v1.0

Checklist operativa derivata dalla revisione post-v0.5.0, con stato al 2026-09-11.

Convenzioni:
- **1 task = 1 commit**, messaggio conventional in italiano.
- Niente nuove feature prima della 1.0. Le idee nuove finiscono in `IDEAS.md`.
- Ogni task dichiara: file toccati, criterio di done, comando di verifica.

## Decisioni prese

- **3.2 Potatura API — keep per tutte e tre.** `WithLabel`/`StatusesByLabel`
  (labels), `WithGroup`/`StartGroup`/`StopGroup`/`StatusesByGroup`, e l'untyped
  `Request`/`RequestAsync` restano: sono testati al 100%, documentati e non hanno
  costo di manutenzione misurabile. Tagliarli romperebbe i consumatori esistenti
  senza un guadagno concreto. Nessuna deprecazione da scrivere.
- **2.5 Gate coverage — resta al 100%, solo sui tag.** `test-and-publish.yml`
  gira su `v*`/`workflow_dispatch`, non su ogni push, come da indicazione.
- **2.4 Fire-and-forget senza timeout — rinviata al dogfooding.** Il fix (wait su
  `Starting` nel check delle dipendenze) è pronto ma non applicato senza prova.

## Stato verificato (2026-09-11)

- Import path **appiattito**: `github.com/lorenzo-vecchio/gorch` (il package non
  è più in `gorch/`). Modulo e package coincidono.
- CI: `format.yml` (gofmt + auto-commit), `lint.yml` (golangci-lint v2 su ogni
  push/PR), `test-and-publish.yml` (build + `go test . -race` + gate coverage
  100% + vet, solo su tag `v*`).
- `WithSelfHeal` su cron o runOnce non è più ignorato: `Register` ritorna
  `ErrUnsupportedOption`.
- `doc.go` presente; README ha Concurrency, Contract, Compatibility & versioning.

---

## Fase 1 — Adesso

### 1.1 Dogfooding strutturato — **in corso (time-based)**
- [ ] Usare la libreria nel monitoring project.
- [ ] Ogni workaround imposto dalla libreria → una voce in un file privato/issue.
- [ ] Dopo 3–4 settimane in produzione, quella lista diventa la roadmap ufficiale.
- **Bloccato dal tempo:** non completabile in una sessione di lavoro.

### 1.2 Contratto scritto (doc only)
- [x] **1.2.1 `doc.go`** — overview: cos'è, use case, cosa non è, contratto. (`a0f6593`)
- [x] **1.2.2 README "Concurrency"** — tabella thread-safety per gruppo di metodi. (`b7fac29`)
- [x] **1.2.3 README "Contract"** — gob wire, `Publish` drop-only, lifecycle
  single-shot, aggregazione errori, tabella dei sentinel. (`43049ff`)
- [x] **1.2.4 README "Compatibility & versioning"** — stabile vs non stabile,
  policy di deprecazione, link alla migration guide. (`57afbda`)

### 1.3 Cron × self-heal
- [x] `Register` fallisce con `ErrUnsupportedOption` su `WithSelfHeal` +
  `WithCron`/`WithRunOnce`; test dedicato; coverage 100%. (`249994b`)

---

## Fase 2 — Robustezza misurabile

### 2.1 golangci-lint in CI
- [x] `.golangci.yml` (staticcheck + errcheck + gosec) e workflow `lint.yml`.
  Fix dei finding reali (unchecked error in `newUUID`, errori non gestiti negli
  esempi). (`563e195`, `b7b7a78`)

### 2.2 Benchmark suite
- [x] `bench_test.go`: Start/Stop 50 servizi, throughput `Messenger`,
  `topoSort` su catena di 200 nodi. (`32ffa19`)

### 2.3 Fuzz del decode path
- [x] `FuzzTypedSubscribeDecode` su `TypedSubscribe`; ~629k esecuzioni, nessun
  panic. (`173be6f`)

### 2.4 Fire-and-forget senza timeout: decisione finale
- [ ] **Rinviata** a dopo 1.1 (dogfooding). Vedi "Decisioni prese".

### 2.5 Gate coverage 100%
- [x] **Decisione:** resta invariato (100%, solo sui tag).

---

## Fase 3 — Ultima breaking sweep, poi freeze

### 3.1 Path d'import alla root
- [x] Package spostato alla root; import `github.com/lorenzo-vecchio/gorch`;
  esempi, README, CI aggiornati; coverage 100%. (`6cd83ce`)

### 3.2 Potatura API
- [x] **Decisione:** keep su labels, gruppi e untyped `Request` (motivazione sopra).

### 3.3 `WithHealthChecks` signature
- [x] `WithHealthChecks(interval, ...HealthCheckOption)` con `WithProbeTimeout` e
  `WithFailureThreshold`; call site e README aggiornati. (`2d60715`)

### 3.4 Migration guide v0.5 → v1.0
- [x] `MIGRATION.md`: import path, `WithHealthChecks`, combinazione self-heal,
  fallback `newUUID`.

---

## Cosa NON fare

- [x] Nessuna nuova feature. (`IDEAS.md` da creare solo quando arriva la prima idea.)

---

## Test finale per la 1.0 (gate di rilascio)

Taggiare `v1.0.0` solo quando **tutte e tre** sono vere:

- [ ] Il monitoring project gira in produzione da 3–4 settimane senza workaround.
- [x] Il contratto (lifecycle single-shot, gob, drop-only, stati) è scritto e
  coerente col codice.
- [x] L'audit API è concluso e la migration guide è pronta.

---

## Dipendenze tra task

- `1.1` → abilita `2.4` e il primo punto del gate finale.
- Fase 3 eseguita in un'unica finestra breaking (fatto).
