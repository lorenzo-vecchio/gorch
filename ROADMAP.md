# gorch — Roadmap verso la v1.0

Checklist operativa derivata dalla revisione post-v0.5.0.

Convenzioni:
- **1 task = 1 commit**, messaggio conventional in italiano (`docs(...)`, `feat(...)`, `test(...)`, `ci(...)`, `refactor(...)`).
- Niente nuove feature prima della 1.0. Le idee nuove finiscono in `IDEAS.md`, non in codice.
- Ogni task dichiara: file toccati, criterio di done, comando di verifica.

## Stato verificato (2026-09-11)

- Modulo `github.com/lorenzo-vecchio/gorch`, package in `gorch/` → import path effettivo `github.com/lorenzo-vecchio/gorch/gorch`.
- `format.yml`: `gofmt -w .` + auto-commit su ogni push.
- `test-and-publish.yml`: build + `go test ./gorch/ -race -coverprofile` + gate coverage 100% + `go vet`, ma **solo su tag `v*` o `workflow_dispatch`** (il gate è già "solo sui tag").
- `WithSelfHeal` su servizio cron: la factory è salvata in `registerConfig` ma letta solo in `lifecycle.go` e `health.go`; `cron.go` non la usa e `Register` non la valida → **silenziosamente ignorata** (confermato).
- `doc.go`: assente.
- Feature candidate alla potatura: labels, `StartGroup`/`StopGroup` (`status.go:101`, `status.go:126`), untyped `Request`/`RequestAsync` (`messenger.go:93`, `messenger.go:122`).
- `WithHealthChecks(interval, timeout, threshold)` con 3 parametri posizionali (`config.go:68`).

---

## Fase 1 — Adesso (alto valore, poco sforzo)

### 1.1 Dogfooding strutturato
- [ ] Usare la libreria nel monitoring project come oggi.
- [ ] Ogni workaround imposto dalla libreria → una voce in un file privato/issue (non in repo pubblico).
- [ ] Dopo 3–4 settimane in produzione, quella lista diventa la roadmap ufficiale.
- **Done:** esiste un registro workaround datato, alimentato come processo e non a memoria.
- **Nota:** nessun file di codice toccato.

### 1.2 Contratto scritto (doc only)
- [ ] **1.2.1 `gorch/doc.go`** — overview del package: cos'è, qual è *il suo* use case, cosa **non** è.
  - File: `gorch/doc.go` (nuovo).
  - Done: `go doc ./gorch` mostra l'overview; nessuna modifica di comportamento.
  - Commit: `docs: aggiungi overview del package in doc.go`.
- [ ] **1.2.2 README → sezione "Concurrency"** — raccogliere i commenti per-metodo già presenti: quali metodi sono safe in parallelo con `Start`/`Stop`.
  - File: `README.md`.
  - Done: tabella/elenco metodi × thread-safety, coerente con i commenti nel codice.
  - Commit: `docs: documenta il contratto di concorrenza`.
- [ ] **1.2.3 README → dichiarazioni esplicite di contratto**:
  - [ ] gob è il formato wire e fa parte del contratto pubblico.
  - [ ] `Publish` è **drop-only**: i messaggi possono perdersi.
  - [ ] errori che `Start`/`Stop` possono ritornare (sentinel da `service.go:216` + aggregati).
  - File: `README.md`.
  - Commit: `docs: dichiara il contratto wire e drop-only`.
- [ ] **1.2.4 README → "Compatibility & versioning"** — cosa è stabile, cosa può cambiare, come si deprecano le API.
  - File: `README.md`.
  - Done: la sezione rimanda a `CHANGELOG.md` e alla futura migration guide.
  - Commit: `docs: aggiungi compatibility & versioning`.

### 1.3 Cron × self-heal: decidere e non ignorare
- [ ] Scegliere **fail at `Register`** (raccomandato) *oppure* supportare il self-heal sui cron. Niente terza via silenziosa.
  - Se fail: nuovo errore sentinella (es. `ErrUnsupportedOption`) in `service.go`, validazione in `Register` (`gorch.go`) quando `cfg.cronSpec != "" && cfg.factory != nil`.
  - File: `gorch/service.go`, `gorch/gorch.go`, `gorch/gorch_test.go`, `CHANGELOG.md`.
  - Done: test che `Register` ritorna l'errore; coverage resta 100%; caso aggiornato in `CHANGELOG.md` sotto `Unreleased`.
  - Commit: `feat(register): fallisci con errore esplicito su cron + self-heal`.

---

## Fase 2 — Prossimo (robustezza misurabile)

### 2.1 golangci-lint in CI
- [ ] Aggiungere job con `staticcheck` + `errcheck` + `gosec` (minimo).
  - File: `.github/workflows/` (nuovo workflow o estensione di `test-and-publish.yml`), eventuale `.golangci.yml`.
  - Done: CI verde, nessuna eccezione non motivata.
  - Commit: `ci: aggiungi golangci-lint (staticcheck, errcheck, gosec)`.

### 2.2 Benchmark suite (3 benchmark)
- [ ] Start/Stop con ~50 servizi.
- [ ] Throughput del `Messenger`.
- [ ] `topoSort` su grafo medio.
  - File: `gorch/gorch_test.go` o nuovo `gorch/bench_test.go`.
  - Done: `go test ./gorch/ -bench . -benchmem` gira; risultati annotati nel commit/README.
  - Commit: `test: aggiungi benchmark di lifecycle, messenger e topoSort`.

### 2.3 Fuzz del decode path del `Messenger`
- [ ] `go test -fuzz` sull'unica superficie che accetta byte arbitrari.
  - File: `gorch/messenger_test.go` (o `gorch/gorch_test.go`).
  - Done: `FuzzDecode` non trova crash dopo una sessione; corpus minimo committato.
  - Commit: `test: fuzz del decode path del messenger`.

### 2.4 Fire-and-forget senza timeout: decisione finale
- [ ] Decidere **dopo** il dogfooding (1.1). Se morde: fix già pronto (wait su `Starting` nel check dipendenze). Se non morde: lasciare documentato.
  - Dipende da: 1.1.
  - Done: decisione esplicita scritta in `ROADMAP.md`/issue, con motivo.

### 2.5 Gate coverage 100%
- [ ] Già attivo solo sui tag → decidere se **tenerlo** così o abbassarlo. Se resta, nessuna azione.
  - Done: decisione registrata.

---

## Fase 3 — Ultima breaking sweep, poi freeze

### 3.1 Path d'import alla root
- [ ] `github.com/lorenzo-vecchio/gorch/gorch` → `github.com/lorenzo-vecchio/gorch`.
  - File: spostamento package a root (o `go.mod` + layout), `README.md`, `examples/*`, import nei test.
  - Done: `go build ./...` verde; nessun doppio `gorch` negli import.
  - Commit: `refactor!: appiattisci il path d'import alla root`.
- **Prerequisito:** Fase 3 tutta insieme, una sola volta.

### 3.2 Potatura API (keep o cut, senza terze vie)
- [ ] `labels` → keep / cut.
- [ ] `StartGroup`/`StopGroup` → keep / cut.
- [ ] untyped `Request`/`RequestAsync` → keep / cut.
  - File: `gorch/service.go`, `gorch/status.go`, `gorch/messenger.go`, relativi test, `README.md`, `CHANGELOG.md`.
  - Done: nessuna API orfana o semi-deprecata; changelog aggiornato.
  - Commit: `refactor!: pota le API non confermate` (scope per singola feature).

### 3.3 `WithHealthChecks` signature
- [ ] Sostituire i 3 parametri posizionali con tipi distinti o opzioni separate.
  - File: `gorch/config.go`, chiamanti, test, `README.md`.
  - Done: impossibile invertire interval/timeout a compile time.
  - Commit: `refactor!: rendi non invertibile WithHealthChecks`.

### 3.4 Migration guide v0.5 → v1.0
- [ ] Scrìvere la guida sulla falsariga di quella 0.4 (esemplare).
  - File: `README.md` o `docs/MIGRATION.md` + `CHANGELOG.md`.
  - Done: ogni breaking change di Fase 3 ha il suo before/after.
  - Commit: `docs: migration guide v0.5 → v1.0`.

---

## Cosa NON fare

- [ ] Nessuna nuova feature. Ogni idea nuova → `IDEAS.md`, congelata fino a dopo la 1.0.
- [ ] Nessuna seconda sweep breaking dopo la Fase 3.

---

## Test finale per la 1.0 (gate di rilascio)

Taggiare `v1.0.0` solo quando **tutte e tre** sono vere:

- [ ] Il monitoring project gira in produzione da 3–4 settimane **senza workaround** causati dalla libreria.
- [ ] Il contratto (lifecycle single-shot, gob, drop-only, stati) è scritto e coerente col codice.
- [ ] L'audit API è concluso e la migration guide è pronta.

---

## Dipendenze tra task

- `1.1` → abilita `2.4`.
- `1.3` e `2.1` sono indipendenti e parallelizzabili.
- Fase 3 va eseguita **dopo** il dogfooding e tutta insieme (un'unica finestra breaking).
