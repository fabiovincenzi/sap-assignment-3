# Service Level Objectives e Service Level Indicators

Documento richiesto dal punto 2 della consegna dell'Assignment 3: *"Define a couple (2) of SLOs and
the corresponding SLIs for the Shipping on the Air case study, extending the implementation so as to
measure the SLIs"*.

Riferimento teorico: Lab Notes *Quality Attributes in SRE*.

## I termini

| Sigla | Nome | Domanda a cui risponde |
|---|---|---|
| **SLI** | Service Level Indicator | *"Come stiamo andando?"* Una misura quantitativa. |
| **SLO** | Service Level Objective | *"Che livello dovremmo raggiungere?"* Un obiettivo interno. |
| **SLA** | Service Level Agreement | *"Cosa promettiamo per contratto?"* |

Il sistema non ha clienti paganti, quindi **non e' definito alcun SLA**: la consegna chiede
esplicitamente solo SLO e SLI.

Entrambi gli obiettivi sono definiti sull'**API Gateway**, che e' l'unico componente esposto
all'esterno e quindi il punto piu' vicino a cio' che l'utente percepisce. E' la raccomandazione delle
Lab Notes, che sull'esempio Shakespeare di Google SRE insistono nel definire la disponibilita' *"in
terms of the user experience"* e non su indicatori puramente tecnici.

## SLO 1: disponibilita'

> Almeno il **99,5%** delle richieste ricevute dall'API Gateway deve essere servito con successo,
> su una finestra mobile di **30 giorni**.

**SLI**: percentuale di richieste con esito positivo sul totale. Una richiesta e' considerata
riuscita quando il gateway risponde con uno **status inferiore a 400**: un 4xx e' comunque un
fallimento dal punto di vista dell'utente, che non ottiene cio' che ha chiesto.

Le chiamate a `/health` sono **escluse** dal conteggio: sono interrogazioni della piattaforma, non
traffico di clienti, e includerle falserebbe l'indicatore.

## SLO 2: latenza

> Il **95esimo percentile** del tempo di risposta dell'API Gateway deve restare sotto **1 secondo**.

**SLI**: 95esimo percentile della durata delle richieste, calcolato dai bucket dell'istogramma.

La soglia di 1 secondo e' ereditata dallo scenario di performance formulato nell'Assignment 1. Nel
report dell'Assignment 2 quello scenario era stato dichiarato **non verificabile**, perche' le
metriche disponibili erano solo counter e gauge: dicevano quante richieste erano state servite, non
quanto tempo avessero impiegato. L'istogramma introdotto in A3 chiude quel limite.

Si usa il percentile e non la media perche', come osservano le Lab Notes, *"average response times
can be misleading, especially in systems with high variability"*: una media bassa puo' nascondere una
minoranza di richieste molto lente, che sono proprio quelle che gli utenti notano.

## Le metriche che li alimentano

Esposte dall'API Gateway sulla porta dedicata `9492`, endpoint `/metrics`.

| Metrica | Tipo | Contenuto |
|---|---|---|
| `gateway_requests_total{outcome="success\|error"}` | counter con label | richieste servite, separate per esito |
| `gateway_request_duration_seconds` | **histogram** | distribuzione dei tempi di risposta |

Scelte di design:

- **Una sola label, `outcome`.** Si era valutato di aggiungere il path della richiesta, ma le rotte
  contengono identificatori (`/api/v1/orders/{id}`): usarli come valori di label farebbe crescere
  senza limite il numero di serie temporali, problema noto come esplosione della **cardinalita'**.
  Gli indicatori definiti qui non ne hanno bisogno.
- **Bucket dell'istogramma**: `0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10` secondi. Il bound
  esatto a **1 secondo** e' deliberato: e' la soglia contro cui e' enunciato lo SLO 2, e
  `histogram_quantile` interpola fra i bucket, quindi e' tanto piu' preciso quanto piu' un bound cade
  sul valore di interesse.
- **Misura a fine risposta**, tramite `addBodyEndHandler`: e' l'unico momento in cui lo status code e'
  gia' determinato e la risposta e' stata effettivamente scritta.

## Query PromQL

**SLI 1, disponibilita':**

```promql
sum(rate(gateway_requests_total{outcome="success"}[30d]))
  /
sum(rate(gateway_requests_total[30d]))
```

**SLI 2, latenza al 95esimo percentile:**

```promql
histogram_quantile(0.95, sum(rate(gateway_request_duration_seconds_bucket[5m])) by (le))
```

Nota che anche i bucket di un istogramma **sono counter**, quindi vanno letti con `rate()` e non nel
loro valore assoluto. La label `le` (*less or equal*) e' quella dei bucket.

## Error budget

Un obiettivo del 99,5% dichiara implicitamente che lo **0,5% puo' fallire**. Quella quota si chiama
**error budget** ed e' il concetto che rende operativi gli SLO.

```
finestra          30 giorni = 43.200 minuti
error budget      0,5% di 43.200 = 216 minuti = circa 3 ore e 36 minuti
```

Il budget si consuma: finche' resta margine si puo' rilasciare e sperimentare, quando e' esaurito
conviene fermarsi e stabilizzare. E' il meccanismo con cui un obiettivo numerico si traduce in una
decisione su cosa fare.

Le Lab Notes fanno lo stesso conto per il 99,9%, che concede 43,2 minuti su 30 giorni, e introducono
**MTBF** e **MTTR** come strumenti per pianificarlo.

## Limiti dichiarati

Tre precisazioni, senza le quali questi obiettivi sarebbero decorativi.

**La finestra di 30 giorni non e' osservabile.** Trenta giorni e' la scala su cui un service level ha
senso in produzione, ed e' per questo che gli obiettivi sono enunciati cosi'. Ma il sistema viene
avviato per la durata delle prove, quindi Prometheus non possiede quella storia. La verifica
automatica usa una finestra di **5 minuti**: cambia l'ampiezza del campione, non la forma
dell'indicatore.

**L'obiettivo di latenza e' ampio rispetto ai valori osservati.** Una misura su richieste di tracking
ha dato circa **35 ms** per richiesta, cioe' quasi trenta volte sotto la soglia. La soglia e' stata
mantenuta a 1 secondo perche' proviene dal quality attribute dichiarato nell'Assignment 1, ma il
margine va riportato: un obiettivo largo dichiarato insieme al suo margine e' informativo, un
obiettivo largo presentato da solo sembra scelto per non essere mai violato.

**Lo SLI di disponibilita' ha un punto cieco.** Conta le richieste **a cui il gateway ha risposto**:
se il gateway fosse spento, numeratore e denominatore resterebbero entrambi fermi e il rapporto
resterebbe alto. E' esattamente la trappola descritta dalle Lab Notes nell'esempio Shakespeare, dove
un guasto riduceva anche il denominatore e la disponibilita' misurata restava al 99,7% mentre gli
utenti non ottenevano piu' nulla. L'indicatore va quindi letto **insieme alla metrica `up`**, generata
automaticamente da Prometheus per ogni target e gia' usata nel QAS-A dell'Assignment 2: `up` copre il
caso "non risponde affatto", lo SLI copre il caso "risponde male".

## Relazione con i Quality Attribute Scenarios

QAS e SLO sono parenti ma non sinonimi, e la differenza vale la pena esplicitarla.

| | QAS (Assignment 2) | SLO (Assignment 3) |
|---|---|---|
| Che cos'e' | uno **scenario** in sei parti | un **obiettivo numerico** continuo |
| Come si verifica | **provocando** la condizione | **osservando** in continuo |
| Esempio | *se il drone-service cade, il guasto e' rilevato entro 15 s* | *il 99,5% delle richieste riesce su 30 giorni* |

Il ponte fra i due e' la **Response Measure** del QAS, che e' gia' a tutti gli effetti un indicatore.
Il report dell'Assignment 2 sosteneva che *"un Quality Attribute Scenario e' verificabile solo se la
sua Response Measure e' osservabile"*: gli SLO applicano lo stesso principio al regime continuo.
