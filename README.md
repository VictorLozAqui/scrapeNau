# scrapeNau

Scraper da NAU com normalizacao para a base `formacao`, export em Excel, upload para Firestore e automacao preparada para Cloud Run Jobs + Cloud Scheduler.

## Fluxo local

1. `npm run scrape`
   Recolhe a listagem completa da NAU, depois a listagem do ACPD, aplica a mesma regra de `Disponivel ate`, descarta no ACPD os cursos cujo `codigo` ja exista no output principal da NAU e gera:
   - `output/nau-links.json`
   - `output/nau-cursos-base.json`
   - `output/nau-cursos-base.xlsx`
   - `output/nau-links-base-skipados.json`
   - `output/acpd-links.json`
   - `output/acpd-cursos-bruto.json`
   - `output/acpd-cursos-bruto.xlsx`
   - `output/acpd-cursos.json`
   - `output/acpd-cursos.xlsx`
   - `output/acpd-links-skipados.json`
   - `output/acpd-duplicados-ignorados.json`
   - `output/nau-cursos.json`
   - `output/nau-cursos.xlsx`
   - `output/nau-areas-conhecimento.xlsx`
   - `output/nau-links-skipados.json`

2. `npm run normalize`
   Normaliza os cursos para o formato da base e gera:
   - `output/formacaoNau.xlsx`

3. `npm run automate`
   Executa o pipeline completo:
   - scrape NAU base
   - scrape ACPD
   - deduplicacao do ACPD por `codigo` contra o output base da NAU
   - normalizacao
   - preenche `obs` com a data da execucao em `dd-mm-aaaa`
   - remove docs antigos de `nau` e `acpd` na colecao `formacao`
   - envia o output final para o Firestore
   - grava relatorio na colecao `automacao_nau`
   - dispara a automacao AMP no fim

## Instalacao

```bash
npm install
```

## Variaveis relevantes

As defaults do normalizador e da automacao ficam no proprio repositorio, sem depender de ficheiros no `Downloads`.

Variaveis mais importantes:

- `FORMACAO_COLLECTION` (default: `formacao`)
- `NAU_LOGS_COLLECTION` (default: `automacao_nau`)
- `NAU_TIME_ZONE` (default: `Europe/Lisbon`)
- `NAU_DIRECT_FETCH` (default recomendado para Cloud Job: `true`)
- `NAU_OBS_DATE` (normalizacao manual; a automacao preenche sozinha)
- `TRIGGER_AMP_AFTER_RUN` (default: `true`)
- `AMP_AUTOMATION_URL`
- `AMP_AUTOMATION_AUDIENCE` (recomendado: URL base do servico AMP, sem `/run`)
- `AMP_AUTOMATION_SOURCE` (default: `nau`)
- `AMP_AUTOMATION_USE_ID_TOKEN` (default: `true`)
- `AMP_AUTOMATION_FAIL_ON_ERROR` (default: `true`)
- `FIREBASE_SERVICE_ACCOUNT_PATH` ou `FIREBASE_SERVICE_ACCOUNT_JSON` para testes locais

Existe um exemplo em [`.env.nau.automacao.example`](/C:/Users/victo/Desktop/scrapeNau/.env.nau.automacao.example).

## Cloud Run Job

Arquivos adicionados:

- [`cloudjobs/nau/Dockerfile`](/C:/Users/victo/Desktop/scrapeNau/cloudjobs/nau/Dockerfile)
- [`cloudjobs/nau/cloudbuild.yaml`](/C:/Users/victo/Desktop/scrapeNau/cloudjobs/nau/cloudbuild.yaml)
- [`scripts/deploy-cloudjob-nau.ps1`](/C:/Users/victo/Desktop/scrapeNau/scripts/deploy-cloudjob-nau.ps1)

Deploy exemplo:

```powershell
.\scripts\deploy-cloudjob-nau.ps1 `
  -ProjectId "SEU_PROJECT_ID" `
  -SchedulerServiceAccount "scheduler-invoker@SEU_PROJECT_ID.iam.gserviceaccount.com" `
  -AmpAutomationUrl "https://SEU_SERVICO_AMP/run" `
  -AmpServiceName "automacao-amp"
```

O script:

- cria ou reutiliza o Artifact Registry
- cria ou reutiliza o service account do job
- publica a imagem
- faz deploy do Cloud Run Job
- agenda 1 execucao semanal no mesmo horario de Lisboa em que o script for corrido
- configura o Scheduler para chamar o job
- opcionalmente atualiza `ALLOWED_SOURCES` no servico AMP para incluir `nau`
- opcionalmente concede `roles/run.invoker` no servico AMP para o service account do job

## Observacoes

- O job grava logs em `automacao_nau` num formato alinhado com `automacao_iefp`.
- O relatorio da automacao agora inclui um bloco `ACPD` com total de links, total bruto, total final e quantos duplicados por `codigo` foram ignorados.
- O upload para `formacao` usa `cod` como document id.
- Se um `cod` ja existir em `formacao` com outra `db_origem`, a linha nova e ignorada para evitar sobrescrever um registo de outra base.
- Antes de escrever os docs novos, o pipeline remove docs antigos das origens `nau` e `acpd`, para nao deixar cursos expirados ou obsoletos na base.
- O `output/nau-cursos.json` continua a ser o JSON final usado pela normalizacao, mas agora ele representa a soma de `NAU base + ACPD deduplicado`.
- Na normalizacao, cursos da NAU saem com `db_origem=nau` e `cod=<codigo>_nau`; cursos vindos do ACPD saem com `db_origem=acpd` e `cod=<codigo>_acpd`.
- O campo `AmpNetwork` continua sem ser preenchido aqui; ele fica para a automacao AMP disparada no fim.
- Se passares `-AmpServiceName`, o deploy da NAU tambem atualiza `ALLOWED_SOURCES` no servico AMP para incluir `nau` e `acpd`.
