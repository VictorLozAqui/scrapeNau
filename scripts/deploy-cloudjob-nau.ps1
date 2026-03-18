param(
  [string]$ProjectId = "",
  [string]$Region = "europe-west1",
  [string]$SchedulerRegion = "europe-west1",
  [string]$Repository = "automacoes",
  [string]$ImageName = "automacao-nau-formacao",
  [string]$JobName = "automacao-nau-formacao",
  [string]$JobServiceAccountName = "automacao-nau-job",
  [string]$SchedulerJobName = "automacao-nau-semanal",
  [string]$SchedulerServiceAccount = "",
  [string]$ScheduleCron = "",
  [string]$AmpAutomationUrl = "",
  [string]$AmpServiceName = "",
  [string]$AmpServiceRegion = "",
  [string]$AmpAllowedSources = "anqep,dges,iefp,nau",
  [switch]$DisableAmpTrigger
)

$ErrorActionPreference = "Stop"

function Invoke-Gcloud {
  param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$Args
  )

  & gcloud @Args
  if ($LASTEXITCODE -ne 0) {
    throw "gcloud command failed: gcloud $($Args -join ' ')"
  }
}

function Get-LisbonNow {
  $tz = [System.TimeZoneInfo]::FindSystemTimeZoneById("GMT Standard Time")
  return [System.TimeZoneInfo]::ConvertTime([DateTimeOffset]::UtcNow, $tz)
}

function Get-LisbonWeeklyCron {
  param(
    [DateTimeOffset]$DateTimeOffset
  )

  $dayMap = @{
    "Sunday" = 0
    "Monday" = 1
    "Tuesday" = 2
    "Wednesday" = 3
    "Thursday" = 4
    "Friday" = 5
    "Saturday" = 6
  }

  return "$($DateTimeOffset.Minute) $($DateTimeOffset.Hour) * * $($dayMap[$DateTimeOffset.DayOfWeek.ToString()])"
}

if ([string]::IsNullOrWhiteSpace($ProjectId)) {
  $ProjectId = (& gcloud config get-value project 2>$null).Trim()
}

if ([string]::IsNullOrWhiteSpace($ProjectId)) {
  throw "Informe -ProjectId ou configure um projeto default no gcloud."
}

if ([string]::IsNullOrWhiteSpace($SchedulerServiceAccount)) {
  throw "Informe -SchedulerServiceAccount com o service account usado pelo Cloud Scheduler."
}

if (-not $DisableAmpTrigger -and [string]::IsNullOrWhiteSpace($AmpAutomationUrl)) {
  throw "Informe -AmpAutomationUrl ou use -DisableAmpTrigger."
}

if ([string]::IsNullOrWhiteSpace($AmpServiceRegion)) {
  $AmpServiceRegion = $Region
}

$ampAudience = ""
if (-not $DisableAmpTrigger) {
  $ampUri = [System.Uri]$AmpAutomationUrl
  $ampAudience = $ampUri.GetLeftPart([System.UriPartial]::Authority)
}

$lisbonNow = Get-LisbonNow
if ([string]::IsNullOrWhiteSpace($ScheduleCron)) {
  $ScheduleCron = Get-LisbonWeeklyCron -DateTimeOffset $lisbonNow
}

$ImageUri = "$Region-docker.pkg.dev/$ProjectId/$Repository/$ImageName`:latest"
$SchedulerTargetUri = "https://run.googleapis.com/v2/projects/$ProjectId/locations/$Region/jobs/${JobName}:run"
$JobServiceAccount = "$JobServiceAccountName@$ProjectId.iam.gserviceaccount.com"
$ampTriggerEnabled = if ($DisableAmpTrigger) { "false" } else { "true" }

$envVars = @(
  "TZ=Europe/Lisbon",
  "NAU_TIME_ZONE=Europe/Lisbon",
  "NAU_DIRECT_FETCH=true",
  "FORMACAO_COLLECTION=formacao",
  "NAU_LOGS_COLLECTION=automacao_nau",
  "TRIGGER_AMP_AFTER_RUN=$ampTriggerEnabled",
  "AMP_AUTOMATION_SOURCE=nau",
  "AMP_AUTOMATION_USE_ID_TOKEN=true",
  "AMP_AUTOMATION_FAIL_ON_ERROR=true",
  "AMP_AUTOMATION_TIMEOUT_SEC=3600"
)

if (-not $DisableAmpTrigger) {
  $envVars += "AMP_AUTOMATION_URL=$AmpAutomationUrl"
  $envVars += "AMP_AUTOMATION_AUDIENCE=$ampAudience"
}

Write-Host "Projeto: $ProjectId"
Write-Host "Imagem: $ImageUri"
Write-Host "Job: $JobName"
Write-Host "Job SA: $JobServiceAccount"
Write-Host "Scheduler: $SchedulerJobName"
Write-Host "Schedule: $ScheduleCron"
Write-Host "Time zone: Europe/Lisbon"
Write-Host "Lisbon now: $($lisbonNow.ToString('yyyy-MM-dd HH:mm'))"

Invoke-Gcloud config set project $ProjectId | Out-Null

Invoke-Gcloud services enable `
  run.googleapis.com `
  cloudbuild.googleapis.com `
  artifactregistry.googleapis.com `
  cloudscheduler.googleapis.com `
  firestore.googleapis.com | Out-Null

$repoExists = (& gcloud artifacts repositories list `
  --location $Region `
  --filter "name~/$Repository$" `
  --format "value(name)")

if ([string]::IsNullOrWhiteSpace($repoExists)) {
  Invoke-Gcloud artifacts repositories create $Repository `
    --repository-format docker `
    --location $Region `
    --description "Repositorio para automacoes Cloud Run Job" | Out-Null
}

$jobSaExists = (& gcloud iam service-accounts list `
  --filter "email=$JobServiceAccount" `
  --format "value(email)")

if ([string]::IsNullOrWhiteSpace($jobSaExists)) {
  Invoke-Gcloud iam service-accounts create $JobServiceAccountName `
    --display-name "Automacao NAU Formacao Job" | Out-Null
}

Invoke-Gcloud projects add-iam-policy-binding $ProjectId `
  --member "serviceAccount:$JobServiceAccount" `
  --role "roles/datastore.user" | Out-Null

Invoke-Gcloud builds submit `
  --config cloudjobs/nau/cloudbuild.yaml `
  --substitutions "_IMAGE_URI=$ImageUri" `
  .

Invoke-Gcloud run jobs deploy $JobName `
  --region $Region `
  --image $ImageUri `
  --service-account $JobServiceAccount `
  --tasks 1 `
  --max-retries 1 `
  --cpu 2 `
  --memory 2Gi `
  --task-timeout 3600s `
  --set-env-vars ($envVars -join ",")

if (-not [string]::IsNullOrWhiteSpace($AmpServiceName)) {
  Invoke-Gcloud run services update $AmpServiceName `
    --region $AmpServiceRegion `
    --update-env-vars "^:^ALLOWED_SOURCES=$AmpAllowedSources" | Out-Null

  Invoke-Gcloud run services add-iam-policy-binding $AmpServiceName `
    --region $AmpServiceRegion `
    --member "serviceAccount:$JobServiceAccount" `
    --role "roles/run.invoker" | Out-Null
}

Invoke-Gcloud run jobs add-iam-policy-binding $JobName `
  --region $Region `
  --member "serviceAccount:$SchedulerServiceAccount" `
  --role "roles/run.invoker" | Out-Null

$schedulerExists = (& gcloud scheduler jobs list `
  --location $SchedulerRegion `
  --filter "name~/$SchedulerJobName$" `
  --format "value(name)")

$schedulerArgs = @(
  "--location", $SchedulerRegion,
  "--schedule", $ScheduleCron,
  "--time-zone", "Europe/Lisbon",
  "--uri", $SchedulerTargetUri,
  "--http-method", "POST",
  "--headers", "Content-Type=application/json",
  "--message-body", "{}",
  "--oauth-service-account-email", $SchedulerServiceAccount,
  "--oauth-token-scope", "https://www.googleapis.com/auth/cloud-platform"
)

if (-not [string]::IsNullOrWhiteSpace($schedulerExists)) {
  Invoke-Gcloud scheduler jobs delete $SchedulerJobName --location $SchedulerRegion --quiet | Out-Null
}

Invoke-Gcloud scheduler jobs create http $SchedulerJobName @schedulerArgs | Out-Null

Write-Host ""
Write-Host "Cloud Job publicado e agendado."
Write-Host "Trigger AMP ativo: $ampTriggerEnabled"
if (-not [string]::IsNullOrWhiteSpace($AmpServiceName)) {
  Write-Host "Invoker do AMP concedido em: $AmpServiceName ($AmpServiceRegion)"
  Write-Host "ALLOWED_SOURCES atualizado para: $AmpAllowedSources"
}
Write-Host "Teste manual:"
Write-Host "gcloud run jobs execute $JobName --region $Region --wait"
