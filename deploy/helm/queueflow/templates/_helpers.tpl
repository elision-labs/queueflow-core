{{/*
Chart name.
*/}}
{{- define "queueflow.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Fully qualified app name.
*/}}
{{- define "queueflow.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "queueflow.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "queueflow.image" -}}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) }}
{{- end }}

{{/*
Common labels (no component).
*/}}
{{- define "queueflow.labels" -}}
helm.sh/chart: {{ include "queueflow.chart" . }}
{{ include "queueflow.selectorLabels" . }}
app.kubernetes.io/version: {{ default .Chart.AppVersion .Values.image.tag | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "queueflow.selectorLabels" -}}
app.kubernetes.io/name: {{ include "queueflow.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Name of the Deployment that serves the HTTP API (target of Service, HPA, PDB).
*/}}
{{- define "queueflow.apiDeploymentName" -}}
{{- if eq .Values.mode "split" }}{{ include "queueflow.fullname" . }}-api{{ else }}{{ include "queueflow.fullname" . }}{{ end }}
{{- end }}

{{/*
Component label value for the API-serving pods.
*/}}
{{- define "queueflow.apiComponent" -}}
{{- if eq .Values.mode "split" }}api{{ else }}all{{ end }}
{{- end }}

{{- define "queueflow.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "queueflow.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Name of the Secret holding the auth credentials.
*/}}
{{- define "queueflow.authSecretName" -}}
{{- if .Values.auth.existingSecret }}{{ .Values.auth.existingSecret }}{{ else }}{{ include "queueflow.fullname" . }}-auth{{ end }}
{{- end }}

{{/*
Validate the values that the server itself would reject at startup, so a
misconfiguration fails at `helm install` with a readable message instead of a
CrashLoopBackOff.
*/}}
{{- define "queueflow.validate" -}}
{{- if not (has .Values.mode (list "all" "split")) }}
{{- fail (printf "mode must be \"all\" or \"split\", got %q" .Values.mode) }}
{{- end }}
{{- if not .Values.database.existingSecret }}
{{- fail "database.existingSecret is required: a Secret whose database.existingSecretKey (default DATABASE_URL) holds the PostgreSQL connection string" }}
{{- end }}
{{- if and (not .Values.auth.dev) (not .Values.auth.existingSecret) }}
{{- if and (not .Values.auth.apiKeys) (not .Values.auth.jwtSecret) }}
{{- fail "auth.apiKeys and/or auth.jwtSecret is required (or set auth.existingSecret); the server refuses to serve the API without tenant credentials" }}
{{- end }}
{{- if not .Values.auth.workerToken }}
{{- fail "auth.workerToken is required (or set auth.existingSecret); the server refuses to serve the API without a worker token" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Environment shared by every queueflow container (DATABASE_URL + tuning).
*/}}
{{- define "queueflow.commonEnv" -}}
- name: DATABASE_URL
  valueFrom:
    secretKeyRef:
      name: {{ .Values.database.existingSecret | quote }}
      key: {{ .Values.database.existingSecretKey | quote }}
- name: RUST_LOG
  value: {{ .Values.config.logLevel | quote }}
- name: QUEUEFLOW_DEFAULT_QUEUE
  value: {{ .Values.config.defaultQueue | quote }}
- name: QUEUEFLOW_MAX_DB_CONNECTIONS
  value: {{ .Values.config.maxDbConnections | quote }}
{{- if .Values.config.retentionHours }}
- name: QUEUEFLOW_RETENTION_HOURS
  value: {{ .Values.config.retentionHours | quote }}
{{- end }}
{{- if .Values.migration.enabled }}
- name: QUEUEFLOW_AUTO_MIGRATE
  value: "false"
{{- end }}
{{- with .Values.config.extraEnv }}
{{ toYaml . }}
{{- end }}
{{- end }}

{{/*
Environment only the API-serving containers need (credentials, CORS).
*/}}
{{- define "queueflow.apiEnv" -}}
{{- if .Values.auth.dev }}
- name: QUEUEFLOW_DEV
  value: "1"
{{- end }}
{{- $secret := include "queueflow.authSecretName" . }}
{{- $keys := .Values.auth.existingSecretKeys }}
{{- if or .Values.auth.apiKeys .Values.auth.existingSecret }}
- name: QUEUEFLOW_API_KEYS
  valueFrom:
    secretKeyRef:
      name: {{ $secret | quote }}
      key: {{ $keys.apiKeys | quote }}
      optional: {{ if .Values.auth.existingSecret }}true{{ else }}false{{ end }}
{{- end }}
{{- if or .Values.auth.jwtSecret .Values.auth.existingSecret }}
- name: QUEUEFLOW_JWT_SECRET
  valueFrom:
    secretKeyRef:
      name: {{ $secret | quote }}
      key: {{ $keys.jwtSecret | quote }}
      optional: {{ if .Values.auth.existingSecret }}true{{ else }}false{{ end }}
{{- end }}
{{- if or .Values.auth.workerToken .Values.auth.existingSecret }}
- name: QUEUEFLOW_WORKER_TOKEN
  valueFrom:
    secretKeyRef:
      name: {{ $secret | quote }}
      key: {{ $keys.workerToken | quote }}
      optional: {{ if .Values.auth.existingSecret }}true{{ else }}false{{ end }}
{{- end }}
{{- if .Values.config.corsOrigins }}
- name: QUEUEFLOW_CORS_ORIGINS
  value: {{ .Values.config.corsOrigins | quote }}
{{- end }}
{{- end }}

{{/*
`serve` arguments. Usage: include "queueflow.serveArgs" (dict "mode" "api" "root" $)
*/}}
{{- define "queueflow.serveArgs" -}}
- serve
- --mode
- {{ .mode }}
{{- if .root.Values.migration.enabled }}
- --auto-migrate
- "false"
{{- end }}
{{- range .root.Values.config.extraArgs }}
- {{ . | quote }}
{{- end }}
{{- end }}
