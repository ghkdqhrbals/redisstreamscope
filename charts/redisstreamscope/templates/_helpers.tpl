{{- define "redisstreamscope.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "redisstreamscope.fullname" -}}
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

{{- define "redisstreamscope.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "redisstreamscope.selectorLabels" -}}
app.kubernetes.io/name: {{ include "redisstreamscope.name" . | quote }}
app.kubernetes.io/instance: {{ .Release.Name | quote }}
{{- end }}

{{- define "redisstreamscope.labels" -}}
helm.sh/chart: {{ include "redisstreamscope.chart" . | quote }}
{{ include "redisstreamscope.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service | quote }}
{{- end }}

{{- define "redisstreamscope.initialAdminSecretName" -}}
{{- default (printf "%s-initial-admin" (include "redisstreamscope.fullname" .)) .Values.initialAdmin.existingSecret }}
{{- end }}

{{- define "redisstreamscope.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "redisstreamscope.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "redisstreamscope.validateValues" -}}
{{- if ne (int .Values.replicaCount) 1 }}
{{- fail "replicaCount must be 1 because RedisStreamScope uses an embedded SQLite database" }}
{{- end }}
{{- $sources := 0 }}
{{- if .Values.redis.host }}{{- $sources = add1 $sources }}{{- end }}
{{- if .Values.redis.url }}{{- $sources = add1 $sources }}{{- end }}
{{- if gt (len .Values.redis.nodes) 0 }}{{- $sources = add1 $sources }}{{- end }}
{{- if gt $sources 1 }}
{{- fail "set exactly one Redis connection source: redis.host, redis.url, or redis.nodes" }}
{{- end }}
{{- if and .Values.config.publicURL (not (regexMatch "^https?://[^[:space:]]+$" .Values.config.publicURL)) }}
{{- fail "config.publicURL must be an absolute HTTP or HTTPS URL" }}
{{- end }}
{{- if and (eq .Values.redis.mode "sentinel") (not .Values.redis.masterName) $sources }}
{{- fail "redis.masterName is required when redis.mode is sentinel" }}
{{- end }}
{{- if and (eq .Values.redis.mode "cluster") (ne (int .Values.redis.database) 0) }}
{{- fail "redis.database must be 0 when redis.mode is cluster" }}
{{- end }}
{{- if ne (not (empty .Values.redis.tls.certKey)) (not (empty .Values.redis.tls.keyKey)) }}
{{- fail "redis.tls.certKey and redis.tls.keyKey must be configured together" }}
{{- end }}
{{- if and (or .Values.redis.tls.caKey .Values.redis.tls.certKey .Values.redis.tls.keyKey) (not .Values.redis.tls.existingSecret) }}
{{- fail "redis.tls.existingSecret is required when Redis TLS key names are configured" }}
{{- end }}
{{- end }}
