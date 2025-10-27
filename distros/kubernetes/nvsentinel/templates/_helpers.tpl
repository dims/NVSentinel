{{/*
Expand the name of the chart.
*/}}
{{- define "nvsentinel.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "nvsentinel.fullname" -}}
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

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "nvsentinel.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "nvsentinel.labels" -}}
helm.sh/chart: {{ include "nvsentinel.chart" . }}
{{ include "nvsentinel.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "nvsentinel.selectorLabels" -}}
app.kubernetes.io/name: {{ include "nvsentinel.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "nvsentinel.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "nvsentinel.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Dynamically determine MongoDB service name based on architecture.
This mirrors the logic from the MongoDB chart's mongodb.service.nameOverride helper.
*/}}
{{- define "nvsentinel.mongodb.serviceName" -}}
{{- if and .Values.global.datastore (eq .Values.global.datastore.provider "mongodb") }}
{{- $mongodbStore := index .Values "mongodb-store" }}
{{- if and $mongodbStore $mongodbStore.mongodb }}
{{- $mongodb := $mongodbStore.mongodb }}
{{- if and $mongodb.service $mongodb.service.nameOverride }}
{{- print $mongodb.service.nameOverride }}
{{- else }}
{{- $fullname := printf "%s-mongodb" (include "nvsentinel.fullname" .) }}
{{- if eq ($mongodb.architecture | default "standalone") "replicaset" }}
{{- printf "%s-headless" $fullname }}
{{- else }}
{{- printf "%s" $fullname }}
{{- end }}
{{- end }}
{{- else }}
{{- printf "%s-mongodb" (include "nvsentinel.fullname" .) }}
{{- end }}
{{- else }}
{{- .Values.global.datastore.connection.host }}
{{- end }}
{{- end }}
