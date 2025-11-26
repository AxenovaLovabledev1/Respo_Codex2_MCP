# influx-mcp

Servidor [Model Context Protocol](https://spec.modelcontextprotocol.io/) para exponer consultas sobre clústeres InfluxDB v2 (Flux) e InfluxDB v1.x (InfluxQL) con un foco en datos de redes IoT.

## Características

- Autodetección opcional de versión del servidor (v2 o v1.x) o selección explícita mediante variables de entorno.
- Registro de herramientas MCP para explorar buckets/databases, measurements, tags, fields y ejecutar consultas de series de tiempo con agregaciones y ventanas.
- Namespace de recursos `influxdb://` que permite a un agente recuperar series de tiempo mediante URIs direccionables.
- Respuestas validadas mediante modelos Pydantic y serializadas a JSON (con soporte opcional de previsualización tabular).
- CLI `python -m influx_mcp.server` con modo `--dry-run` para validar la configuración y conectividad.

## Instalación

```bash
pip install -e .
```

> **Nota:** el proyecto requiere Python 3.11 o superior.

## Configuración

Copie el archivo `.env.example` como base y complete los valores según su despliegue:

```bash
cp .env.example .env
```

Variables disponibles:

| Variable | Descripción |
| --- | --- |
| `INFLUX_VERSION` | `2`, `1` o `auto` (default `auto`). |
| `INFLUX_URL` | URL base del clúster (incluye protocolo y puerto). |
| `INFLUX_ORG` | Organización para InfluxDB v2. |
| `INFLUX_TOKEN` | Token de acceso v2. |
| `INFLUX_DEFAULT_BUCKET` | Bucket por defecto (opcional). |
| `INFLUX_USERNAME` | Usuario v1 (opcional si se deshabilitó auth). |
| `INFLUX_PASSWORD` | Password v1. |
| `INFLUX_DEFAULT_DB` | Base de datos por defecto v1.x. |
| `INFLUX_DEFAULT_RP` | Retention policy por defecto v1.x. |
| `INFLUX_REQUEST_TIMEOUT_SEC` | Timeout en segundos (default 30). |
| `MCP_LOG_LEVEL` | Nivel de log (`INFO` por defecto). |

## Ejecución

```bash
python -m influx_mcp.server
```

Parámetros CLI disponibles:

- `--dry-run`: valida la configuración, intenta conectarse al servidor y muestra la versión detectada junto con los targets por defecto. No inicia el loop MCP.

## Herramientas MCP

Todas las herramientas retornan JSON con los campos descritos a continuación. Los modelos están documentados en `influx_mcp/schemas.py`.

- `list_buckets_or_dbs()`
  - Respuesta: lista de `{name, type, retention}`.
- `list_measurements(target)`
  - `target` corresponde a un bucket (v2) o `database[/retention_policy]` (v1).
  - Respuesta: lista de `{name}`.
- `list_fields(target, measurement)`
  - Respuesta: lista `{name, type}` cuando está disponible.
- `list_tags(target, measurement)`
  - Respuesta: lista `{key, values}`.
- `last_point(target, measurement, field?, tags?)`
  - Respuesta: `{time_iso, value, field, tags}`.
- `query_timeseries(...)`
  - Parámetros: `target`, `measurement`, `field`, `start`, `stop?`, `tags?`, `aggregate?`, `every?`, `limit?`, `fill?`.
  - Respuesta: `{series: [...], stats: {...}}` con los puntos y metadatos de la consulta.
- `window_stats(target, measurement, field, window, tags?)`
  - Calcula métricas agregadas (`mean`, `min`, `max`, `last`, `count`) en la ventana indicada.
- `write_point(target, measurement, fields, tags?, time_iso?)` *(opcional, habilitada si el token/usuario tiene permisos de escritura)*.

## Recursos MCP `influxdb://`

- `read_resource(uri)`: ejecuta una consulta equivalente a `query_timeseries` en función de los parámetros de la URI y devuelve un texto con un encabezado descriptivo, una previsualización tabular y el JSON completo.
- `list_resources()`: expone un número acotado de URIs sugeridas (p.ej. measurements populares del target por defecto) para facilitar la exploración guiada.

Formato de URI:

```
influxdb://<target>/<measurement>?field=<field>&start=<iso_rel>&stop=<iso_rel>&every=<dur>&aggregate=<agg>&tag.device_id=abc
```

## Ejemplos de uso

- Último nivel de batería:

```json
{
  "tool": "last_point",
  "params": {
    "target": "sensors",
    "measurement": "device_status",
    "field": "battery",
    "tags": {"device_id": "abc123"}
  }
}
```

- Serie agregada 24h (promedio cada 5 minutos):

```json
{
  "tool": "query_timeseries",
  "params": {
    "target": "sensors",
    "measurement": "env",
    "field": "temperature",
    "start": "-24h",
    "every": "5m",
    "aggregate": "mean",
    "tags": {"site": "planta1"}
  }
}
```

## Pruebas

```bash
pytest
```

Los tests mockean el cliente Influx y validan el parseo de tiempos, URIs y contratos básicos de las herramientas.
