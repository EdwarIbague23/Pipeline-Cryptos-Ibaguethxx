# Pipeline Kafka + CoinGecko en Tiempo Real

Pipeline de datos en tiempo real para monitoreo de precios de criptomonedas usando Apache Kafka. Consume datos reales de CoinGecko API, los publica en un broker Kafka, y un processor calcula estadísticas por ventanas de tiempo de 1 minuto. Incluye dashboard visual interactivo que simula el flujo completo del pipeline.

## Arquitectura

```
┌─────────────────┐     ┌──────────────┐     ┌─────────────┐     ┌──────────┐
│  CoinGecko API  │────▶│   Producer   │────▶│    Kafka    │────▶│ Processor│
│   (sin API key) │     │ (Python POO) │     │   Broker    │     │(Python)  │
└─────────────────┘     └──────────────┘     └─────────────┘     └────┬─────┘
                                                                     │
                              ┌───────────────────────────────────────┘
                              ▼
                        ┌───────────┐     ┌────────────┐
                        │  datos    │     │  Alertas   │
                        │  .csv     │     │  .log      │
                        └───────────┘     └────────────┘
                                │
                                ▼
                          ┌───────────┐
                          │ Dashboard │
                          │   .html   │
                          └───────────┘
```

## Pasos de Ejecución

1. Iniciar los servicios de Kafka: `docker-compose up -d`
2. Instalar dependencias: `pip install kafka-python requests`
3. En Terminal A ejecutar: `python producer_crypto.py`
4. En Terminal B ejecutar: `python processor.py`
5. (Opcional) En Terminal C ejecutar: `python producer_sensores.py` para segundo topic
6. Abrir `dashboard.html` en cualquier navegador web
7. Acceder a Kafka UI: `http://localhost:8080`

## Ventanas Tumbling

Las ventanas tumbling son un mecanismo de procesamiento de streams que divide el flujo continuo en segmentos temporales fijos y no solapados. En este proyecto:

- Cada ventana tiene una duración de 60 segundos
- Los eventos que llegan dentro de la ventana se acumulan
- Al cerrar la ventana se calculan estadísticas (min, max, promedio)
- Inmediatamente comienza una nueva ventana
- Las alertas se disparan cuando los umbrales se superan en la ventana

## Guía de Demo para Sustentación

1. Ejecutar `docker-compose up -d` y verificar que los contenedores estén corriendo
2. Abrir http://localhost:8080 para ver el Kafka UI
3. Ejecutar `python producer_crypto.py` - observar los precios en consola
4. Ejecutar `python processor.py` - observar las estadísticas y alertas
5. Abrir dashboard.html - hacer clic en [Iniciar] para ver la simulación
6. Mostrar el archivo `data/estadisticas.csv` con los datos persistidos
7. Explicar el flujo de datos desde CoinGecko hasta el CSV
8. Demostrar las alertas cambiando los umbrales en el código

## Monedas Monitoreadas

- Bitcoin (BTC)
- Ethereum (ETH)
- Solana (SOL)
- Cardano (ADA)
- Dogecoin (DOGE)

## Requisitos

- Python 3.8+
- Docker y Docker Compose
- kafka-python
- requests