"""
Producer de criptomonedas que consume la API de CoinGecko y publica en Kafka.
"""
import json
import time
import requests
from kafka import KafkaProducer
from datetime import datetime

class CoinGeckoClient:
    """
    Cliente para consumir la API gratuita de CoinGecko.
    """

    def __init__(self):
        self.url = "https://api.coingecko.com/api/v3/simple/price"
        self.params = {
            'ids': 'bitcoin,ethereum,solana,cardano,dogecoin',
            'vs_currencies': 'usd',
            'include_24hr_change': 'true'
        }

    def obtener_precios(self):
        """
        Obtiene los precios actuales de las criptomonedas.
        
        Returns:
            dict: Diccionario con precios y cambio 24h por moneda.
        """
        try:
            respuesta = requests.get(self.url, params=self.params, timeout=10)
            respuesta.raise_for_status()
            return respuesta.json()
        except requests.exceptions.RequestException as e:
            print(f"❌ Error al conectar con CoinGecko: {e}")
            return {}

class EventoCrypto:
    """
    Representa un evento de precio de criptomoneda.
    """

    MAPA_SIMBOLOS = {
        'bitcoin': 'BTC',
        'ethereum': 'ETH',
        'solana': 'SOL',
        'cardano': 'ADA',
        'dogecoin': 'DOGE'
    }

    def __init__(self, coin_id, precio_usd, cambio_24h):
        self.coin_id = coin_id
        self.symbol = self.MAPA_SIMBOLOS.get(coin_id, coin_id.upper())
        self.precio_usd = precio_usd
        self.cambio_24h = cambio_24h
        self.timestamp = datetime.now().isoformat()

    def to_json(self):
        """
        Serializa el evento a JSON string.
        
        Returns:
            str: Representación JSON del evento.
        """
        return json.dumps({
            'coin_id': self.coin_id,
            'symbol': self.symbol,
            'precio_usd': self.precio_usd,
            'cambio_24h': self.cambio_24h,
            'timestamp': self.timestamp
        })

    def __str__(self):
        emoji = '📈' if self.cambio_24h >= 0 else '📉'
        return f"{emoji} {self.symbol}: ${self.precio_usd:,.2f} ({self.cambio_24h:+.2f}%)"

class CryptoProducer:
    """
    Productor Kafka que publica eventos de precios de criptomonedas.
    """

    def __init__(self, bootstrap_servers, topic):
        """
        Args:
            bootstrap_servers: Lista de servidores Kafka.
            topic: Nombre del topic donde publicar.
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.cliente = CoinGeckoClient()
        self.producer = None

    def conectar(self):
        """
        Instancia el KafkaProducer con configuración adecuada.
        """
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                api_version=(0, 10, 0),
                retries=5,
                value_serializer=lambda v: v.encode('utf-8')
            )
            print(f"✅ Conectado a Kafka en {self.bootstrap_servers}")
        except Exception as e:
            print(f"❌ Error al conectar con Kafka: {e}")
            raise

    def publicar_evento(self, evento):
        """
        Publica un evento en el topic de Kafka.
        
        Args:
            evento: Instancia de EventoCrypto a publicar.
        """
        try:
            future = self.producer.send(self.topic, value=evento.to_json())
            future.get(timeout=10)
            self.producer.flush()
            print(f"✅ {evento}")
        except Exception as e:
            print(f"❌ Error al publicar evento: {e}")

    def iniciar(self, intervalo_segundos=10):
        """
        Inicia el loop de producción de eventos.
        
        Args:
            intervalo_segundos: Segundos entre cada consulta a la API.
        """
        print(f"🚀 Iniciando producer (intervalo: {intervalo_segundos}s)")
        print("Presiona Ctrl+C para detener")
        
        while True:
            datos = self.cliente.obtener_precios()
            
            if datos:
                for coin_id, info in datos.items():
                    precio = info.get('usd', 0)
                    cambio = info.get('usd_24h_change', 0)
                    evento = EventoCrypto(coin_id, precio, cambio)
                    self.publicar_evento(evento)

                    if cambio < -2:
                        print(f"📉 CAÍDA: {coin_id.upper()} bajó {cambio:.2f}%")
                    elif cambio > 2:
                        print(f"📈 ALZA: {coin_id.upper()} subió {cambio:.2f}%")
            
            time.sleep(intervalo_segundos)

if __name__ == "__main__":
    producer = CryptoProducer(
        bootstrap_servers=['localhost:9092'],
        topic='crypto-prices'
    )
    producer.conectar()
    producer.iniciar(intervalo_segundos=10)