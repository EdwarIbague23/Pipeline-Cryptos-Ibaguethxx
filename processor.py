"""
Processor que consume mensajes de Kafka, procesa ventanas y genera alertas.
"""
import json
import csv
import os
from datetime import datetime
from kafka import KafkaConsumer


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

    def __init__(self, coin_id, precio_usd, cambio_24h, timestamp=None):
        self.coin_id = coin_id
        self.symbol = self.MAPA_SIMBOLOS.get(coin_id, coin_id.upper())
        self.precio_usd = precio_usd
        self.cambio_24h = cambio_24h
        self.timestamp = timestamp or datetime.now().isoformat()

    @classmethod
    def from_json(cls, json_str):
        """
        Deserializa un evento desde JSON.
        
        Args:
            json_str: String JSON con los datos del evento.
        
        Returns:
            EventoCrypto: Instancia del evento.
        """
        datos = json.loads(json_str)
        return cls(
            coin_id=datos.get('coin_id'),
            precio_usd=datos.get('precio_usd'),
            cambio_24h=datos.get('cambio_24h'),
            timestamp=datos.get('timestamp')
        )

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


class VentanaTumbling:
    """
    Implementa una ventana temporal de tipo tumbling para aggregation de eventos.
    """

    def __init__(self, duracion_segundos=60):
        """
        Args:
            duracion_segundos: Duración de la ventana en segundos.
        """
        self.duracion_segundos = duracion_segundos
        self.eventos = []
        self.inicio = datetime.now()

    def agregar(self, evento):
        """
        Añade un evento a la ventana actual.
        
        Args:
            evento: Instancia de EventoCrypto a agregar.
        """
        self.eventos.append(evento)

    def esta_cerrada(self):
        """
        Verifica si la ventana ha excedido su duración.
        
        Returns:
            bool: True si la ventana está cerrada.
        """
        elapsed = (datetime.now() - self.inicio).total_seconds()
        return elapsed >= self.duracion_segundos

    def calcular_estadisticas(self):
        """
        Calcula estadísticas agregadas por criptomoneda.
        
        Returns:
            list: Lista de diccionarios con estadísticas por moneda.
        """
        if not self.eventos:
            return []

        grupos = {}
        for evento in self.eventos:
            if evento.coin_id not in grupos:
                grupos[evento.coin_id] = {'precios': [], 'cambios': []}
            grupos[evento.coin_id]['precios'].append(evento.precio_usd)
            grupos[evento.coin_id]['cambios'].append(evento.cambio_24h)

        resultados = []
        ventana_fin = datetime.now()

        for coin_id, datos in grupos.items():
            precios = datos['precios']
            cambios = datos['cambios']
            resultados.append({
                'coin_id': coin_id,
                'min_precio': min(precios),
                'max_precio': max(precios),
                'promedio_precio': sum(precios) / len(precios),
                'promedio_cambio': sum(cambios) / len(cambios),
                'num_eventos': len(precios),
                'ventana_inicio': self.inicio.isoformat(),
                'ventana_fin': ventana_fin.isoformat()
            })

        return resultados

    def resetear(self):
        """
        Vacía la lista de eventos y resetea el tiempo de inicio.
        """
        self.eventos = []
        self.inicio = datetime.now()


class AlertaManager:
    """
    Gestor de alertas basado en umbrales configurables.
    """

    def __init__(self, umbral_caida=-5.0, umbral_alza=5.0):
        """
        Args:
            umbral_caida: Porcentaje negativo que activa alerta de caída.
            umbral_alza: Porcentaje positivo que activa alerta de alza.
        """
        self.umbral_caida = umbral_caida
        self.umbral_alza = umbral_alza
        os.makedirs('data', exist_ok=True)
        self.ruta_log = 'data/alertas.log'

    def evaluar(self, estadisticas):
        """
        Evalúa si las estadísticas superan los umbrales de alerta.
        
        Args:
            estadisticas: Lista de diccionarios con estadísticas por moneda.
        
        Returns:
            list: Lista de tuplas (tipo, coin, valor) con alertas disparadas.
        """
        alertas = []
        
        for stats in estadisticas:
            cambio = stats.get('promedio_cambio', 0)
            
            if cambio < self.umbral_caida:
                alertas.append(('caida', stats['coin_id'], cambio))
            elif cambio > self.umbral_alza:
                alertas.append(('alza', stats['coin_id'], cambio))
        
        return alertas

    def dispara_alerta(self, tipo, coin, valor):
        """
        Imprime y guarda una alerta formateada.
        
        Args:
            tipo: 'caida' o 'alza'.
            coin: Identificador de la criptomoneda.
            valor: Valor que superó el umbral.
        """
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        if tipo == 'caida':
            mensaje = f"[{timestamp}] ⚠️ ALERTA: {coin.upper()} cayó {valor:.2f}% (umbral: {self.umbral_caida}%)"
        else:
            mensaje = f"[{timestamp}] ⚠️ ALERTA: {coin.upper()} subió {valor:.2f}% (umbral: {self.umbral_alza}%)"

        print(mensaje)

        with open(self.ruta_log, 'a') as f:
            f.write(mensaje + '\n')


class PersistenciaCsv:
    """
    Persistencia de estadísticas en archivo CSV.
    """

    def __init__(self, ruta_archivo):
        """
        Args:
            ruta_archivo: Ruta del archivo CSV donde guardar.
        """
        self.ruta_archivo = ruta_archivo
        os.makedirs(os.path.dirname(ruta_archivo), exist_ok=True)

    def guardar(self, estadisticas):
        """
        Escribe estadísticas en el archivo CSV.
        
        Args:
            estadisticas: Lista de diccionarios con estadísticas.
        """
        if not estadisticas:
            return

        escribir_header = not os.path.exists(self.ruta_archivo) or os.path.getsize(self.ruta_archivo) == 0

        with open(self.ruta_archivo, 'a', newline='') as f:
            campos = ['timestamp', 'coin_id', 'min_precio', 'max_precio', 'promedio_precio', 'num_eventos', 'ventana_inicio', 'ventana_fin']
            escritor = csv.DictWriter(f, fieldnames=campos)

            if escribir_header:
                escritor.writeheader()

            for stats in estadisticas:
                fila = {
                    'timestamp': datetime.now().isoformat(),
                    'coin_id': stats['coin_id'],
                    'min_precio': stats['min_precio'],
                    'max_precio': stats['max_precio'],
                    'promedio_precio': stats['promedio_precio'],
                    'num_eventos': stats['num_eventos'],
                    'ventana_inicio': stats['ventana_inicio'],
                    'ventana_fin': stats['ventana_fin']
                }
                escritor.writerow(fila)


class StreamProcessor:
    """
    Procesador principal que orquesta el consumo y procesamiento de mensajes.
    """

    def __init__(self, bootstrap_servers, topic):
        """
        Args:
            bootstrap_servers: Lista de servidores Kafka.
            topic: Nombre del topic a consumir.
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.ventana = VentanaTumbling(duracion_segundos=60)
        self.alerta_manager = AlertaManager(umbral_caida=-5.0, umbral_alza=5.0)
        self.persistencia = PersistenciaCsv('data/estadisticas.csv')
        self.consumer = None

    def conectar(self):
        """
        Suscribe al topic de Kafka.
        """
        try:
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id='grupo-crypto',
                auto_offset_reset='earliest',
                api_version=(0, 10, 0),
                value_deserializer=lambda m: m.decode('utf-8')
            )
            print(f"✅ Conectado al topic '{self.topic}'")
        except Exception as e:
            print(f"❌ Error al conectar con Kafka: {e}")
            raise

    def procesar_mensaje(self, mensaje):
        """
        Deserializa el mensaje y lo agrega a la ventana.
        
        Args:
            mensaje: Mensaje raw de Kafka.
        """
        try:
            evento = EventoCrypto.from_json(mensaje.value)
            self.ventana.agregar(evento)
            print(f"📥 Procesado: {evento}")
        except Exception as e:
            print(f"❌ Error al procesar mensaje: {e}")

    def imprimir_tabla_estadisticas(self, estadisticas):
        """
        Imprime una tabla formateada de estadísticas.
        
        Args:
            estadisticas: Lista de diccionarios con estadísticas.
        """
        print("\n" + "=" * 60)
        print("📊 VENTANA TUMBLING - ESTADÍSTICAS")
        print("=" * 60)
        
        for stats in estadisticas:
            print(f"  {stats['coin_id'].upper():8} | "
                  f"min: ${stats['min_precio']:,.2f} | "
                  f"max: ${stats['max_precio']:,.2f} | "
                  f"avg: ${stats['promedio_precio']:,.2f} | "
                  f"n: {stats['num_eventos']}")
        
        print("=" * 60 + "\n")

    def iniciar(self):
        """
        Inicia el loop principal de procesamiento.
        """
        print("🚀 Iniciando processor...")
        print("Presiona Ctrl+C para detener")

        try:
            for mensaje in self.consumer:
                self.procesar_mensaje(mensaje)

                if self.ventana.esta_cerrada():
                    estadisticas = self.ventana.calcular_estadisticas()
                    
                    if estadisticas:
                        self.imprimir_tabla_estadisticas(estadisticas)
                        
                        alertas = self.alerta_manager.evaluar(estadisticas)
                        for tipo, coin, valor in alertas:
                            self.alerta_manager.dispara_alerta(tipo, coin, valor)
                        
                        self.persistencia.guardar(estadisticas)

                    self.ventana.resetear()

        except KeyboardInterrupt:
            print("\n🛑 Processor detenido")
        except Exception as e:
            print(f"❌ Error en processor: {e}")


if __name__ == "__main__":
    processor = StreamProcessor(
        bootstrap_servers=['localhost:9092'],
        topic='crypto-prices'
    )
    processor.conectar()
    processor.iniciar()