class CryptoSimulator {
    constructor() {
        this.preciosBase = {
            'BTC': 94200,
            'ETH': 3100,
            'SOL': 180,
            'ADA': 0.45,
            'DOGE': 0.12
        };
        this.preciosActual = { ...this.preciosBase };
    }

    actualizar() {
        const variacion = () => (Math.random() - 0.5) * 0.01;
        
        for (let coin in this.preciosActual) {
            this.preciosActual[coin] *= (1 + variacion());
        }
    }

    generarEvento() {
        this.actualizar();
        const coins = Object.keys(this.preciosActual);
        const coin = coins[Math.floor(Math.random() * coins.length)];
        
        const cambio = (Math.random() - 0.5) * 10;
        
        return {
            coin: coin,
            precio: this.preciosActual[coin],
            cambio: cambio,
            timestamp: new Date().toISOString()
        };
    }

    getTodosPrecios() {
        const resultado = [];
        for (let coin in this.preciosActual) {
            resultado.push({
                coin: coin,
                precio: this.preciosActual[coin],
                cambio: (Math.random() - 0.5) * 10
            });
        }
        return resultado;
    }
}

class KafkaBrokerVisual {
    constructor() {
        this.cola = [];
        this.offset = 0;
        this.maxCola = 6;
    }

    encolar(evento) {
        this.offset++;
        const mensaje = {
            offset: this.offset,
            evento: evento
        };
        
        if (this.cola.length >= this.maxCola) {
            this.cola.shift();
        }
        
        this.cola.push(mensaje);
        return mensaje;
    }

    desencolar() {
        if (this.cola.length === 0) return null;
        return this.cola.shift();
    }

    getColaVisual() {
        return this.cola.slice(0, this.maxCola);
    }
}

class DashboardController {
    constructor() {
        this.simulator = new CryptoSimulator();
        this.broker = new KafkaBrokerVisual();
        
        this.producerActivo = false;
        this.intervaloId = null;
        this.ventanaId = null;
        
        this.contadorProducidos = 0;
        this.contadorConsumidos = 0;
        
        this.ventanaInicio = null;
        this.ventanaDuracion = 60;
        
        this.ventanaEventos = [];
        this.ventanaPrecios = {};
        
        this.velocidad = 1000;
        this.ultimoLog = '';
    }

    iniciar() {
        this.producerActivo = true;
        this.ventanaInicio = Date.now();
        
        document.getElementById('btn-iniciar').disabled = true;
        document.getElementById('btn-detener').disabled = false;
        document.getElementById('status-indicator').classList.add('active');
        document.getElementById('status-text').textContent = 'ACTIVO';

        this.intervaloId = setInterval(() => this.cicloProduccion(), this.velocidad);
        this.ventanaId = setInterval(() => this.actualizarVentana(), 1000);
        
        this.agregarLog('system', 'Sistema iniciado');
    }

    detener() {
        this.producerActivo = false;
        
        if (this.intervaloId) clearInterval(this.intervaloId);
        if (this.ventanaId) clearInterval(this.ventanaId);
        
        document.getElementById('btn-iniciar').disabled = false;
        document.getElementById('btn-detener').disabled = true;
        document.getElementById('status-indicator').classList.remove('active');
        document.getElementById('status-text').textContent = 'INACTIVO';
        
        this.agregarLog('system', 'Sistema detenido');
    }

    cicloProduccion() {
        const evento = this.simulator.generarEvento();
        
        this.contadorProducidos++;
        document.getElementById('enviados-count').textContent = this.contadorProducidos;
        document.getElementById('footer-producidos').textContent = this.contadorProducidos;

        const mensaje = this.broker.encolar(evento);
        
        this.agregarLog('producer', `producer → ${evento.coin} $${evento.precio.toFixed(2)} publicado`);
        this.agregarLog('broker', `broker → offset #${mensaje.offset} encolado`);
        
        this.actualizarCola();
        this.consumirMensaje(mensaje);
    }

    consumirMensaje(mensaje) {
        setTimeout(() => {
            this.contadorConsumidos++;
            document.getElementById('footer-consumidos').textContent = this.contadorConsumidos;
            
            const evento = mensaje.evento;
            this.agregarLog('consumer', `consumer ← #${mensaje.offset} procesado`);
            
            this.ventanaEventos.push(evento);
            
            if (!this.ventanaPrecios[evento.coin]) {
                this.ventanaPrecios[evento.coin] = [];
            }
            this.ventanaPrecios[evento.coin].push(evento.precio);
            
            this.actualizarConsumerPrecios();
            
            if (Math.abs(evento.cambio) > 5) {
                const tipo = evento.cambio > 0 ? 'subió' : 'bajó';
                this.agregarLog('alerta', `⚠️ ALERTA: ${evento.coin} ${tipo} ${Math.abs(evento.cambio).toFixed(1)}%`);
            }
            
            this.actualizarCola();
        }, 200);
    }

    generarRafaga() {
        for (let i = 0; i < 5; i++) {
            setTimeout(() => {
                if (this.producerActivo) this.cicloProduccion();
            }, i * 300);
        }
    }

    actualizarCola() {
        const cola = this.broker.getColaVisual();
        const colaEl = document.getElementById('cola-mensajes');
        
        colaEl.innerHTML = cola.map((m, idx) => {
            const procesando = idx === 0 ? 'processing' : '';
            return `<div class="cola-msg ${procesando}">#${m.offset} ${m.evento.coin} $${m.evento.precio.toFixed(2)}</div>`;
        }).join('');

        document.getElementById('cola-count').textContent = `${cola.length} msgs`;
        document.getElementById('offset-value').textContent = `#${this.broker.offset}`;
        document.getElementById('footer-cola').textContent = cola.length;
    }

    actualizarConsumerPrecios() {
        const precios = this.simulator.getTodosPrecios();
        const container = document.getElementById('consumer-prices');
        
        container.innerHTML = precios.map(p => {
            const cambioClase = p.cambio > 0.5 ? 'up' : p.cambio < -0.5 ? 'down' : 'neutral';
            const emoji = p.cambio > 0.5 ? '📈' : p.cambio < -0.5 ? '📉' : '➡️';
            
            return `
                <div class="crypto-price">
                    <span class="crypto-symbol">${p.coin}</span>
                    <span class="crypto-value">
                        $${p.precio.toFixed(p.precio < 1 ? 4 : 2)}
                        <span class="crypto-change ${cambioClase}">${emoji} ${p.cambio.toFixed(1)}%</span>
                    </span>
                </div>
            `;
        }).join('');
    }

    actualizarVentana() {
        if (!this.ventanaInicio) return;
        
        const elapsed = (Date.now() - this.ventanaInicio) / 1000;
        const remaining = Math.max(0, this.ventanaDuracion - elapsed);
        
        const mins = Math.floor(remaining / 60).toString().padStart(2, '0');
        const secs = Math.floor(remaining % 60).toString().padStart(2, '0');
        
        document.getElementById('ventana-timer').textContent = `${mins}:${secs}`;
        document.getElementById('footer-ventana').textContent = `${mins}:${secs} restantes`;
        
        if (elapsed >= this.ventanaDuracion) {
            this.cerrarVentana();
            this.ventanaInicio = Date.now();
        }
    }

    cerrarVentana() {
        const statsEl = document.getElementById('ventana-stats');
        
        if (Object.keys(this.ventanaPrecios).length === 0) {
            statsEl.innerHTML = '<div class="coin-row">Sin datos en esta ventana</div>';
            return;
        }
        
        let html = '';
        for (let coin in this.ventanaPrecios) {
            const precios = this.ventanaPrecios[coin];
            const min = Math.min(...precios);
            const max = Math.max(...precios);
            const avg = precios.reduce((a, b) => a + b, 0) / precios.length;
            
            html += `<div class="coin-row">${coin}: min $${min.toFixed(2)} | max $${max.toFixed(2)} | avg $${avg.toFixed(2)}</div>`;
        }
        
        statsEl.innerHTML = html;
        
        this.agregarLog('system', `📊 Ventana cerrada - ${this.ventanaEventos.length} eventos`);
        
        this.ventanaEventos = [];
        this.ventanaPrecios = {};
    }

    agregarLog(tipo, mensaje) {
        const logPanel = document.getElementById('log-panel');
        const timestamp = new Date().toLocaleTimeString();
        
        let tipoClase = '';
        if (tipo === 'producer') tipoClase = 'producer-log';
        else if (tipo === 'broker') tipoClase = 'broker-log';
        else if (tipo === 'consumer') tipoClase = 'consumer-log';
        else if (tipo === 'alerta') tipoClase = 'alerta';
        
        const entry = document.createElement('div');
        entry.className = `log-entry ${tipoClase}`;
        entry.innerHTML = `<span class="timestamp">[${timestamp}]</span> ${mensaje}`;
        
        logPanel.insertBefore(entry, logPanel.children[1]);
        
        while (logPanel.children.length > 20) {
            logPanel.removeChild(logPanel.lastChild);
        }
    }

    setSpeed(speed) {
        document.querySelectorAll('.speed-btn').forEach(btn => btn.classList.remove('active'));
        event.target.classList.add('active');
        
        if (speed === 'lenta') this.velocidad = 2000;
        else if (speed === 'normal') this.velocidad = 1000;
        else if (speed === 'rapida') this.velocidad = 500;
        
        if (this.producerActivo) {
            clearInterval(this.intervaloId);
            this.intervaloId = setInterval(() => this.cicloProduccion(), this.velocidad);
        }
    }

    reiniciar() {
        this.detener();
        
        this.contadorProducidos = 0;
        this.contadorConsumidos = 0;
        this.broker = new KafkaBrokerVisual();
        this.ventanaEventos = [];
        this.ventanaPrecios = {};
        this.ventanaInicio = null;
        
        document.getElementById('enviados-count').textContent = '0';
        document.getElementById('footer-producidos').textContent = '0';
        document.getElementById('footer-consumidos').textContent = '0';
        document.getElementById('cola-mensajes').innerHTML = '';
        document.getElementById('ventana-stats').innerHTML = '';
        document.getElementById('ventana-timer').textContent = '00:00';
        
        const logPanel = document.getElementById('log-panel');
        while (logPanel.children.length > 1) {
            logPanel.removeChild(logPanel.lastChild);
        }
        
        this.agregarLog('system', 'Sistema reiniciado');
    }
}

const dashboard = new DashboardController();

function iniciarProducer() {
    dashboard.iniciar();
}

function detenerProducer() {
    dashboard.detener();
}

function generarRafaga() {
    dashboard.generarRafaga();
}

function setSpeed(speed) {
    dashboard.setSpeed(speed);
}

function reiniciar() {
    dashboard.reiniciar();
}

window.onload = () => {
    dashboard.agregarLog('system', 'Dashboard listo - Presiona [Iniciar] para comenzar');
};
