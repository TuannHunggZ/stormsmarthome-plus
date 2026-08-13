const alertList = document.getElementById('alertList');
const connectionStatus = document.getElementById('connectionStatus');
const alertCount = document.getElementById('alertCount');

const maxVisibleAlerts = 20;
let socket;
let reconnectTimer;

function formatDate(value) {
    try {
        return new Date(value).toLocaleString();
    } catch (error) {
        return String(value || 'Unknown');
    }
}

function formatValue(value) {
    if (value === null || value === undefined) {
        return 'N/A';
    }

    if (typeof value === 'number') {
        return Number.isInteger(value) ? String(value) : value.toFixed(2);
    }

    return String(value);
}

function setConnected(isConnected) {
    connectionStatus.textContent = isConnected ? 'Connected' : 'Disconnected';
    connectionStatus.classList.toggle('connected', isConnected);
    connectionStatus.classList.toggle('disconnected', !isConnected);
}

function updateAlertCount() {
    alertCount.textContent = `${alertList.children.length} alerts`;
}

function renderAlert(alert, highlight = true) {
    const card = document.createElement('article');
    card.className = `alert${highlight ? ' recent' : ''}`;

    const title = document.createElement('div');
    title.className = 'alert-top';

    const heading = document.createElement('div');
    heading.className = 'alert-title';

    const badge = document.createElement('span');
    badge.className = `badge ${String(alert.anomalyType || '').toLowerCase()}`;
    badge.textContent = alert.type || 'ANOMALY';

    const anomaly = document.createElement('strong');
    anomaly.textContent = alert.anomalyType || 'UNKNOWN';

    heading.appendChild(badge);
    heading.appendChild(anomaly);

    const time = document.createElement('div');
    time.className = 'alert-time';
    time.textContent = formatDate(alert.receivedAt || alert.timestamp);

    title.appendChild(heading);
    title.appendChild(time);

    const grid = document.createElement('div');
    grid.className = 'alert-grid';

    const fields = [
        ['Window size', alert.windowSize],
        ['House', alert.houseId],
        ['Household', alert.householdId],
        ['Plug', alert.plugId],
        ['Current value', alert.value],
        ['Historical average', alert.avg],
        ['Historical min', alert.min],
        ['Historical max', alert.max],
        ['Threshold', alert.anomalyThresholdPercent]
    ];

    for (const [label, value] of fields) {
        const field = document.createElement('div');
        field.className = 'field';

        const labelNode = document.createElement('span');
        labelNode.className = 'label';
        labelNode.textContent = label;

        const valueNode = document.createElement('span');
        valueNode.className = 'value';
        valueNode.textContent = formatValue(value);

        field.appendChild(labelNode);
        field.appendChild(valueNode);
        grid.appendChild(field);
    }

    card.appendChild(title);
    card.appendChild(grid);
    alertList.prepend(card);

    while (alertList.children.length > maxVisibleAlerts) {
        alertList.removeChild(alertList.lastElementChild);
    }

    updateAlertCount();

    if (highlight) {
        window.setTimeout(() => {
            card.classList.remove('recent');
        }, 1500);
    }
}

function handleMessage(event) {
    const payload = JSON.parse(event.data);

    if (payload.type === 'history' && Array.isArray(payload.alerts)) {
        alertList.innerHTML = '';
        payload.alerts.slice().reverse().forEach((alert) => renderAlert(alert, false));
        return;
    }

    renderAlert(payload, true);
}

function connect() {
    if (socket && (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CONNECTING)) {
        return;
    }

    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    socket = new WebSocket(`${protocol}//${window.location.host}`);

    socket.addEventListener('open', () => {
        setConnected(true);
        if (reconnectTimer) {
            window.clearTimeout(reconnectTimer);
            reconnectTimer = undefined;
        }
    });

    socket.addEventListener('message', handleMessage);

    socket.addEventListener('close', () => {
        setConnected(false);
        reconnectTimer = window.setTimeout(connect, 2000);
    });

    socket.addEventListener('error', () => {
        setConnected(false);
    });
}

setConnected(false);
connect();