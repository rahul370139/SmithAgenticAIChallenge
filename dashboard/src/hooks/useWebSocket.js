import { useState, useEffect, useRef, useCallback } from 'react';

const _apiUrl = import.meta.env.VITE_API_URL ?? '';
// Same-origin proxy mode (Pathwise pattern): empty VITE_API_URL → wss/ws via Vercel domain
const _wsBase = _apiUrl
  ? _apiUrl.replace(/^https/, 'wss').replace(/^http/, 'ws')
  : `${window.location.protocol === 'https:' ? 'wss' : 'ws'}://${window.location.host}`;
const WS_URL = `${_wsBase}/ws/events`;

export function useWebSocket(eventTypes = []) {
  const [messages, setMessages] = useState([]);
  const [connected, setConnected] = useState(false);
  const wsRef = useRef(null);
  const reconnectRef = useRef(null);

  const connect = useCallback(() => {
    if (wsRef.current?.readyState === WebSocket.OPEN) return;
    try {
      const ws = new WebSocket(WS_URL);
      ws.onopen = () => setConnected(true);
      ws.onclose = () => {
        setConnected(false);
        reconnectRef.current = setTimeout(connect, 3000);
      };
      ws.onerror = () => ws.close();
      ws.onmessage = (evt) => {
        try {
          const data = JSON.parse(evt.data);
          if (eventTypes.length === 0 || eventTypes.includes(data.type)) {
            setMessages(prev => [...prev.slice(-99), data]);
          }
        } catch { /* ignore non-JSON */ }
      };
      wsRef.current = ws;
    } catch { /* connection refused */ }
  }, [eventTypes.join(',')]);

  useEffect(() => {
    connect();
    return () => {
      clearTimeout(reconnectRef.current);
      wsRef.current?.close();
    };
  }, [connect]);

  const clearMessages = useCallback(() => setMessages([]), []);

  return { messages, connected, clearMessages };
}
