/* Candidate shared-socket plugin messaging bridge. No credentials are exposed. */
(function (root) {
  'use strict';
  class MessageBridge {
    constructor(send, makeId) {
      this.send = send; this.makeId = makeId; this.pending = new Map();
      this.history = {}; this.channelNames = {}; this.tracker = null; this.listeners = new Set(); this.refreshing = false; this.buffered = [];
    }
    subscribe(callback) { this.listeners.add(callback); return () => this.listeners.delete(callback); }
    emit(event) { for (const callback of this.listeners) { try { callback(event); } catch (_) {} } }
    requestHistory() { this.refreshing = true; this.buffered = []; this.send({type:'get_messages'}); }
    sendMessage(text, destination, channel, options = {}) {
      const requestId = this.makeId();
      const request = {type:'send_message', text, dest_id:destination || null,
        channel_index:channel || 0, request_id:requestId};
      if (options.messageId) { request.delivery_tracking = true; request.message_id = options.messageId; }
      return new Promise((resolve, reject) => {
        this.pending.set(requestId, {resolve, reject});
        try { this.send(request); } catch (e) { this.pending.delete(requestId); reject(e); }
      });
    }
    receive(event) {
      if (event.type === 'connection_status' && event.tracker) { this.tracker = event.tracker; this.emit({type:'tracker', tracker:event.tracker});
      } else if (event.type === 'messages_data') {
        this.history = event.messages || {}; this.channelNames = event.channel_names || {};
        for (const m of this.buffered) { const key = m.channel_index || 0; const list = this.history[key] ||= []; if (!list.some(x => m.message_id ? x.message_id === m.message_id : JSON.stringify(x) === JSON.stringify(m))) list.unshift(m); }
        this.refreshing = false; this.buffered = [];
        this.emit({type:'history', messages:this.history, channelNames:this.channelNames});
      } else if (event.type === 'new_message') {
        const m = event.message; if (m && this.refreshing) this.buffered.push(m); if (m) { const key = m.channel_index || 0; const list = this.history[key] ||= []; if (!m.message_id || !list.some(x => x.message_id === m.message_id)) list.unshift(m); }
        this.emit({type:'message', message:event.message});
      } else if (event.type === 'message_delivery_update') {
        this.emit({type:'delivery', update:event});
      } else if (event.type === 'send_result' && event.request_id) {
        const pending = this.pending.get(event.request_id);
        if (pending) { this.pending.delete(event.request_id); pending.resolve(event); }
      }
    }
    cancelRequests(ids) { for (const id of ids) { const p = this.pending.get(id); if (p) { this.pending.delete(id); p.reject(new Error('Authentication cancelled; message not sent')); } } }
    disconnected() {
      for (const pending of this.pending.values()) pending.reject(new Error('Disconnected; send outcome unknown'));
      this.pending.clear(); this.emit({type:'disconnected'});
    }
  }
  root.MessageBridge = MessageBridge;
})(typeof window !== 'undefined' ? window : globalThis);
