// ==UserScript==
// @name         Neko Community Chat (喵宇宙社区聊天)
// @namespace    neko-community-chat
// @version      0.1.0
// @description  在喵宇宙社区网页右下角注入 QQ 式聊天窗，和本地 YUI 实时对话（本地服务 127.0.0.1:7870，需 neko_community_chat 插件运行）
// @author       jh
// @match        https://community.project-neko.cn/*
// @match        http://community.project-neko.cn/*
// @grant        none
// @run-at       document-end
// ==/UserScript==

(function () {
  'use strict';
  const API = 'http://127.0.0.1:7870';
  let lastId = 0;

  // 浮动按钮
  const fab = document.createElement('div');
  fab.textContent = '🐱';
  fab.style.cssText = [
    'position:fixed', 'right:20px', 'bottom:24px', 'z-index:2147483647',
    'width:54px', 'height:54px', 'border-radius:50%',
    'background:linear-gradient(135deg,#7ec8ff,#b18cff)',
    'color:#fff', 'font-size:24px', 'display:flex', 'align-items:center',
    'justify-content:center', 'cursor:pointer', 'box-shadow:0 4px 14px rgba(0,0,0,.25)',
    'user-select:none', 'transition:transform .15s'
  ].join(';');
  fab.title = '和 YUI 聊天';
  document.body.appendChild(fab);

  // 聊天窗（iframe 嵌本地面板）
  const panel = document.createElement('div');
  panel.style.cssText = [
    'position:fixed', 'right:20px', 'bottom:88px', 'z-index:2147483646',
    'width:360px', 'height:520px', 'border-radius:14px', 'overflow:hidden',
    'box-shadow:0 8px 30px rgba(0,0,0,.3)', 'background:#fff',
    'display:none', 'flex-direction:column', 'border:1px solid #e3e5e8'
  ].join(';');
  const header = document.createElement('div');
  header.style.cssText = [
    'padding:10px 14px', 'background:#2b2f3a', 'color:#fff', 'font-size:13px',
    'display:flex', 'justify-content:space-between', 'align-items:center', 'cursor:move'
  ].join(';');
  header.innerHTML = '<span>喵宇宙聊天 · YUI</span><span style="cursor:pointer;font-size:15px" id="ncc-close">✕</span>';
  const frame = document.createElement('iframe');
  frame.src = API + '/chat';
  frame.style.cssText = 'flex:1;border:none;width:100%;';
  panel.appendChild(header);
  panel.appendChild(frame);
  document.body.appendChild(panel);

  let open = false;
  fab.addEventListener('click', () => {
    open = !open;
    panel.style.display = open ? 'flex' : 'none';
    fab.style.transform = open ? 'rotate(90deg)' : '';
    if (open) { try { frame.contentWindow.location.reload(); } catch (e) {} }
  });
  document.getElementById('ncc-close').addEventListener('click', () => {
    open = false;
    panel.style.display = 'none';
  });

  // 拖拽（简单版：mousedown 移动）
  let drag = null;
  header.addEventListener('mousedown', e => {
    drag = { dx: e.clientX - panel.offsetLeft, dy: e.clientY - panel.offsetTop };
    document.addEventListener('mousemove', onMove);
    document.addEventListener('mouseup', () => {
      document.removeEventListener('mousemove', onMove);
    });
  });
  function onMove(e) {
    if (!drag) return;
    const x = Math.min(Math.max(e.clientX - drag.dx, -panel.offsetWidth + 80), window.innerWidth - 40);
    const y = Math.min(Math.max(e.clientY - drag.dy, 0), window.innerHeight - 60);
    panel.style.right = 'auto';
    panel.style.bottom = 'auto';
    panel.style.left = x + 'px';
    panel.style.top = y + 'px';
  }
})();
