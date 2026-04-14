/* =========================================
   CUSTOM INSET SCROLLBAR (GLOBAL)
   ========================================= */

(function () {
  function initCustomScrollbar() {
    const area   = document.getElementById('scrollArea');
    const header = document.getElementById('pageHeader');
    const track  = document.getElementById('customScroll');
    const thumb  = document.getElementById('customThumb');

    if (!area || !track || !thumb) return;

    let dragging = false;
    let startY = 0;
    let startScrollTop = 0;

    const clamp = (v, min, max) => Math.max(min, Math.min(max, v));

    function layout() {
      const top = header
        ? Math.ceil(header.getBoundingClientRect().bottom)
        : 0;

      const height = window.innerHeight - top;

      track.style.top = top + 'px';
      track.style.height = height + 'px';
      area.style.height = height + 'px';

      toggleTrack();
      refreshThumb();
    }

    function toggleTrack() {
      track.style.display =
        (area.scrollHeight > area.clientHeight + 1)
          ? 'block'
          : 'none';
    }

    function refreshThumb() {
      const trackH = track.clientHeight;
      const maxScroll = Math.max(0, area.scrollHeight - area.clientHeight);
      const ratio = area.clientHeight / (area.scrollHeight || 1);

      const minThumb = 48;
      const thumbH = clamp(
        Math.round(trackH * ratio),
        minThumb,
        trackH
      );

      thumb.style.height = thumbH + 'px';

      const maxTop = trackH - thumbH;
      const top = maxScroll
        ? (area.scrollTop / maxScroll) * maxTop
        : 0;

      thumb.style.transform = `translateY(${top}px)`;
    }

    thumb.addEventListener('pointerdown', (e) => {
      dragging = true;
      startY = e.clientY;
      startScrollTop = area.scrollTop;
      thumb.setPointerCapture(e.pointerId);
      e.preventDefault();
    });

    thumb.addEventListener('pointermove', (e) => {
      if (!dragging) return;

      const trackH = track.clientHeight;
      const thumbH = thumb.offsetHeight;
      const maxTop = Math.max(1, trackH - thumbH);
      const maxScroll = Math.max(1, area.scrollHeight - area.clientHeight);

      const deltaY = e.clientY - startY;
      const scrollPerPx = maxScroll / maxTop;

      area.scrollTop = clamp(
        startScrollTop + deltaY * scrollPerPx,
        0,
        maxScroll
      );
    });

    function stopDrag(e) {
      dragging = false;
      try { thumb.releasePointerCapture(e.pointerId); } catch (_) {}
    }

    thumb.addEventListener('pointerup', stopDrag);
    thumb.addEventListener('pointercancel', stopDrag);
    thumb.addEventListener('lostpointercapture', () => dragging = false);

    track.addEventListener('pointerdown', (e) => {
      if (e.target !== track) return;

      const rect = track.getBoundingClientRect();
      const clickY = e.clientY - rect.top;

      const thumbH = thumb.offsetHeight;
      const maxTop = Math.max(1, track.clientHeight - thumbH);
      const maxScroll = Math.max(1, area.scrollHeight - area.clientHeight);

      const targetTop = clamp(clickY - thumbH / 2, 0, maxTop);
      area.scrollTop = (targetTop / maxTop) * maxScroll;
    });

    area.addEventListener('scroll', refreshThumb, { passive: true });
    window.addEventListener('resize', layout);
    window.addEventListener('load', layout);

    setTimeout(layout, 200);
  }

  document.addEventListener('DOMContentLoaded', initCustomScrollbar);
})();
