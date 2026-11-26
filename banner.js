/*
 * Seasonal banner for stockingreport.com
 * ---------------------------------------
 * Shows a dismissible bar at the top of every page, but ONLY between the
 * start and end dates below. Outside that window it renders nothing and
 * costs the visitor ~1KB. Nothing needs to be redeployed to take it down.
 *
 * To run another one later (Christmas, opening day, etc.), just edit the
 * BANNER object: change the dates and the text. To kill it immediately,
 * set `enabled: false`.
 *
 * Styling is inline on purpose. tailwind.config.js only scans *.html,
 * scraper.py and public/waters/*.html, so Tailwind classes used in this
 * file would be purged out of tailwind.css and silently do nothing.
 */
(function () {
  'use strict';

  var BANNER = {
    enabled: true,

    // Local dates, inclusive start / exclusive end.
    // Shows Wed Nov 25 through end of day Sun Nov 29, 2026.
    start: new Date(2026, 10, 25, 0, 0, 0),  // month is 0-indexed: 10 = November
    end:   new Date(2026, 10, 30, 0, 0, 0),

    // Bump this string for each new banner so a visitor who dismissed the
    // last one still sees the next one.
    id: 'thanksgiving-2026',

    emoji: '🦃', // turkey
    headline: 'Happy Thanksgiving',
    message: 'Tight lines and safe travels from NM Stocking Report.',

    // Autumn palette, chosen to sit over the site's blue header without clashing.
    background: 'linear-gradient(90deg, #7c2d12 0%, #9a3412 45%, #b45309 100%)',
    textColor: '#fff7ed'
  };

  function shouldShow() {
    // Preview override: add ?banner=preview to any URL (or set
    // window.SR_BANNER_PREVIEW = true) to force the banner on regardless of
    // date or dismissal. Useful for checking a banner before its window opens.
    try {
      if (window.SR_BANNER_PREVIEW === true) return true;
      if (window.location.search.indexOf('banner=preview') !== -1) return true;
    } catch (e) {
      // Ignore and fall through to the normal date check.
    }

    if (!BANNER.enabled) return false;

    var now = new Date();
    if (now < BANNER.start || now >= BANNER.end) return false;

    try {
      if (window.localStorage.getItem('srBannerDismissed') === BANNER.id) return false;
    } catch (e) {
      // Private mode or blocked storage: show the banner rather than fail.
    }

    return true;
  }

  function dismiss(el) {
    var reduceMotion = window.matchMedia &&
      window.matchMedia('(prefers-reduced-motion: reduce)').matches;

    try {
      window.localStorage.setItem('srBannerDismissed', BANNER.id);
    } catch (e) {
      // Nothing to do; the banner will reappear next load.
    }

    if (reduceMotion) {
      el.remove();
      return;
    }

    el.style.transition = 'opacity .25s ease, max-height .25s ease';
    el.style.overflow = 'hidden';
    el.style.maxHeight = el.offsetHeight + 'px';
    // Force reflow so the collapse animates from the measured height.
    void el.offsetHeight;
    el.style.opacity = '0';
    el.style.maxHeight = '0px';
    window.setTimeout(function () { el.remove(); }, 260);
  }

  function render() {
    if (!shouldShow()) return;
    if (document.getElementById('sr-seasonal-banner')) return;

    var bar = document.createElement('div');
    bar.id = 'sr-seasonal-banner';
    bar.setAttribute('role', 'region');
    bar.setAttribute('aria-label', 'Holiday message');
    bar.style.cssText = [
      'background:' + BANNER.background,
      'color:' + BANNER.textColor,
      'font-family:inherit',
      'padding:10px 44px 10px 16px',
      'position:relative',
      'text-align:center',
      'line-height:1.45',
      'box-shadow:0 1px 4px rgba(0,0,0,.25)'
    ].join(';');

    var inner = document.createElement('div');
    inner.style.cssText = 'max-width:64rem;margin:0 auto;font-size:.9rem;';

    var strong = document.createElement('strong');
    strong.style.cssText = 'font-weight:700;letter-spacing:.01em;';
    strong.textContent = BANNER.emoji + ' ' + BANNER.headline;

    var rest = document.createElement('span');
    rest.style.cssText = 'opacity:.95;';
    rest.textContent = ' — ' + BANNER.message;

    inner.appendChild(strong);
    inner.appendChild(rest);
    bar.appendChild(inner);

    var close = document.createElement('button');
    close.type = 'button';
    close.setAttribute('aria-label', 'Dismiss holiday message');
    close.textContent = '×';
    close.style.cssText = [
      'position:absolute',
      'top:50%',
      'right:10px',
      'transform:translateY(-50%)',
      'background:transparent',
      'border:0',
      'color:' + BANNER.textColor,
      'font-size:1.5rem',
      'line-height:1',
      'width:32px',
      'height:32px',
      'cursor:pointer',
      'opacity:.75',
      'border-radius:6px'
    ].join(';');
    close.addEventListener('mouseenter', function () { close.style.opacity = '1'; });
    close.addEventListener('mouseleave', function () { close.style.opacity = '.75'; });
    close.addEventListener('focus', function () {
      close.style.opacity = '1';
      close.style.outline = '2px solid ' + BANNER.textColor;
      close.style.outlineOffset = '1px';
    });
    close.addEventListener('blur', function () {
      close.style.opacity = '.75';
      close.style.outline = 'none';
    });
    close.addEventListener('click', function () { dismiss(bar); });
    bar.appendChild(close);

    document.body.insertBefore(bar, document.body.firstChild);
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', render);
  } else {
    render();
  }
})();
