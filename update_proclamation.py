#!/usr/bin/env python3
"""
Rewrite the proclamation links in the hand-maintained static pages from
proclamation.json.

The five top-level pages are served straight off disk, so instead of a build
step they carry marked regions:

    <!-- proclamation:nav:start --> ... <!-- proclamation:nav:end -->

Everything between a start/end pair is regenerated from proclamation.json.
The generated water pages don't need this -- scraper.py fills their
{{PROCLAMATION_URL}} placeholder from the same config.

Idempotent: running it twice changes nothing. Run after editing
proclamation.json; the daily workflow runs it too, so a stale link is caught
even if someone forgets.
"""

import re
import sys

import proclamation

PAGES = ["index.html", "waters.html", "about.html", "contact.html", "privacy.html"]

NAV_CLASS = "text-blue-600 hover:text-blue-800 font-medium transition-colors"


def render_nav(pdf_url, season, publications_url, indent):
    return '%s<a href="%s" target="_blank" rel="noopener noreferrer" class="%s">Fishing Rules</a>' % (
        indent, pdf_url, NAV_CLASS)


def render_footer(pdf_url, season, publications_url, indent):
    return '%s<a href="%s" target="_blank" rel="noopener noreferrer" class="hover:underline">Fishing Rules</a> •' % (
        indent, pdf_url)


def render_callout(pdf_url, season, publications_url, indent):
    # "the 2026–2027 fishing rules" reads oddly with no season, so drop the
    # qualifier entirely rather than emitting a dangling dash.
    heading = "Know Before You Go: %s Fishing Rules" % season if season else "Know Before You Go: Fishing Rules"
    button = "Read the %s Fishing Rules &amp; Information (PDF)" % season if season else "Read the Fishing Rules &amp; Information (PDF)"
    i = indent
    return "\n".join([
        '%s<section class="mb-8 bg-white/90 backdrop-blur-sm rounded-xl shadow-lg p-6 md:p-8">' % i,
        '%s    <h2 class="text-2xl font-bold text-gray-800 mb-2">' % i,
        '%s        <svg class="inline w-6 h-6 mr-2 text-blue-600 -mt-1" fill="none" stroke="currentColor" viewBox="0 0 24 24" aria-hidden="true"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 6.253v13m0-13C10.832 5.477 9.246 5 7.5 5S4.168 5.477 3 6.253v13C4.168 18.477 5.754 18 7.5 18s3.332.477 4.5 1.247m0-13C13.168 5.477 14.754 5 16.5 5c1.747 0 3.332.477 4.5 1.253v13C19.832 18.477 18.247 18 16.5 18c-1.746 0-3.332.477-4.5 1.247"/></svg>%s' % (i, heading),
        '%s    </h2>' % i,
        '%s    <p class="text-gray-600 mb-4">Bag limits, special trout waters, license requirements and season dates all live in the official proclamation from the New Mexico Department of Wildlife. It runs April 1 through March 31, so make sure you are reading the current one.</p>' % i,
        '%s    <a href="%s" target="_blank" rel="noopener noreferrer" class="inline-block bg-blue-600 hover:bg-blue-700 text-white font-semibold px-5 py-3 rounded-lg transition-colors">%s</a>' % (i, pdf_url, button),
        '%s    <p class="text-xs text-gray-500 mt-3">Opens the official NMDOW booklet. Browse every edition on the <a href="%s" target="_blank" rel="noopener noreferrer" class="text-blue-600 hover:underline">NMDOW publications page</a>.</p>' % (i, publications_url),
        '%s</section>' % i,
    ])


REGIONS = {"nav": render_nav, "footer": render_footer, "callout": render_callout}


def update_page(path, pdf_url, season, publications_url):
    """Rewrite every marked region in one page. Returns (changed, regions_found)."""
    with open(path, encoding="utf-8") as f:
        html = f.read()
    original, found = html, []

    for name, render in REGIONS.items():
        pattern = re.compile(
            r"([ \t]*)<!-- proclamation:%s:start -->\n.*?\n([ \t]*)<!-- proclamation:%s:end -->" % (name, name),
            re.S)

        def replace(m):
            found.append(name)
            indent = m.group(1)
            body = render(pdf_url, season, publications_url, indent)
            return "%s<!-- proclamation:%s:start -->\n%s\n%s<!-- proclamation:%s:end -->" % (
                indent, name, body, m.group(2), name)

        html = pattern.sub(replace, html)

    if html != original:
        with open(path, "w", encoding="utf-8") as f:
            f.write(html)
        return True, found
    return False, found


def main():
    pdf_url, season, publications_url = proclamation.load()

    # Bail before touching anything: quietly rewriting every "Fishing Rules"
    # link to the publications index is worse than leaving the pages alone.
    if pdf_url == publications_url:
        print("ERROR: no usable pdf_url in %s -- refusing to rewrite the pages."
              % proclamation.CONFIG_FILE)
        return 1
    print("Proclamation: %s (season: %s)" % (pdf_url, season or "unknown"))

    problems = []
    for path in PAGES:
        changed, found = update_page(path, pdf_url, season, publications_url)
        missing = {"nav", "footer"} - set(found)
        if missing:
            problems.append("%s is missing region(s): %s" % (path, ", ".join(sorted(missing))))
        print("  %-14s %-9s regions: %s" % (path, "updated" if changed else "unchanged",
                                            ", ".join(found) or "none"))

    if problems:
        print("\nERROR: marked regions are missing -- those links are no longer managed:")
        for p in problems:
            print("  -", p)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
